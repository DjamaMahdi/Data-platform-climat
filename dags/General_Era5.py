"""
### Use Airflow together with DuckDB and MotherDuck

This DAG takes the Monthly data from the ERA5 dataset (1940 to present) on the Copernicus Climate Data Store, ingests it into MotherDuck using DuckDB's HTTPFS connector, 
transforms it with dbt, and finally exports the gold fact table to Cloudflare R2 as a Parquet file.
"""

import cdsapi
import zipfile
import os
import duckdb
import xarray as xr
import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime
from dateutil.relativedelta import relativedelta
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
CDSAPI_KEY = os.getenv("CDSAPI_KEY")
extract_dir = 'include/dataset'
START_YEAR = 1940
ERA5_LAG_MONTHS = 2
os.makedirs(extract_dir, exist_ok=True)


def get_era5_cutoff():
    """Returns the last safely available ERA5 final month as (year_str, month_str)."""
    cutoff = datetime.today() - relativedelta(months=ERA5_LAG_MONTHS)
    return cutoff.year, cutoff.month


def get_latest_month_in_motherduck():
    """Returns the latest (year, month) already loaded, or None if no data."""
    try:
        conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token": MOTHERDUCK_TOKEN})
        table_exists = conn.sql("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'Bronze' AND table_name = 'General1'
        """).fetchone()[0]
        if table_exists == 0:
            conn.close()
            return None
        result = conn.sql("SELECT MAX(valid_time) FROM Climate_Era5.Bronze.General1").fetchone()[0]
        conn.close()
        if result is None:
            return None
        return (result.year, result.month)
    except Exception:
        return None


def get_missing_months(latest_loaded):
    """
    Returns a list of (year, month) tuples that are available on ERA5
    but not yet loaded in MotherDuck.
    latest_loaded: (year, month) or None (= no data at all).
    """
    cutoff_year, cutoff_month = get_era5_cutoff()
    cutoff_date = datetime(cutoff_year, cutoff_month, 1)

    if latest_loaded is None:
        start_date = datetime(START_YEAR, 1, 1)
    else:
        start_date = datetime(latest_loaded[0], latest_loaded[1], 1) + relativedelta(months=1)

    months = []
    current = start_date
    while current <= cutoff_date:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


ERA5_VARIABLES = [
    "2m_dewpoint_temperature",
    "2m_temperature",
    "total_precipitation",
    "10m_wind_speed",
    "surface_solar_radiation_downwards",
    "evaporation",
    "potential_evaporation",
    "runoff",
    "soil_temperature_level_1",
    "volumetric_soil_water_layer_1",
    "mean_sea_level_pressure",
    "surface_pressure",
    "sea_surface_temperature",
    "mean_wave_direction",
    "mean_wave_period",
]

YEARS_PER_BATCH = 10


def build_cds_requests(missing_months):
    """
    Build a list of CDS API requests, chunked to avoid the cartesian product bug.

    Groups missing months by year, then:
    - Full years (all 12 months needed) are chunked into groups of YEARS_PER_BATCH
    - Partial years (< 12 months) each get their own request with only their specific months

    Returns a list of request dicts safe to send to the CDS API.
    """
    # Group months by year
    months_by_year = {}
    for y, m in missing_months:
        months_by_year.setdefault(y, []).append(m)

    full_years = []
    partial_years = []
    for year, months in sorted(months_by_year.items()):
        if sorted(months) == list(range(1, 13)):
            full_years.append(year)
        else:
            partial_years.append((year, sorted(months)))

    requests = []
    base = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": ERA5_VARIABLES,
        "time": ["00:00"],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": [12.7, 41.7, 10.9, 43.5],
    }

    # Chunk full years into groups of YEARS_PER_BATCH
    for i in range(0, len(full_years), YEARS_PER_BATCH):
        chunk = full_years[i:i + YEARS_PER_BATCH]
        req = {**base, "year": [str(y) for y in chunk], "month": [str(m).zfill(2) for m in range(1, 13)]}
        requests.append(req)

    # Each partial year gets its own request
    for year, months in partial_years:
        req = {**base, "year": [str(year)], "month": [str(m).zfill(2) for m in months]}
        requests.append(req)

    return requests


@dag(start_date=datetime(2026, 1, 1),
     schedule=None,
     catchup=False,
     tags=['duckdb'],
     max_active_tasks=1,
)

def era5_in_taskflow():

    @task
    def retrieve_and_ingest_Era5():
        "Download ERA5 data and ingest directly into MotherDuck (no CSV intermediate)"

        latest_loaded = get_latest_month_in_motherduck()
        missing_months = get_missing_months(latest_loaded)
        incremental = latest_loaded is not None

        if not missing_months:
            print("No missing months — MotherDuck is up to date. Skipping.")
            return

        requests = build_cds_requests(missing_months)
        mode = "INCREMENTAL" if incremental else "FULL LOAD"
        print(f"Mode: {mode} | {len(missing_months)} missing months across {len(requests)} batch(es)")

        target = 'include/dataset/Djibouti_era5.zip'
        client = cdsapi.Client("https://cds.climate.copernicus.eu/api", CDSAPI_KEY)
        dataset = "reanalysis-era5-single-levels-monthly-means"
        conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token": MOTHERDUCK_TOKEN})

        # First batch of a full load uses CREATE OR REPLACE; all others INSERT INTO
        first_batch = not incremental

        for batch_idx, request in enumerate(requests, start=1):
            print(f"Batch {batch_idx}/{len(requests)}: year={request['year']} month={request['month']}")

            # Download
            client.retrieve(dataset, request, target)
            # Unzip
            with zipfile.ZipFile(target) as z:
                z.extractall(extract_dir)
            os.remove(target)

            # Ingest .nc files (sorted for consistent General1/General2 mapping)
            nc_files = sorted(f for f in os.listdir(extract_dir) if f.endswith('.nc'))

            for i, nc_file in enumerate(nc_files, start=1):
                ds = xr.open_dataset(os.path.join(extract_dir, nc_file))
                df = ds.to_dataframe().reset_index()
                table_name = f"General{i}"
                print(f"  {table_name} columns: {list(df.columns)}")

                if first_batch:
                    print(f"  {table_name}: {df.shape[0]} rows (full replace)")
                    conn.sql(f"""
                        CREATE OR REPLACE TABLE Climate_Era5.Bronze.{table_name}
                        AS SELECT * FROM df
                    """)
                else:
                    print(f"  {table_name}: {df.shape[0]} rows (append)")
                    conn.sql(f"""
                        INSERT INTO Climate_Era5.Bronze.{table_name}
                        SELECT * FROM df
                    """)
                os.remove(os.path.join(extract_dir, nc_file))

            # After the first batch, all subsequent batches append
            first_batch = False

        conn.close()
        print("Ingestion complete.")
    
    @task
    def ingest_GDAM():
        "LOAD GDAM LVL 1 Geojson into motherduck (bronze schema) as a reference table "
        conn = duckdb.connect("md:Climate_Era5", config = {"motherduck_token" : MOTHERDUCK_TOKEN})
        conn.execute("INSTALL Spatial")
        conn.execute("LOAD Spatial")
        geojson_path = 'include/dataset/Djibouti_Region.json'
        conn.execute(f"""
            CREATE OR REPLACE TABLE Climate_Era5.Bronze.Djibouti_Region 
            AS SELECT 
               NAME_1,
               geom
               FROM ST_Read('{geojson_path}')
            """)
        conn.close()
        print("GDAM ingestion complete.")


    staging = DbtTaskGroup(
        group_id='staging',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging']
        )
    )

    marts = DbtTaskGroup(
        group_id='marts',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts']
        )

    )
    @task
    def export_to_R2() :
        "Exporting the gold fact table into cloudfare as a parquet file for storage"
        
        conn = duckdb.connect("md:Climate_Era5", config = {"motherduck_token" : MOTHERDUCK_TOKEN})
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute(f"SET s3_endpoint = '{os.getenv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com'")
        conn.execute(f"SET s3_access_key_id = '{os.getenv('R2_ACCESS_KEY_ID')}'")
        conn.execute(f"SET s3_secret_access_key = '{os.getenv('R2_SECRET_ACCESS_KEY')}'")
        conn.execute(f"set s3_region = 'auto'")
        conn.execute(f"SET s3_url_style = 'path'")
        conn.execute(f"""
            COPY (SELECT * FROM Climate_Era5.Gold.fact_GeneralEra5)
            TO 's3://{os.getenv('R2_BUCKET_NAME')}/datasets/Institution/Era5/GeneralEra5.parquet'
            (FORMAT PARQUET)
        """)
        row_count = conn.sql("SELECT COUNT(*) FROM Climate_Era5.Gold.fact_GeneralEra5").fetchone()[0]
        conn.close()
        print(f"Exported {row_count} rows to R2")



    

    chain(retrieve_and_ingest_Era5(), 
          ingest_GDAM(),
          staging, 
          marts, 
          export_to_R2())


era5_in_taskflow()