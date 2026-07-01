"""Agrometeorological ERA5 pipeline (ingest + export), Airflow-free.

Carries the logic that previously lived inside the `agri_era5_in_taskflow` DAG
tasks so it can run from both Airflow and the standalone CLI runner. Behaviour
is identical to the original DAG.
"""

import os
import time
import zipfile

import cdsapi
import duckdb
import xarray as xr
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

EXTRACT_DIR = "include/dataset"

AGRO_DATASET = "sis-agrometeorological-indicators"
START_YEAR = 2021
AGRO_LAG_DAYS = 15
MONTHS_PER_BATCH = 1            # 1 month per CDS request (safest for daily data)
DJIBOUTI_AREA = [12.7, 41.7, 10.9, 43.5]
CDS_MAX_RETRIES = 3
CDS_BASE_DELAY = 60             # seconds before first retry

AGRO_VARIABLE_SPECS = [
    {
        "variable": "2m_temperature",
        "statistics": ["24_hour_maximum", "24_hour_mean", "24_hour_minimum"],
        "time": None,
        "table": "Ag_temperature_2m",
    },
    {
        "variable": "10m_wind_speed",
        "statistics": ["24_hour_mean"],
        "time": None,
        "table": "Ag_wind_10m",
    },
    {
        "variable": "2m_relative_humidity",
        "statistics": None,
        "time": ["12_00"],
        "table": "Ag_humidity_2m",
    },
    {
        "variable": "precipitation_flux",
        "statistics": None,
        "time": None,
        "table": "Ag_precip_flux",
    },
    {
        "variable": "reference_evapotranspiration",
        "statistics": None,
        "time": None,
        "table": "Ag_reference_et",
    },
]


def get_agro_cutoff():
    """Returns the last safely available agro month as (year, month)."""
    cutoff = datetime.today() - timedelta(days=AGRO_LAG_DAYS)
    return cutoff.year, cutoff.month


def get_max_info(conn, table_name):
    """Returns the max date in the table, or None if table doesn't exist."""
    try:
        result = conn.sql(
            f'SELECT MAX(time) FROM Climate_Era5.Bronze."{table_name}"'
        ).fetchone()
        if result is None or result[0] is None:
            return None
        return result[0]
    except Exception:
        return None


def get_missing_months(max_date, cutoff_year, cutoff_month):
    """Returns (year, month) tuples that need downloading.

    Starts from max_date's own month (it may be partially loaded)
    or from START_YEAR if no data exists yet.
    """
    cutoff_date = datetime(cutoff_year, cutoff_month, 1)
    if max_date is None:
        start_date = datetime(START_YEAR, 1, 1)
    else:
        start_date = datetime(max_date.year, max_date.month, 1)
    months = []
    current = start_date
    while current <= cutoff_date:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def build_agro_cds_requests(missing_months, spec):
    """Build one CDS request per month for a single variable."""
    months_by_year = {}
    for y, m in missing_months:
        months_by_year.setdefault(y, []).append(m)

    base = {
        "variable": spec["variable"],
        "version": "2_0",
        "area": DJIBOUTI_AREA,
        "day": [str(d).zfill(2) for d in range(1, 32)],
        "data_format": "netcdf",
        "download_format": "zip",
    }
    if spec["statistics"] is not None:
        base["statistic"] = spec["statistics"]

    if spec["time"] is not None:
        base["time"] = spec["time"]

    requests = []
    for year, months in sorted(months_by_year.items()):
        for m in sorted(months):
            req = {**base, "year": [str(year)], "month": [str(m).zfill(2)]}
            requests.append(req)
    return requests


def retrieve_with_retry(client, dataset, request, target):
    """CDS API retrieve with exponential backoff.

    Retries on 403 (cost limits, often transient) and 5xx (server errors).
    Does NOT retry on 400 (bad request) or 401 (auth).
    """
    for attempt in range(CDS_MAX_RETRIES + 1):
        try:
            client.retrieve(dataset, request, target)
            return
        except Exception as e:
            error_str = str(e).lower()
            if "400" in error_str or "bad request" in error_str:
                raise
            if "401" in error_str or "unauthorized" in error_str:
                raise
            if attempt == CDS_MAX_RETRIES:
                raise
            wait = CDS_BASE_DELAY * (2 ** attempt)
            print(
                f"      Retry {attempt + 1}/{CDS_MAX_RETRIES}: "
                f"{e} — waiting {wait:.0f}s"
            )
            time.sleep(wait)


def extract_nc_files(download_path):
    """Handle zip downloads. Returns list of .nc paths."""
    with zipfile.ZipFile(download_path) as z:
        z.extractall(EXTRACT_DIR)
    os.remove(download_path)

    return sorted(
        os.path.join(EXTRACT_DIR, f)
        for f in os.listdir(EXTRACT_DIR)
        if f.endswith(".nc")
    )


def cleanup_nc_files():
    """Remove leftover .nc files from extract directory."""
    for f in os.listdir(EXTRACT_DIR):
        if f.endswith(".nc"):
            os.remove(os.path.join(EXTRACT_DIR, f))


def ingest(raise_on_failure=False):
    """
    Download agro indicators from CDS and ingest into MotherDuck Bronze.
    - Per-variable incremental tracking via MAX(time)
    - Append-only: skips months already loaded
    - Retry with exponential backoff on transient CDS failures
    - Next run automatically retries only failed variables

    raise_on_failure: when True (CLI/CI) raise if any variable failed so the
    job exits non-zero. When False (Airflow DAG) preserve the original
    behaviour of completing with a printed summary and retrying next run.
    """
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    cutoff_year, cutoff_month = get_agro_cutoff()
    download_target = os.path.join(EXTRACT_DIR, "agri_era5_download.zip")
    client = cdsapi.Client("https://cds.climate.copernicus.eu/api", os.getenv("CDSAPI_KEY"))
    conn = duckdb.connect(
        "md:Climate_Era5", config={"motherduck_token": os.getenv("MOTHERDUCK_TOKEN")}
    )

    failed = []
    succeeded = []
    skipped = []

    try:
        for spec in AGRO_VARIABLE_SPECS:
            var_name = spec["variable"]
            table_name = spec["table"]

            try:
                max_date = get_max_info(conn, table_name)
                missing = get_missing_months(max_date, cutoff_year, cutoff_month)

                if not missing:
                    print(f"  {var_name}: up to date")
                    skipped.append(var_name)
                    continue

                requests = build_agro_cds_requests(missing, spec)
                # Safety: if table exists, ALWAYS append — never overwrite
                need_create = max_date is None
                mode = "INCREMENTAL" if max_date else "FULL LOAD"
                print(f"\n{'=' * 60}")
                print(f"  {var_name} [{mode}] -> {table_name}")
                if max_date:
                    print(f"  Latest in DB: {max_date}")
                print(f"  {len(missing)} month(s) to download, {len(requests)} batch(es)")

                # ── Delete last partial month ONCE before downloading ──
                if max_date and not need_create:
                    conn.sql(
                        f"""
                        DELETE FROM Climate_Era5.Bronze."{table_name}"
                        WHERE EXTRACT(YEAR FROM time) = {max_date.year}
                          AND EXTRACT(MONTH FROM time) = {max_date.month}
                        """
                    )
                    print(f"  Removed partial month {max_date.year}-{max_date.month:02d} — will re-download")

                for batch_idx, request in enumerate(requests, start=1):
                    batch_year = request["year"][0]
                    batch_month = request["month"][0]
                    print(f"    Batch {batch_idx}/{len(requests)}: {batch_year}-{batch_month}")

                    retrieve_with_retry(client, AGRO_DATASET, request, download_target)
                    nc_paths = extract_nc_files(download_target)

                    # open_mfdataset handles both concat-along-time and merge-columns cases.
                    ds = xr.open_mfdataset(nc_paths, combine="by_coords")
                    merged = ds.to_dataframe().reset_index()
                    ds.close()
                    merged = merged.drop(columns=["crs"], errors="ignore")

                    if need_create:
                        conn.sql(
                            f"""
                            CREATE OR REPLACE TABLE
                                Climate_Era5.Bronze."{table_name}"
                            AS SELECT * FROM merged
                            """
                        )
                        need_create = False
                    else:
                        conn.sql(
                            f"""
                            INSERT INTO Climate_Era5.Bronze."{table_name}"
                            BY NAME SELECT * FROM merged
                            """
                        )

                    cleanup_nc_files()

                succeeded.append(var_name)
                print(f"  {var_name}: done")
            except Exception as e:
                print(f"  FAILED: {var_name} — {e}")
                failed.append((var_name, str(e)))
                cleanup_nc_files()

    finally:
        conn.close()

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"  Succeeded : {len(succeeded)}")
    print(f"  Skipped   : {len(skipped)} (already up to date)")
    print(f"  Failed    : {len(failed)}")
    if failed:
        for v, e in failed:
            print(f"  - {v}: {e}")
        print("Next run will retry only the failed variables.")
        if raise_on_failure:
            raise RuntimeError(f"{len(failed)} agro variable(s) failed: {[v for v, _ in failed]}")


def export():
    """Export the Agriculture gold table from MotherDuck to Cloudflare R2 as Parquet."""
    conn = duckdb.connect(
        "md:Climate_Era5", config={"motherduck_token": os.getenv("MOTHERDUCK_TOKEN")}
    )
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_endpoint = '{os.getenv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com'")
    conn.execute(f"SET s3_access_key_id = '{os.getenv('R2_ACCESS_KEY_ID')}'")
    conn.execute(f"SET s3_secret_access_key = '{os.getenv('R2_SECRET_ACCESS_KEY')}'")
    conn.execute("SET s3_region = 'auto'")
    conn.execute("SET s3_url_style = 'path'")
    bucket = os.getenv("R2_BUCKET_NAME")
    conn.execute(
        f"""
        COPY (SELECT * FROM Climate_Era5.Gold.fact_AgricultureEra5)
        TO 's3://{bucket}/Copernicus/Agriculture/AgricultureEra5.parquet'
        (FORMAT PARQUET)
        """
    )
    conn.close()
    print("Exported fact_AgricultureEra5 to R2.")
