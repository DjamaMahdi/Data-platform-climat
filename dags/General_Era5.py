"""
### Use Airflow together with DuckDB and MotherDuck

This DAG shows examples of how to interact with DuckDB and MotherDuck from within 
TaskFlow tasks. The tasks interacting with MotherDuck will need a MotherDuck token.
"""

import cdsapi
import zipfile
import os
import pandas as pd
import duckdb
import xarray as xr
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
CDSAPI_KEY = os.getenv("CDSAPI_KEY")
extract_dir = 'include/dataset'
os.makedirs(extract_dir, exist_ok=True)

@dag(start_date=datetime(2026, 1, 1),
     schedule=None, 
     catchup=False,
     tags=['duckdb'],
     max_active_tasks=1,            
)

def era5_in_taskflow():
 
    @task
    def retrieve_database():
        "Retrieve ERA5 data and store it in a local file"

        target = 'include/dataset/Djibouti_era5.zip'
        client = cdsapi.Client("https://cds.climate.copernicus.eu/api",CDSAPI_KEY)
 
        dataset = "reanalysis-era5-single-levels-monthly-means"
        request = {
              "product_type": ["monthly_averaged_reanalysis"],
             "variable": [
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
             ],
            "year": ["2025", "2026"],
            "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
            ],
            "time": ["00:00"],
            "data_format": "netcdf",
            "download_format": "zip",
             "area": [12.7, 41.7, 10.9, 43.5]
            }
        # Download
        client.retrieve(dataset, request, target)
        # Unzip
        with zipfile.ZipFile(target) as z:
            z.extractall(extract_dir)
        # Clean up zip
        os.remove(target)
         # Convert all .nc files to dataframes
        nc_files = [f for f in os.listdir(extract_dir) if f.endswith('.nc')]

        for i, nc_file in enumerate(nc_files, start=1):
           ds = xr.open_dataset(os.path.join(extract_dir, nc_file))
           df = ds.to_dataframe().reset_index()
           csv_path = os.path.join(extract_dir, f"Table{i}.csv")
           df.to_csv(csv_path, index=False)
           print(f"Table{i}: {df.shape[0]} rows")
           os.remove(os.path.join(extract_dir, nc_file))
           
    @task
    def ingest_motherduck_database():
        "Ingest data in a MotherDuck database to store the ERA5 data in it"
        # Connect to MotherDuck (this will create a new database file if it doesn't exist)
        conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token" : MOTHERDUCK_TOKEN})
        csv_files = [f for f in os.listdir(extract_dir) if f.endswith('.csv')]
        for csv_file in csv_files:
           name = csv_file.replace('.csv', '')
           filepath = os.path.join(extract_dir, csv_file)
           print(f"Copying {name}...")
           conn.sql(f"""
            CREATE OR REPLACE TABLE Climate_Era5.Bronze.{name}
            AS SELECT * FROM read_csv_auto('{filepath}')
            """)

        conn.close()
    

    chain(retrieve_database(), 
          ingest_motherduck_database())


era5_in_taskflow()