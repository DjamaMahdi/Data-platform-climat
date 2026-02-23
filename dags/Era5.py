"""
### Use Airflow together with DuckDB and MotherDuck

This DAG shows examples of how to interact with DuckDB and MotherDuck from within 
TaskFlow tasks. The tasks interacting with MotherDuck will need a MotherDuck token.
"""

import cdsapi
import os
import netCDF4 as nc
import pandas as pd
import duckdb
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime

#CSV_PATH = "include/dataset/Online_Retail.csv"
#CSV_PATH2 = "include/dataset/Country.csv"
#LOCAL_DUCKDB_STORAGE_PATH = "include/dataset/Online_Retail.duckdb"
#MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
CDSAPI_KEY = os.getenv("CDSAPI_KEY")

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
            "volumetric_soil_water_layer_2"
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
            "download_format": "unarchived",
             "area": [12.7, 41.7, 10.9, 43.5]
            }
        target = 'Djibouti_era5.netcdf'
        client = cdsapi.Client("https://cds.climate.copernicus.eu/api",CDSAPI_KEY)
        df=nc.Dataset(client.retrieve(dataset, request,target))
        client.close()
        print(f"DataFrame created : {df.shape[0]} rows, {df.shape[1]} columns")


    retrieve_database()


era5_in_taskflow()