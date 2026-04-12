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
geojson_path = 'include/dataset/Djibouti_Region.json'


@dag(start_date=datetime(2026, 1, 1),
     schedule=None,
     catchup=False,
     tags=['duckdb'],
     max_active_tasks=1,
)

def Region_in_taskflow():

    @task
    def ingest_GDAM():
        "LOAD GDAM LVL 1 Geojson into motherduck (bronze schema) as a reference table "
        conn = duckdb.connect("md:Climate_Era5", config = {"motherduck_token" : MOTHERDUCK_TOKEN})
        conn.execute("INSTALL Spatial")
        conn.execute("LOAD Spatial")
        conn.execute(f"""
            CREATE OR REPLACE TABLE Climate_Era5.Bronze.Djibouti_Region 
            AS SELECT 
               NAME_1,
               geom
               FROM ST_Read('{geojson_path}')
            """)
        conn.close()
        print("GDAM ingestion complete.")

        ingest_GDAM()

Region_in_taskflow()