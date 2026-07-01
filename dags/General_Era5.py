"""
### Use Airflow together with DuckDB and MotherDuck

This DAG takes the Monthly data from the ERA5 dataset (1940 to present) on the Copernicus Climate Data Store, ingests it into MotherDuck using DuckDB's HTTPFS connector,
transforms it with dbt, and finally exports the gold fact table to Cloudflare R2 as a Parquet file.

The data-fetch / ingest / export logic lives in `include/pipeline/general_pipeline.py`
so it can also run Airflow-free from the monthly GitHub Action
(`scripts/run_pipeline.py`). This DAG is a thin wrapper — behaviour is unchanged.
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.pipeline import general_pipeline


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
        general_pipeline.ingest()

    staging = DbtTaskGroup(
        group_id='staging',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging/General']
        )
    )

    marts = DbtTaskGroup(
        group_id='marts',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts/General']
        )

    )

    @task
    def export_to_R2():
        "Exporting the gold fact table into cloudfare as a parquet file for storage"
        general_pipeline.export()

    chain(retrieve_and_ingest_Era5(),
          staging,
          marts,
          export_to_R2())


era5_in_taskflow()
