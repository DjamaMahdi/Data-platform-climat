"""
### Agrometeorological Indicators — CDS to MotherDuck Bronze

Fetches daily agrometeorological indicators from the Copernicus CDS API
(`sis-agrometeorological-indicators`) and ingests each variable into its
own Bronze table (Ag_xxx) in MotherDuck.

The data-fetch / ingest / export logic lives in
`include/pipeline/agriculture_pipeline.py` so it can also run Airflow-free from
the monthly GitHub Action (`scripts/run_pipeline.py`). This DAG is a thin
wrapper — behaviour is unchanged.
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.pipeline import agriculture_pipeline


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['duckdb', 'agriculture', 'era5'],
    max_active_tasks=1,
)
def agri_era5_in_taskflow():

    @task
    def retrieve_and_ingest_AgriEra5():
        "Download agro indicators from CDS and ingest into MotherDuck Bronze."
        agriculture_pipeline.ingest()

    staging = DbtTaskGroup(
        group_id='staging',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging/Agriculture']
        )
    )

    marts = DbtTaskGroup(
        group_id='marts',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts/Agriculture']
        )

    )

    @task
    def export_to_R2():
        "Exporting the gold fact table into cloudfare as a parquet file for storage"
        agriculture_pipeline.export()

    chain(
        retrieve_and_ingest_AgriEra5(),
        staging,
        marts,
        export_to_R2()
    )


agri_era5_in_taskflow()
