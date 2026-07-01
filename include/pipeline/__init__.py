"""Pure (non-Airflow) ERA5 pipeline logic.

These modules carry the data-fetch / ingest / export logic so it can run both
from the Airflow DAGs (`dags/*.py`) and from the standalone CLI runner
(`scripts/run_pipeline.py`) used by the monthly GitHub Action. They must NOT
import `airflow` or `cosmos`.
"""
