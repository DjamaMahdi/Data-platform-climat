"""
### Use Airflow together with DuckDB and MotherDuck

This DAG takes the Agricultural daily data from the ERA5 dataset (1940 to present) on the Copernicus Climate Data Store, ingests it into MotherDuck using DuckDB's HTTPFS connector, 
transforms it with dbt, and finally exports the gold fact table to Cloudflare R2 as a Parquet file.
"""

