"""Load the GADM Level-1 region polygons into MotherDuck Bronze.

The dbt spatial joins read `Climate_Era5.Bronze.Djibouti_Region` (see
`models/staging/General/stg_Djibouti_Region.sql`). This loader is idempotent
(`CREATE OR REPLACE`) and safe to run on every pipeline execution — it protects
CI from a missing Bronze region table without touching the climate data.
"""

import os

import duckdb

# Relative path works in both Airflow (cwd=/usr/local/airflow) and CI (cwd=repo root)
GEOJSON_PATH = "include/dataset/Djibouti_Region.json"
REGION_TABLE = "Climate_Era5.Bronze.Djibouti_Region"


def load_regions():
    """(Re)create Bronze.Djibouti_Region from the GADM GeoJSON. Idempotent."""
    token = os.getenv("MOTHERDUCK_TOKEN")
    conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token": token})
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {REGION_TABLE} AS
            SELECT * FROM ST_Read('{GEOJSON_PATH}')
            """
        )
        row_count = conn.sql(f"SELECT COUNT(*) FROM {REGION_TABLE}").fetchone()[0]
        print(f"Loaded {row_count} regions into {REGION_TABLE}")
    finally:
        conn.close()
