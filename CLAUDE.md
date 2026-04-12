# Data Platform Climat — Project Notes

## DAG: General_Era5.py

### ERA5 Incremental Logic (updated 2026-03-12)

The DAG `dags/General_Era5.py` fetches ERA5 monthly climate data for Djibouti via the Copernicus CDS API, ingests into MotherDuck, then runs dbt staging and marts.

**Key constants:**
- `START_YEAR = 1940` — start of full historical load (ERA5 reanalysis available from 1940)
- `ERA5_LAG_MONTHS = 2` — ERA5 monthly means publish ~1 month after month ends; 2-month lag is safe
- `YEARS_PER_BATCH = 10` — max years per CDS API request to avoid oversized requests

**How the incremental logic works:**
1. `get_latest_month_in_motherduck()` queries `MAX(valid_time)` from `Climate_Era5.Bronze.General1` to find the latest loaded `(year, month)`, or `None` if table is empty/missing
2. `get_missing_months(latest_loaded)` computes all `(year, month)` tuples between `latest_loaded + 1` and the ERA5 cutoff (`today - 2 months`)
3. If **no data** (`None`) → full load: downloads all months from `START_YEAR` to cutoff
4. If **data exists** → incremental: downloads only the missing months (typically 1 month)
5. If **no missing months** → skips download and ingestion entirely (no duplicates)
6. Ingestion uses `INSERT INTO` (incremental) or `CREATE OR REPLACE TABLE` (full load, first batch only)

**Batched CDS requests (added 2026-03-12):**
- `build_cds_requests()` splits missing months into cartesian-safe batches
- Full years (all 12 months) are chunked into groups of 10 years
- Partial years (< 12 months, e.g. 2026 with only Jan) get their own request
- Prevents the cartesian product bug: old code sent all years × all months to CDS, which included invalid future months
- For a full load from 1940: ~9 batches (1940-49, 1950-59, ..., 2020-2025, 2026 partial)

**Single-task architecture (performance optimization):**
- `retrieve_and_ingest()` is a single Airflow task that downloads AND ingests in a batch loop
- NetCDF → xarray → pandas DataFrame → DuckDB reads DataFrame **directly in memory** (`SELECT * FROM df`)
- No CSV intermediate files — eliminates disk I/O and parsing overhead
- Single MotherDuck connection for all tables and batches
- .nc files sorted before ingestion for consistent General1/General2/... mapping across batches

**ERA5 variables retrieved (updated 2026-03-12):**
- 2m_dewpoint_temperature, 2m_temperature, total_precipitation
- 10m_wind_speed, surface_solar_radiation_downwards, evaporation
- potential_evaporation, runoff, soil_temperature_level_1, volumetric_soil_water_layer_1
- mean_sea_level_pressure, surface_pressure (atmospheric pressure — same grid as above)
- sea_surface_temperature (ocean only, NaN over land)
- mean_wave_direction, mean_wave_period (wave model — may create a separate .nc file / General3 table)

**NetCDF column names (for reference):**
- General1 (flux/accumulated): tp, ssrd, e, pev, ro
- General2 (instantaneous/state): d2m, t2m, si10, stl1, swvl1
- New atmospheric: msl (mean sea level pressure), sp (surface pressure), sst (sea surface temp)
- New wave: mwd (mean wave direction), mwp (mean wave period)
- Note: CDS API may redistribute columns across .nc files when new variables are added. Check logs after re-run.

**Area:** Djibouti bounding box `[12.7, 41.7, 10.9, 43.5]`

**Dataset:** `reanalysis-era5-single-levels-monthly-means`
**Product type:** `monthly_averaged_reanalysis`

### ERA5 publication lag

- ERA5 **monthly means** (`reanalysis-era5-single-levels-monthly-means`) are the **final** product. ERA5T (preliminary) monthly means are **restricted** — requesting them causes `AccessError: Restricted access to ERA5T/ERA5LANDT Monthly means`.
- Safe lag is 2 months (`ERA5_LAG_MONTHS = 2`). January 2026 data was confirmed available by late February 2026.
- ERA5 **hourly** data (ERA5T) is available with ~5 day lag, but is a different dataset and much larger.

### Cartesian product bug & fix (2026-03-12)

**Problem:** Changing `START_YEAR` from 2025 to 1940 broke the DAG. `build_cds_request()` built a cartesian product of all unique years × all unique months. With 86+ years, year 2026 got paired with months 02-12 (which don't exist yet), triggering ERA5T restricted access error. Also, a single request spanning 86 years was too large.

**Fix:** Replaced `build_cds_request()` (singular) with `build_cds_requests()` (plural). Groups missing months by year, chunks full years into batches of 10, gives partial years their own request. Each batch is cartesian-safe.

### Why the original DAG broke (2026-03-08)
The request hardcoded `year: ["2025", "2026"]` with all 12 months, causing the MARS server to reject months not yet published (ERA5T restricted access error). Fixed by dynamic date computation.

### Data loss incident & fix (2026-03-08)

**Root cause:** `dags/__pycache__/General_Era5 - Copie.cpython-312.pyc` — stale bytecode from an old copy of the DAG. It contained `CREATE OR REPLACE TABLE` (always replaces, ignoring `incremental` flag). Airflow loaded this cached bytecode for `ingest_motherduck_database`, overwriting all historical data with only the latest month.

**Fix applied:**
1. Moved bad `.pyc` to project root (do not put back in `dags/__pycache__/`)
2. Dropped `Climate_Era5.Bronze.General1` and `General2` to force a full reload
3. Triggered DAG → ran FULL LOAD → 672 rows x 2 tables restored (12 months x 56 grid points, Jan-Dec 2025)

**Rule:** Never leave `General_Era5 - Copie.py` (or any file defining the same `dag_id = era5_in_taskflow`) inside `dags/` or `dags/__pycache__/`. It silently overrides the correct ingest logic.

### Temperature cross-check (2026-03-12)

ERA5 monthly mean temperatures for Djibouti were cross-checked against published climate data (Weather Spark, climate-data.org, World Bank). Results match closely:
- January spatial mean: 23.7°C (ERA5) vs 23.7°C (reference)
- July spatial mean: 33.0°C (ERA5) vs 34.3°C (reference — Djibouti City only, hotter than spatial avg)
- Annual mean (2020-2025): 28.8-29.9°C — consistent with known ~29°C
- Values appear "low" because they are 24h means (day+night) averaged across 56 grid points including highland areas (Goda Mountains ~1500m)
- No data quality issues found. Zero duplicates confirmed across all layers.

### Full load stats (2026-03-12, START_YEAR=1940)

- Bronze.General1: 57848 rows, 1033 distinct months, 56 grid points
- Bronze.General2: 57848 rows, same structure
- Date range: 1940-01-01 to 2026-01-01
- expver: `0001` (final ERA5) for 1940-2025, `0005` (ERA5T) for Jan 2026 only
- Zero duplicates in Bronze, Silver, and Gold layers

## MotherDuck
- Database: `Climate_Era5` | Schema: `Bronze` | Tables: `General1`, `General2`, `General3`, `Regions_GADM1`
- Schemas: `Bronze` (raw), `Silver` (staging views), `Gold` (mart tables)
- Also: `MEDD` (DuckLake), `my_db`
- Claude can connect directly via MCP MotherDuck tools
- To force a full reload: drop all Bronze tables → trigger DAG (get_latest_month returns None → FULL LOAD)

## dbt models
- **Sources:** `include/dbt/models/staging/Era5/Era5_General_sources.yml` — references `Bronze.General1`, `Bronze.General2`, `Bronze.General3`, `Bronze.Regions_GADM1`
- **Staging:** `stg_General_1.sql` (precipitation, radiation, evaporation, runoff), `stg_General_2.sql` (temperature, wind, soil), `stg_General_3.sql` (wave data), `stg_Grid_Region.sql` (grid point to region mapping)
- **Intermediate:** `int_datetime.sql` (ephemeral — extracts year, month, day, weekday from valid_time)
- **Marts:** `fact_GeneralEra5.sql` (joins stg1 + stg2 + stg3 + int_datetime + stg_Grid_Region on surrogate key / lat+lon)
- Staging = Silver schema (views), Marts = Gold schema (tables), Intermediate = ephemeral
- **Note:** dbt models will need updating after new variables are added and tables are reloaded — new columns (msl, sp, sst, mwd, mwp) need to be added to staging/mart SQL and sources.yml if new tables are created

## Session Changelog — 2026-03-12

All code changes made to `dags/General_Era5.py` in this session:

### 1. Batching fix for START_YEAR=1940
- **Removed:** `build_cds_request()` (singular) — built a single request with cartesian product of all years × all months
- **Added:** `build_cds_requests()` (plural, line 94) — returns a list of cartesian-safe request dicts
  - Groups missing months by year
  - Full years (all 12 months) chunked into batches of `YEARS_PER_BATCH = 10`
  - Partial years (< 12 months) get their own request
- **Added:** `ERA5_VARIABLES` constant (line 78) — extracted variable list to module level
- **Added:** `YEARS_PER_BATCH = 10` constant (line 96)

### 2. Updated `retrieve_and_ingest()` for batch loop
- Now loops over all requests from `build_cds_requests()`
- `first_batch` flag: first batch of a full load uses `CREATE OR REPLACE TABLE`, all subsequent batches use `INSERT INTO`
- `.nc` files are `sorted()` before ingestion (line 190) for consistent General1/General2 mapping across batches
- Single MotherDuck connection reused across all batches

### 3. Added 5 new ERA5 variables (line 78-93)
- `mean_sea_level_pressure` (msl), `surface_pressure` (sp) — atmospheric, same grid
- `sea_surface_temperature` (sst) — ocean only, NaN over land
- `mean_wave_direction` (mwd), `mean_wave_period` (mwp) — wave model, may produce extra .nc files
- dbt models NOT touched — to be updated after next full reload reveals column-to-table mapping

### 4. Added column logging (line 196)
- `print(f"  {table_name} columns: {list(df.columns)}")` — logs which columns land in which table during ingestion

### 5. Data quality verification (queries only, no code changes)
- Confirmed Bronze.General1 & General2: 57848 rows each, 1033 months × 56 grid points, 1940-01 to 2026-01
- Zero duplicates in Bronze, Silver (stg_General_1, stg_General_2), and Gold (fact_GeneralEra5)
- Cross-checked ERA5 temperatures against Weather Spark, climate-data.org, World Bank — values match published climate data
- `expver` split: `0001` (final ERA5) for 1940-2025, `0005` (ERA5T) for Jan 2026

### Pending after next DAG run
- Drop all Bronze tables and trigger full reload (new variables need fresh data)
- Check logs to see which columns go into which table (General1/2/3/...)
- Update `Era5_General_sources.yml` if new tables are created (General3+)
- Update `stg_General_1.sql`, `stg_General_2.sql` (or create new staging models) for new columns
- Update `fact_GeneralEra5.sql` to include new fields

## Parquet Export to Cloudflare R2 (added 2026-03-15)

**Purpose:** Final task in the DAG that exports the Gold `fact_GeneralEra5` table directly from MotherDuck to Cloudflare R2 as Parquet.

**R2 bucket:** `medd`
**R2 object key:** `s3://medd/dataset/Institution/Era5/General_Era5.parquet`
**R2 endpoint:** `<R2_ACCOUNT_ID>.r2.cloudflarestorage.com`

**How it works (zero intermediate storage):**
1. `export_to_r2()` is a `@task` that runs after `marts` completes
2. Connects to MotherDuck, configures DuckDB's built-in httpfs S3 layer to point at R2
3. `COPY ... TO 's3://medd/...' (FORMAT PARQUET, COMPRESSION ZSTD)` streams data directly from MotherDuck → R2
4. No temp files, no RAM buffers — the Airflow container is just a passthrough
5. Chained last: `chain(ingested, staging, marts, exported)`

**Why Parquet over Excel:**
- XLSX is a ZIP archive — cannot be streamed, always requires full file in memory or on disk
- Parquet is streamable, columnar, and 5-10x smaller (ZSTD compressed)
- DuckDB writes Parquet directly to S3-compatible storage via httpfs — no boto3 or openpyxl needed

**DAG pipeline order:** `retrieve_and_ingest → load_regions_to_bronze → staging (dbt) → marts (dbt) → export_to_r2`

**No new dependencies** — DuckDB's httpfs is bundled. No changes to `requirements.txt`.

**DuckDB S3 settings used in task:**
- `s3_endpoint` → R2 account endpoint
- `s3_url_style = 'path'` → R2 uses path-style URLs (not virtual-hosted)
- `s3_region = 'auto'` → R2 expects `auto`

**R2 credentials in `.env`:**
- `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`

## GADM Region Mapping (added 2026-03-15)

**Purpose:** Map ERA5 grid points to Djibouti's 6 administrative regions (GADM Level 1) so climate data can be analyzed by region.

**GeoJSON source:** `include/dataset/Djibouti_Region.json` (GADM Level 1)
**6 regions:** AliSabieh, Arta, Dikhil, Djiboutii, Obock, Tadjoura

**Architecture — two-step approach:**

### Step 1: DAG task `load_regions_to_bronze()` (in `General_Era5.py`)
- Loads the raw GeoJSON into `Bronze.Regions_GADM1` using DuckDB's `ST_Read()`
- Requires `INSTALL spatial` + `LOAD spatial` before `ST_Read()` works
- `CREATE OR REPLACE TABLE` — idempotent, 6 rows (one per region)
- Columns: NAME_1, GID_1, ISO_1, COUNTRY, geom (MULTIPOLYGON), and other GADM properties
- GeoJSON path inside Docker: `/usr/local/airflow/include/dataset/Djibouti_Region.json`
- Chain position: after `retrieve_and_ingest`, before `staging`
- Code:
```python
@task
def load_regions_to_bronze():
    """Load GADM Level 1 GeoJSON into Bronze as raw reference data."""
    conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token": MOTHERDUCK_TOKEN})
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    geojson_path = '/usr/local/airflow/include/dataset/Djibouti_Region.json'
    conn.execute(f"""
        CREATE OR REPLACE TABLE Climate_Era5.Bronze.Regions_GADM1 AS
        SELECT * FROM ST_Read('{geojson_path}')
    """)
    row_count = conn.sql("SELECT COUNT(*) FROM Climate_Era5.Bronze.Regions_GADM1").fetchone()[0]
    conn.close()
    print(f"Loaded {row_count} regions into Bronze.Regions_GADM1")
```

### Step 2: Spatial join in dbt (user implements)
- All spatial logic (ST_Contains, ST_Point, point-in-polygon) handled in dbt models, not the DAG
- Requires `spatial` extension configured in `profiles.yml` (add to extensions list)
- `stg_Grid_Region.sql`: staging model that does the spatial join between grid points and region polygons
- `ST_Contains(geom, ST_Point(longitude, latitude))` — note: argument order is (x, y) = (lon, lat)
- LEFT JOIN so sea/outside-Djibouti points get NULL region
- `fact_GeneralEra5.sql`: LEFT JOIN stg_Grid_Region on (latitude, longitude) to add region_name, gid_1

**Updated DAG chain:**
```python
ingested = retrieve_and_ingest()
regions_loaded = load_regions_to_bronze()
exported = export_to_r2()
chain(ingested, regions_loaded, staging, marts, exported)
```

**Known limitation:** Current fact table uses INNER JOIN with stg_General_3 (wave data), which keeps only 16 ocean grid points out of 56 total. Most ocean points will have NULL region. To get land-based regional analysis, change that INNER JOIN to LEFT JOIN in fact_GeneralEra5.sql.

## Session Changelog — 2026-04-01

### 1. Date format mismatch bug & fix in `int_datetime.sql`

**Problem:** `Gold.fact_AgricultureEra5` had only 236k rows instead of ~617k. Every month showed exactly 12 days instead of 28-31.

**Root cause:** Date format mismatch between staging models and `int_datetime.sql`:
- Staging models (`stg_Ag_*.sql`): `strftime("time", '%d/%m/%Y')` → day/month/year (e.g. Jan 5 = `"05/01/2021"`)
- `int_datetime.sql`: `strftime(date_day, '%m/%d/%Y')` → month/day/year (e.g. Jan 5 = `"01/05/2021"`)

**Impact on INNER JOIN (`t1.datetime_id = dt.datetime_id`):**
- **Days 1-12**: join succeeds but dates are **scrambled** (day/month swapped) — staging's Jan 5 (`"05/01/2021"`) matches int_datetime's May 1 (`"05/01/2021"`)
- **Days 13-31**: join fails (no month 13-31 exists) → rows **silently dropped** (~60% of data)
- Result: 236k rows instead of 617k, with wrong datetime values on every surviving row

**Fix:** Changed `int_datetime.sql` line 14 from `'%m/%d/%Y'` to `'%d/%m/%Y'` to match staging models.

**Rule:** All models generating `datetime_id` must use the same `strftime` format. Current standard is `'%d/%m/%Y'` (day/month/year).

### 2. Region mapping: INNER JOIN → LEFT JOIN in `int_AgricultureERA5.sql`

**Problem:** With INNER JOIN on `int_Ag_mappingRegion`, fact table dropped from 617k to ~430k rows — 98 out of 324 grid points are ocean/outside Djibouti and have no region match.

**Grid point breakdown:**
- **324 total** grid points in the Djibouti bounding box (0.1° resolution)
- **226 land** grid points that overlap with a Djibouti region
- **98 ocean/outside** grid points with no region match

**Fix:** Changed to `LEFT JOIN` on `int_Ag_mappingRegion` in `int_AgricultureERA5.sql` line 26. Ocean points now kept with `Region = NULL`.

### 3. Agriculture dbt models (current state)

**Staging (Silver views):**
- `stg_Ag_temperature_2m.sql` — Kelvin→Celsius, surrogate keys (`key_id` = lat+lon+datetime_id, `coord_id` = lat+lon)
- `stg_Ag_wind_10m.sql`, `stg_Ag_humidity_2m.sql` (/100→decimal), `stg_Ag_precip_flux.sql`, `stg_Ag_reference_et.sql` — same key structure
- `stg_Djibouti_Region.sql` — region names + geometries from `Bronze.Djibouti_Region`

**Intermediate (ephemeral):**
- `int_Ag_mappingRegion.sql` — spatial join: 0.1°×0.1° grid cell envelopes × regions, assigns each grid point to region with largest overlap
- `int_AgricultureERA5.sql` — INNER JOINs 5 staging tables on `key_id`, LEFT JOIN region on `coord_id`, INNER JOIN `int_datetime` on `datetime_id`
- `int_datetime.sql` — shared date spine (1940 to today), format `'%d/%m/%Y'`

**Marts (Gold table):**
- `fact_AgricultureEra5.sql` — columns: datetime, year, month_year, lat, lon, Region, Temp_Max/Mean/Min_24h, Wind_Speed, Reference_ET, Precipitation_Flux, Relative_Humidity

### 4. Agriculture Bronze table status (2026-04-01)

| Table | Rows | Date Range | Grid Points |
|---|---|---|---|
| `Ag_temperature_2m` | 617,868 | 2021-01-01 to 2026-03-22 | 324 |
| `Ag_reference_et` | 617,544 | 2021-01-01 to 2026-03-21 | 324 |
| `Ag_precip_flux` | 616,896 | 2021-01-01 to 2026-03-19 | 324 |
| `Ag_humidity_2m` | 616,572 | 2021-01-01 to 2026-03-18 | 324 |
| `Ag_wind_10m` | 616,572 | 2021-01-01 to 2026-03-18 | 324 |

**Gold fact table:** ~617k rows (LEFT JOIN on region), date range 2021-01-01 to 2026-03-18 (bounded by common minimum across 5 tables), 324 grid points (226 with region, 98 ocean with `Region = NULL`).

### 5. Agriculture DAG: current 5 variables

DAG reduced from 14 to 5 variables:

| Variable | Statistics | Bronze Table |
|---|---|---|
| `2m_temperature` | 24h max/mean/min | `Ag_temperature_2m` |
| `10m_wind_speed` | 24_hour_mean | `Ag_wind_10m` |
| `2m_relative_humidity` | time=12_00 | `Ag_humidity_2m` |
| `precipitation_flux` | none | `Ag_precip_flux` |
| `reference_evapotranspiration` | none | `Ag_reference_et` |

### Pending work
- [ ] Consider materializing Silver as tables (not views) — millions of rows per staging model
- [ ] Add R2 export task for the agriculture fact table
- [ ] Uncomment export task in Agriculture DAG chain
- [ ] Add remaining 9 agro variables back to DAG when CDS quota allows
- [ ] Create dbt sources YAML for Agriculture Bronze tables

## Airflow / Docker
- Scheduler container: `data-platform-climat_d199a1-scheduler-1`
- Trigger DAG: `docker exec <scheduler> airflow dags trigger era5_in_taskflow`
- Check task states: query Postgres → `docker exec data-platform-climat_d199a1-postgres-1 psql -U postgres -c "SELECT task_id, state, start_date, end_date FROM task_instance WHERE dag_id='era5_in_taskflow' ORDER BY start_date;"`
- Task logs: `/usr/local/airflow/logs/dag_id=era5_in_taskflow/run_id=.../task_id=.../attempt=1.log`
