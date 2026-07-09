# Data Platform Climat — Djibouti Climate Data Pipeline

An automated data platform that collects **ERA5 climate reanalysis data** for Djibouti from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/), transforms it through a medallion architecture (Bronze → Silver → Gold) in **MotherDuck**, and publishes analysis-ready datasets to **Cloudflare R2** and the [FAO climate data portal](https://data-climat.vercel.app).

## Architecture

```
Copernicus CDS API (ERA5)
        │  monthly download (NetCDF)
        ▼
MotherDuck — Bronze (raw tables)
        │  dbt transformations
        ▼
MotherDuck — Silver (staging views) → Gold (fact tables)
        │  DuckDB COPY (Parquet, ZSTD)
        ▼
Cloudflare R2 (Parquet + XLSX)
        │  Supabase registration
        ▼
Data portal (data-climat.vercel.app)
```

The whole flow is streamed in memory — NetCDF → xarray → pandas → DuckDB → R2 — with no intermediate CSV or temp files.

## Pipelines

| Pipeline | Data | Frequency | Resolution |
|---|---|---|---|
| **General ERA5** | Temperature, precipitation, wind, radiation, evaporation, pressure, sea surface temperature, waves | Monthly means, 1940 → present | 0.25° grid (56 points) |
| **Agriculture ERA5** | 2m temperature (min/mean/max), wind, humidity, precipitation flux, reference evapotranspiration | Daily, 2021 → present | 0.1° grid (324 points) |

Both pipelines map grid points to Djibouti's **6 administrative regions** (GADM Level 1) via a DuckDB spatial join, so climate indicators can be analyzed per region.

## How it works — step by step

1. **Incremental ingestion** — the pipeline queries MotherDuck for the latest loaded month, computes the missing months up to the ERA5 publication cutoff (today − 2 months), and downloads only what's missing from the CDS API in batched, cartesian-safe requests. An empty table triggers a full historical load.
2. **Bronze load** — NetCDF files are read with xarray and inserted directly into MotherDuck Bronze tables (`General1`, `General2`, `General3`, `Ag_*`). GADM region polygons are loaded from GeoJSON with DuckDB's `spatial` extension.
3. **dbt transformations** — staging views (Silver) clean and key the raw data (unit conversions, surrogate keys); intermediate models join variables, dates, and regions; mart tables (Gold) produce `fact_GeneralEra5`, `fact_AgricultureEra5`, and `dim_vagues`.
4. **Export to R2** — DuckDB's `httpfs` extension streams the Gold tables straight from MotherDuck to Cloudflare R2 as ZSTD-compressed Parquet (`COPY ... TO 's3://medd/...'`).
5. **Portal sync** — `portal_sync.py` converts the Parquet exports to XLSX on R2 and upserts the dataset records in the portal's Supabase database, preserving any edits made by admins in the portal UI.

## Running the pipeline

### Option A — GitHub Actions (production)

The workflow [`.github/workflows/monthly-era5.yml`](.github/workflows/monthly-era5.yml) runs the General pipeline monthly (cron `0 3 6 * *`) and can be triggered manually from the Actions tab. Secrets are stored in the repository **Environment `Secret`**.

### Option B — CLI

```bash
python scripts/run_pipeline.py --target general      # General ERA5 only
python scripts/run_pipeline.py --target agriculture  # Agriculture only
python scripts/run_pipeline.py --target all
```

Each target runs: load regions → ingest → `dbt run` → export → portal sync.

### Option C — Airflow (local, via Astronomer)

```bash
astro dev start
```

DAGs: `era5_in_taskflow` (General, monthly) and `agri_era5_in_taskflow` (Agriculture). Both are thin wrappers around the shared modules in `include/pipeline/`.

## Configuration

Required environment variables / secrets:

| Variable | Purpose |
|---|---|
| `MOTHERDUCK_TOKEN` | MotherDuck database access |
| `CDSAPI_KEY` | Copernicus CDS API key |
| `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME` | Cloudflare R2 export |
| `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` | Portal dataset registration |
| `VERCEL_DEPLOY_HOOK` *(optional)* | Redeploy the portal after sync |

## Project structure

```
├── dags/                        # Airflow DAGs (thin wrappers)
│   ├── General_Era5.py
│   └── Agriculture_Era5.py
├── include/
│   ├── pipeline/                # Shared pipeline logic (no Airflow deps)
│   │   ├── general_pipeline.py  # ingest + export (General ERA5)
│   │   ├── agriculture_pipeline.py
│   │   ├── regions.py           # GADM region loading
│   │   └── portal_sync.py       # Parquet → XLSX + Supabase upsert
│   ├── dbt/                     # dbt project (staging / intermediate / marts)
│   └── dataset/                 # GADM GeoJSON reference data
├── scripts/
│   └── run_pipeline.py          # CLI runner (used by CI)
├── .github/workflows/
│   └── monthly-era5.yml         # Monthly production run
├── Dockerfile                   # Astro Runtime image
└── requirements.txt
```

## Tech stack

- **Orchestration:** GitHub Actions (production) · Apache Airflow / Astronomer (local)
- **Storage & compute:** MotherDuck (DuckDB cloud), Cloudflare R2
- **Transformation:** dbt (duckdb adapter), DuckDB `spatial` + `httpfs` extensions
- **Ingestion:** CDS API (`cdsapi`), xarray, pandas
- **Portal:** Next.js on Vercel, Supabase (PostgREST)

## Data quality notes

- ERA5 monthly means publish with a ~2-month lag (`ERA5_LAG_MONTHS = 2`); the pipeline never requests restricted ERA5T monthly data.
- Ingestion is idempotent: already-loaded months are skipped, so re-runs never create duplicates.
- Temperatures were cross-validated against published climate references (Weather Spark, climate-data.org, World Bank) — see `CLAUDE.md` for details.
