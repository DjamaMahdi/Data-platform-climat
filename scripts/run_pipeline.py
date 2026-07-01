#!/usr/bin/env python3
"""Airflow-free runner for the ERA5 pipelines (used by the monthly GitHub Action).

For each selected target it runs:  regions -> ingest -> dbt run -> export to R2.

Usage:
    python scripts/run_pipeline.py --target all
    python scripts/run_pipeline.py --target general
    python scripts/run_pipeline.py --target agriculture

Required env vars: MOTHERDUCK_TOKEN, CDSAPI_KEY, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME.

Run from the repository root so the relative paths (include/dbt, include/dataset)
resolve correctly.
"""

import argparse
import os
import subprocess
import sys

# Ensure the repo root is importable (so `include.pipeline` resolves) regardless
# of how this script is invoked, and run dbt with the repo root as cwd.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

from include.pipeline import agriculture_pipeline, general_pipeline, portal_sync, regions  # noqa: E402

DBT_PROJECT_DIR = "include/dbt"

REQUIRED_ENV = [
    "MOTHERDUCK_TOKEN",
    "CDSAPI_KEY",
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET_NAME",
]

# dbt model selectors per target
DBT_SELECTORS = {
    "general": ["path:models/staging/General", "path:models/marts/General"],
    "agriculture": ["path:models/staging/Agriculture", "path:models/marts/Agriculture"],
}


def check_env():
    missing = [v for v in REQUIRED_ENV if not os.getenv(v)]
    if missing:
        sys.exit(f"ERROR: missing required environment variables: {', '.join(missing)}")


def run_dbt(args):
    """Run a dbt command against the project/profiles in include/dbt."""
    cmd = [
        "dbt", *args,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ]
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def run_general():
    print("\n========== GENERAL ERA5 ==========")
    regions.load_regions()
    general_pipeline.ingest()
    run_dbt(["run", "--select", *DBT_SELECTORS["general"]])
    general_pipeline.export()
    # Refresh the portal datasets (parquet -> XLSX in R2 + Supabase upsert),
    # preserving any admin edits. No-op if Supabase env vars are absent.
    portal_sync.register()


def run_agriculture():
    print("\n========== AGRICULTURE ERA5 ==========")
    regions.load_regions()
    agriculture_pipeline.ingest(raise_on_failure=True)
    run_dbt(["run", "--select", *DBT_SELECTORS["agriculture"]])
    agriculture_pipeline.export()


def main():
    parser = argparse.ArgumentParser(description="Run the ERA5 pipeline(s) without Airflow.")
    parser.add_argument(
        "--target",
        choices=["general", "agriculture", "all"],
        default="all",
        help="Which pipeline(s) to run (default: all)",
    )
    parser.add_argument(
        "--skip-dbt-deps",
        action="store_true",
        help="Skip 'dbt deps' (use when dbt packages are already installed)",
    )
    args = parser.parse_args()

    check_env()

    if not args.skip_dbt_deps:
        run_dbt(["deps"])

    if args.target in ("general", "all"):
        run_general()
    if args.target in ("agriculture", "all"):
        run_agriculture()

    print("\nPipeline run complete.")


if __name__ == "__main__":
    main()
