FROM astrocrpublic.azurecr.io/runtime:3.1-13

# Créer et installer dbt dans le venv
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --upgrade pip && \
    dbt_venv/bin/pip install --no-cache-dir dbt-duckdb==1.9.0