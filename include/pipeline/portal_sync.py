"""Refresh the institutional ERA5 datasets on the MEDD portal after an export.

Runs at the end of each pipeline: converts the final gold parquet in R2 to XLSX
(also in R2) and upserts the dataset rows in the portal's Supabase.

IMPORTANT — the admin's platform-side edits are preserved: dataset name,
description and per-column metadata are written ONLY on first insert. On every
subsequent run the row already exists, so we refresh just the file size (the
XLSX itself is re-generated from the latest parquet); the editable fields are
never touched. This is the portal-side equivalent of the previous
`/api/admin/sync` endpoint, moved into the pipeline so it no longer depends on
the (fragile) Vercel serverless runtime.

Each entry carries a `group` matching a `run_pipeline.py --target` value, so
`register("general")` only touches the General datasets and
`register("agriculture")` only the Agriculture one.

Requires env vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
R2_BUCKET_NAME, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY. If the Supabase vars
are absent the refresh is skipped (a no-op), so the pipeline stays green when
run outside CI.
"""
import os
import duckdb
import requests

# Well-known ERA5 column metadata (French) — mirrors the portal's ERA5_COLUMN_INFO.
# Keys are compared lowercased.
COLINFO = {
    "time": ("Date de la mesure", ""), "date": ("Date de la mesure", ""),
    "valid_time": ("Date et heure de validité", ""), "number": ("Numéro d'ensemble", ""),
    "expver": ("Version de l'expérience", ""), "latitude": ("Latitude", "°N"),
    "longitude": ("Longitude", "°E"), "t2m": ("Température à 2 mètres", "K"),
    "d2m": ("Température du point de rosée à 2m", "K"), "tp": ("Précipitations totales", "m"),
    "u10": ("Composante U du vent à 10m", "m/s"), "v10": ("Composante V du vent à 10m", "m/s"),
    "si10": ("Vitesse du vent à 10m", "m/s"),
    "sp": ("Pression de surface", "Pa"), "msl": ("Pression au niveau de la mer", "Pa"),
    "tcc": ("Couverture nuageuse totale", "0-1"), "ssr": ("Rayonnement solaire net en surface", "J/m²"),
    "ssrd": ("Rayonnement solaire descendant en surface", "J/m²"),
    "strd": ("Rayonnement thermique descendant en surface", "J/m²"),
    "str": ("Rayonnement thermique net en surface", "J/m²"), "e": ("Évaporation", "m"),
    "pev": ("Évaporation potentielle", "m"), "ro": ("Ruissellement", "m"),
    "skt": ("Température de surface du sol", "K"), "sst": ("Température de surface de la mer", "K"),
    "stl1": ("Température du sol niveau 1", "K"), "swvl1": ("Teneur en eau du sol niveau 1", "m³/m³"),
    "datetime_id": ("Identifiant date-heure", ""), "versionera5": ("Version ERA5 de l'extraction", ""),
    "mwd": ("Direction moyenne des vagues", "°"), "mwp": ("Période moyenne des vagues", "s"),
    "direction_moyenne_vagues": ("Direction moyenne des vagues", "°"),
    "periode_moyenne_vagues": ("Période moyenne des vagues", "s"),
    # ── Agriculture gold table (fact_AgricultureEra5) ──
    "années": ("Année de la mesure", ""),
    "mois_années": ("Mois et année de la mesure", ""),
    "region": ("Région administrative (GADM niveau 1)", ""),
    "temp_max_24h": ("Température maximale de l'air à 2 m sur 24 h", "°C"),
    "temp_moyenne_24h": ("Température moyenne de l'air à 2 m sur 24 h", "°C"),
    "temp_min_24h": ("Température minimale de l'air à 2 m sur 24 h", "°C"),
    "vitesse_vent_10m_moyenne": ("Vitesse moyenne du vent à 10 m sur 24 h", "m/s"),
    "reference_evapotranspiration": ("Évapotranspiration de référence (Penman-Monteith FAO-56)", "mm/jour"),
    "precipitation_flux": ("Flux de précipitations", "mm/jour"),
    "humidité_relative": ("Humidité relative à 2 m (12h00 UTC)", "fraction 0-1"),
    "coord_id": ("Identifiant de coordonnée", ""),
}
STRIP_COLUMNS = {"key_id"}

# Hard limit of the XLSX format (1 048 576 rows including the header row).
XLSX_MAX_ROWS = 1_048_576

DATASETS = [
    {
        "group": "general",
        "parquet": "Copernicus/General/GeneralEra5.parquet",
        "xlsx": "Copernicus/General/GeneralEra5.xlsx",
        "name": "ERA5 - Données climatiques mensuelles - Djibouti",
        "description": (
            "Données de réanalyse climatique ERA5 (ECMWF) — extraction mensuelle pour Djibouti. "
            "Inclut les principales variables atmosphériques et de surface : température, "
            "précipitations, vent, pression, rayonnement solaire, humidité."
        ),
        "source": "Copernicus Climate Change Service (ECMWF) — ERA5",
        "theme_slug": "meteorologie_climat",
        "tags": ["ERA5", "climat", "mensuel", "Djibouti", "réanalyse", "ECMWF"],
    },
    {
        "group": "general",
        "parquet": "Copernicus/General/vagues.parquet",
        "xlsx": "Copernicus/General/vagues.xlsx",
        "name": "ERA5 - Données de vagues océaniques - Djibouti",
        "description": (
            "Données de réanalyse ERA5 (ECMWF) — vagues océaniques pour Djibouti. "
            "Inclut la direction moyenne et la période moyenne des vagues."
        ),
        "source": "Copernicus Climate Change Service (ECMWF) — ERA5",
        "theme_slug": "meteorologie_climat",
        "tags": ["ERA5", "vagues", "océanographie", "houle", "Djibouti", "réanalyse", "ECMWF"],
    },
    {
        "group": "agriculture",
        "parquet": "Copernicus/Agriculture/AgricultureEra5.parquet",
        "xlsx": "Copernicus/Agriculture/AgricultureEra5.xlsx",
        "name": "ERA5 - Indicateurs agrométéorologiques journaliers - Djibouti",
        "description": (
            "Indicateurs agrométéorologiques journaliers dérivés de la réanalyse ERA5 "
            "(Copernicus C3S / AgERA5), depuis 2021 pour Djibouti, sur une grille de 0,1° "
            "(324 points) rattachée aux 6 régions administratives. Variables : températures "
            "de l'air à 2 m (maximale, moyenne, minimale sur 24 h), vitesse moyenne du vent "
            "à 10 m, humidité relative à 12h00, flux de précipitations et évapotranspiration "
            "de référence (Penman-Monteith FAO-56)."
        ),
        "source": "Copernicus Climate Change Service (ECMWF) — AgERA5 / ERA5",
        # Same theme as the two General datasets: this is climate/weather
        # reanalysis, the "agro" qualifier describes the indicators, not the theme.
        "theme_slug": "meteorologie_climat",
        "tags": [
            "ERA5", "AgERA5", "agrométéorologie", "journalier", "Djibouti",
            "agriculture", "évapotranspiration", "réanalyse", "ECMWF",
        ],
    },
]

GROUPS = sorted({d["group"] for d in DATASETS})


def _map_type(duck_type):
    t = duck_type.upper()
    if any(x in t for x in ("TIMESTAMP", "DATE", "TIME")):
        return "date"
    if "BOOL" in t:
        return "boolean"
    if any(x in t for x in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGEINT")):
        return "number"
    return "string"


def _connect_r2():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL excel; LOAD excel;")
    con.execute(f"SET s3_endpoint='{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com';")
    con.execute(f"SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}';")
    con.execute(f"SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}';")
    con.execute("SET s3_url_style='path'; SET s3_region='auto';")
    return con


def _fetch_theme_ids(base_url, headers, slugs):
    """Resolve the portal theme slugs used by the selected datasets to their ids."""
    resp = requests.get(
        f"{base_url}/rest/v1/themes",
        headers=headers,
        params={"slug": f"in.({','.join(sorted(slugs))})", "select": "id,slug"},
        timeout=30,
    )
    resp.raise_for_status()
    return {row["slug"]: row["id"] for row in resp.json()}


def _select_datasets(group):
    if group is None:
        return list(DATASETS)
    if group not in GROUPS:
        raise ValueError(f"portal_sync: unknown group '{group}' (known: {', '.join(GROUPS)})")
    return [d for d in DATASETS if d["group"] == group]


def register(group=None):
    """Convert the gold parquet(s) to XLSX in R2 and upsert the portal datasets.

    group: "general", "agriculture", or None for every configured dataset.
    """
    datasets = _select_datasets(group)

    supabase_url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_key:
        print("portal_sync: SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set — skipping portal refresh")
        return

    supabase_url = supabase_url.rstrip("/")
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    }
    bucket = os.environ["R2_BUCKET_NAME"]
    con = _connect_r2()

    theme_ids = _fetch_theme_ids(supabase_url, headers, {d["theme_slug"] for d in datasets})

    for d in datasets:
        theme_id = theme_ids.get(d["theme_slug"])
        if theme_id is None:
            print(f"portal_sync: theme '{d['theme_slug']}' not found in Supabase — skipping {d['name']}")
            continue

        print(f"portal_sync: {d['name']}")
        src = f"s3://{bucket}/{d['parquet']}"
        dst = f"s3://{bucket}/{d['xlsx']}"

        describe = con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()
        cols = [(r[0], r[1]) for r in describe if r[0].lower() not in STRIP_COLUMNS]
        selection = ", ".join(f'"{name}"' for name, _ in cols)

        # Fail loudly rather than publish a truncated workbook. The daily Agriculture
        # table grows ~118k rows/year and will eventually reach this ceiling.
        row_count = con.execute(f"SELECT count(*) FROM '{src}'").fetchone()[0]
        if row_count + 1 > XLSX_MAX_ROWS:
            raise RuntimeError(
                f"portal_sync: {d['parquet']} has {row_count:,} rows — exceeds the XLSX limit of "
                f"{XLSX_MAX_ROWS:,} (header included). Publish it as CSV/Parquet instead, or split "
                f"it, before this dataset can be refreshed again."
            )

        # Always regenerate the XLSX from the freshest parquet (final gold table).
        # HEADER true is required: DuckDB's xlsx writer omits column names by default.
        con.execute(f"COPY (SELECT {selection} FROM '{src}') TO '{dst}' WITH (FORMAT xlsx, HEADER true)")
        size = con.execute(f"SELECT octet_length(content) FROM read_blob('{dst}')").fetchone()[0]
        print(f"  parquet -> xlsx in R2 ({row_count:,} rows, {size / 1024 / 1024:.1f} MB)")

        file_path = f"r2:{d['xlsx']}"
        existing = requests.get(
            f"{supabase_url}/rest/v1/datasets",
            headers=headers,
            params={"file_path": f"eq.{file_path}", "select": "id"},
            timeout=30,
        )
        existing.raise_for_status()
        found = existing.json()

        if found:
            # Preserve every platform-side edit — only the file size can change.
            requests.patch(
                f"{supabase_url}/rest/v1/datasets",
                headers=headers,
                params={"file_path": f"eq.{file_path}"},
                json={"file_size_bytes": size},
                timeout=30,
            ).raise_for_status()
            print(f"  refreshed file (id={found[0]['id']}); name/description/columns left untouched")
            continue

        col_meta = []
        for name, duck_type in cols:
            info = COLINFO.get(name.lower())
            col_meta.append({
                "name": name,
                "description": info[0] if info else name,
                "unit": info[1] if info else "",
                "type": _map_type(duck_type),
            })
        body = {
            "name": d["name"], "description": d["description"], "source": d["source"],
            "theme_id": theme_id, "file_path": file_path, "file_type": "xlsx",
            "file_size_bytes": size, "columns": col_meta, "tags": d["tags"],
            "status": "validated", "created_by": None,
        }
        created = requests.post(
            f"{supabase_url}/rest/v1/datasets",
            headers={**headers, "Prefer": "return=representation"},
            json=body,
            timeout=30,
        )
        if created.status_code >= 300:
            raise RuntimeError(f"portal_sync insert failed ({created.status_code}): {created.text[:300]}")
        print(f"  created dataset id={created.json()[0]['id']} (status=validated)")

    print("portal_sync: done")
