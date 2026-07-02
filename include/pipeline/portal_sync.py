"""Refresh the institutional ERA5 datasets on the MEDD portal after an export.

Runs at the end of the General pipeline: converts the final gold parquet in R2
to XLSX (also in R2) and upserts the dataset rows in the portal's Supabase.

IMPORTANT — the admin's platform-side edits are preserved: dataset name,
description and per-column metadata are written ONLY on first insert. On every
subsequent run the row already exists, so we refresh just the file size (the
XLSX itself is re-generated from the latest parquet); the editable fields are
never touched. This is the portal-side equivalent of the previous
`/api/admin/sync` endpoint, moved into the pipeline so it no longer depends on
the (fragile) Vercel serverless runtime.

Requires env vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
R2_BUCKET_NAME, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY. If the Supabase vars
are absent the refresh is skipped (a no-op), so the pipeline stays green when
run outside CI.
"""
import os
import duckdb
import requests

# Well-known ERA5 column metadata (French) — mirrors the portal's ERA5_COLUMN_INFO.
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
}
STRIP_COLUMNS = {"key_id"}
THEME_SLUG = "meteorologie_climat"

DATASETS = [
    {
        "parquet": "Copernicus/General/GeneralEra5.parquet",
        "xlsx": "Copernicus/General/GeneralEra5.xlsx",
        # Excluded from the XLSX only — the gold parquet keeps these columns.
        "strip": {"date", "année"},
        "name": "ERA5 - Données climatiques mensuelles - Djibouti",
        "description": (
            "Données de réanalyse climatique ERA5 (ECMWF) — extraction mensuelle pour Djibouti. "
            "Inclut les principales variables atmosphériques et de surface : température, "
            "précipitations, vent, pression, rayonnement solaire, humidité."
        ),
        "source": "Copernicus Climate Change Service (ECMWF) — ERA5",
        "tags": ["ERA5", "climat", "mensuel", "Djibouti", "réanalyse", "ECMWF"],
    },
    {
        "parquet": "Copernicus/General/vagues.parquet",
        "xlsx": "Copernicus/General/vagues.xlsx",
        "name": "ERA5 - Données de vagues océaniques - Djibouti",
        "description": (
            "Données de réanalyse ERA5 (ECMWF) — vagues océaniques pour Djibouti. "
            "Inclut la direction moyenne et la période moyenne des vagues."
        ),
        "source": "Copernicus Climate Change Service (ECMWF) — ERA5",
        "tags": ["ERA5", "vagues", "océanographie", "houle", "Djibouti", "réanalyse", "ECMWF"],
    },
]


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


def register():
    """Convert the gold parquet(s) to XLSX in R2 and upsert the portal datasets."""
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

    resp = requests.get(
        f"{supabase_url}/rest/v1/themes",
        headers=headers,
        params={"slug": f"eq.{THEME_SLUG}", "select": "id"},
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        print(f"portal_sync: theme '{THEME_SLUG}' not found in Supabase — skipping")
        return
    theme_id = rows[0]["id"]

    for d in DATASETS:
        print(f"portal_sync: {d['name']}")
        src = f"s3://{bucket}/{d['parquet']}"
        dst = f"s3://{bucket}/{d['xlsx']}"

        strip = STRIP_COLUMNS | d.get("strip", set())
        describe = con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()
        cols = [(r[0], r[1]) for r in describe if r[0].lower() not in strip]
        selection = ", ".join(f'"{name}"' for name, _ in cols)

        # Always regenerate the XLSX from the freshest parquet (final gold table).
        con.execute(f"COPY (SELECT {selection} FROM '{src}') TO '{dst}' WITH (FORMAT xlsx)")
        size = con.execute(f"SELECT octet_length(content) FROM read_blob('{dst}')").fetchone()[0]
        print(f"  parquet -> xlsx in R2 ({size / 1024 / 1024:.1f} MB)")

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
