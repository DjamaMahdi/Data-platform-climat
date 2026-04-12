# Agriculture DAG — Full Code Explanation

> **File:** `dags/Agriculture_Era5.py`
> **DAG ID:** `agri_era5_in_taskflow`
> **Last updated:** 2026-03-23

This document explains every block of the Agriculture ERA5 DAG for someone learning Python and data engineering.

---

## BLOCK 1 — The description at the top

```python
"""
### Agrometeorological Indicators — CDS to MotherDuck Bronze

Fetches daily agrometeorological indicators from the Copernicus CDS API
(`sis-agrometeorological-indicators`) and ingests each variable into its
own Bronze table (Ag_xxx) in MotherDuck.
"""
```

This is just a comment (text between `"""`). It does nothing — it's a note for humans reading the code. It says: "this file downloads agriculture weather data and puts it in our database."

---

## BLOCK 2 — The imports (the toolbox)

```python
import cdsapi                    # The "phone" to call Copernicus (the climate data provider)
import zipfile                   # Opens .zip files (some downloads come zipped)
import os                        # Talks to the computer's file system (create folders, delete files)
import time                      # Lets us pause the code (time.sleep)
import random                    # Generates random numbers (for retry timing)
import duckdb                    # The "phone" to call MotherDuck (our database)
import pandas as pd              # A table/spreadsheet library (like Excel in Python)
import xarray as xr              # Reads .nc files (NetCDF = the format climate data comes in)
from airflow.decorators import dag, task   # Airflow decorators (explained below)
from datetime import datetime, timedelta   # Work with dates and time differences
from dateutil.relativedelta import relativedelta  # "Add 1 month" to a date
```

Think of `import` like bringing tools to a workbench. `import duckdb` means "I'm going to need the DuckDB tool later." If you don't import it, you can't use it.

`from X import Y` means "from the toolbox X, I only need the specific tool Y."

---

## BLOCK 3 — Secrets and folders

```python
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")   # Read the database password from environment
CDSAPI_KEY = os.getenv("CDSAPI_KEY")               # Read the API key from environment
EXTRACT_DIR = 'include/dataset'                     # Folder where downloads go temporarily
os.makedirs(EXTRACT_DIR, exist_ok=True)             # Create that folder if it doesn't exist
```

`os.getenv("MOTHERDUCK_TOKEN")` means: "go check the computer's environment variables and find the one called MOTHERDUCK_TOKEN." This is how we keep passwords OUT of the code. The passwords live in a `.env` file that Docker loads.

`os.makedirs(EXTRACT_DIR, exist_ok=True)` means: "create the folder `include/dataset/`. If it already exists, don't crash (`exist_ok=True`)."

---

## BLOCK 4 — Constants (settings)

```python
AGRO_DATASET = "sis-agrometeorological-indicators"  # The dataset name on Copernicus
START_YEAR = 2021                                    # How far back we want data
AGRO_LAG_DAYS = 10          # Climate data takes ~10 days to be published
MONTHS_PER_BATCH = 1        # Ask the API for 1 month at a time
DJIBOUTI_AREA = [12.7, 41.7, 10.9, 43.5]   # Geographic box [north, west, south, east]
CDS_MAX_RETRIES = 3         # If API fails, retry up to 3 times
CDS_BASE_DELAY = 60         # Wait 60 seconds before first retry
INTER_VARIABLE_DELAY = 15   # Wait 15 seconds between variables (for API, costs 0 CU on MotherDuck)
```

Constants are written in `ALL_CAPS` by convention. They're values that never change during the code's execution. Think of them as the "settings panel" — if you want to change behavior, you change these values here, not buried in the code below.

---

## BLOCK 5 — Variable registry (the menu of what to download)

```python
AGRO_VARIABLE_SPECS = [
    {
        "variable": "2m_temperature",          # What to ask the API for
        "statistics": [                        # What summaries we want
            "24_hour_maximum", "24_hour_mean", "24_hour_minimum",
            "day_time_maximum", "day_time_mean",
            "night_time_mean", "night_time_minimum",
        ],
        "table": "Ag_temperature_2m",          # Where to store it in our database
    },
    {
        "variable": "10m_wind_speed",
        "statistics": ["24_hour_mean"],        # Only 1 statistic = cheap API call
        "table": "Ag_wind_10m",
    },
    # ... same pattern for all other variables ...
    {
        "variable": "precipitation_flux",
        "statistics": None,                    # None = no statistic needed
        "table": "Ag_precip_flux",
    },
    # ... etc for all 11 variables
]
```

This is a **list** (`[...]`) of **dictionaries** (`{...}`). A dictionary is like a labeled box:
- `"variable"` -> the label on Copernicus to find this data
- `"statistics"` -> what kind of daily summary (some need it, some don't)
- `"table"` -> the table name in our MotherDuck database

Each variable gets its own table because they come from different API calls and can have different columns.

---

## BLOCK 6 — `variable_cost()` helper

```python
def variable_cost(spec):
    """Heuristic CDS cost: more statistics = higher cost = more likely to 403."""
    stats = spec.get("statistics")       # Get the statistics list from the dictionary
    return len(stats) if stats else 1    # Count them. If None, cost = 1
```

`def` means "define a function" — a reusable piece of code with a name.

This function takes a variable spec (like the temperature one above) and returns a number representing how expensive it is to download. Temperature has 7 statistics -> cost = 7. Wind has 1 -> cost = 1. Precipitation has `None` -> cost = 1.

We use this later to sort: cheap variables first, expensive last.

`len(stats)` = "length of the list" = how many items are in it.

`if stats else 1` = "if stats is not None, use len(stats), otherwise use 1." This is a one-line if/else in Python.

---

## BLOCK 7 — `get_agro_cutoff()` helper

```python
def get_agro_cutoff():
    """Returns the last safely available agro month as (year, month)."""
    cutoff = datetime.today() - timedelta(days=AGRO_LAG_DAYS)
    return cutoff.year, cutoff.month
```

`datetime.today()` = today's date (2026-03-23).

`timedelta(days=10)` = "10 days."

So `cutoff = March 23 - 10 days = March 13`. Then we return `(2026, 3)`.

This tells us: "don't try to download anything past March 2026, because the data isn't published yet."

---

## BLOCK 8 — `get_loaded_info()` — THE CU-OPTIMIZED FUNCTION

```python
def get_loaded_info(conn, table_name):
    """Returns (loaded_months set, max_date) in a single query."""
    try:
        rows = conn.sql(f"""
            SELECT EXTRACT(YEAR FROM time)::INT AS y,
                   EXTRACT(MONTH FROM time)::INT AS m,
                   MAX(time) AS max_date
            FROM Climate_Era5.Bronze."{table_name}"
            GROUP BY 1, 2
        """).fetchall()
```

Breaking this down:

- `conn` = our database connection (passed in from outside)
- `table_name` = e.g., `"Ag_wind_10m"`
- `conn.sql(...)` = "run this SQL query on the database"
- `f"""..."""` = a "formatted string" — the `{table_name}` gets replaced with the actual table name
- The SQL query says: "For every distinct (year, month) combination in the `time` column, give me the year, the month, and the latest date in that month"
- `GROUP BY 1, 2` = group by the first two columns (year, month)
- `.fetchall()` = "give me all the results as a list of rows"

Example result for `Ag_wind_10m`:
```
[(2021, 1, 2021-01-31), (2021, 2, 2021-02-28), ..., (2026, 3, 2026-03-13)]
```

Then:

```python
        if not rows:                              # If the query returned nothing (empty table)
            return set(), None                    # Return empty set and no date

        loaded = {(r[0], r[1]) for r in rows}    # Build a SET of (year, month) tuples
        max_date = max(r[2] for r in rows)        # Find the overall latest date
        return loaded, max_date
    except Exception:          # If the table doesn't exist, the query crashes
        return set(), None     # That's fine — return empty, meaning "no data yet"
```

A **set** is like a list but with no duplicates and super-fast lookups. `{(2021,1), (2021,2), ...}`. Checking "is (2021, 5) in this set?" is instant.

`max(r[2] for r in rows)` = "from all rows, take the 3rd value (index 2 = the date), and find the biggest one." That's our latest date across all months.

**Why this matters for billing:** This is ONE query to MotherDuck. The old code ran 3 queries per variable + 1 query per month per variable. For 11 variables with 60 months each = 671 queries. Now it's just 11 queries. Each query costs minimum 1 CU second, so we save ~660 CU seconds.

---

## BLOCK 9 — `get_missing_months()` — What months do we still need?

```python
def get_missing_months(max_date, cutoff_year, cutoff_month):
    """Returns (year, month) tuples that need downloading."""
    cutoff_date = datetime(cutoff_year, cutoff_month, 1)     # e.g., 2026-03-01
    if max_date is None:                                      # No data at all?
        start_date = datetime(START_YEAR, 1, 1)               # Start from 2021-01-01
    else:
        start_date = datetime(max_date.year, max_date.month, 1)  # Start from max_date's month
    months = []
    current = start_date
    while current <= cutoff_date:             # Loop from start to cutoff
        months.append((current.year, current.month))   # Add each (year, month) to the list
        current += relativedelta(months=1)    # Move forward 1 month
    return months
```

Example: `max_date = 2026-03-13`, cutoff = `(2026, 3)`:
- `start_date = 2026-03-01` (max_date's own month — because it might be partial!)
- Loop: `(2026, 3)` -> done
- Returns: `[(2026, 3)]`

Example: `max_date = None` (empty table), cutoff = `(2026, 3)`:
- `start_date = 2021-01-01`
- Loop: `(2021, 1), (2021, 2), ..., (2026, 3)`
- Returns 63 months

**The bug fix:** Old code did `start_date = max_date's month + 1 month`. So if March was partially loaded (days 1-13), it would skip to April and days 14-31 would be lost forever. Now we start from max_date's own month.

---

## BLOCK 10 — `build_agro_cds_requests()` — Build API request list

```python
def build_agro_cds_requests(missing_months, spec):
    """Build one CDS request per month for a single variable."""
    months_by_year = {}
    for y, m in missing_months:
        months_by_year.setdefault(y, []).append(m)
    #   ^ Groups months by year: {2021: [1,2,3,...], 2022: [1,2,...]}
    #   setdefault = "if key doesn't exist, create it with empty list, then append"

    base = {
        "variable": spec["variable"],       # e.g., "10m_wind_speed"
        "version": "2_0",                   # API version
        "area": DJIBOUTI_AREA,              # Geographic bounding box
        "day": [str(d).zfill(2) for d in range(1, 32)],   # ["01", "02", ..., "31"]
    }
    #   ^ zfill(2) = pad with zeros: 1 -> "01", 12 -> "12"
    #   range(1, 32) = numbers 1 through 31

    if spec["statistics"] is not None:
        base["statistic"] = spec["statistics"]   # Add statistics if this variable needs them

    requests = []
    for year, months in sorted(months_by_year.items()):   # For each year, in order
        for m in sorted(months):                           # For each month, in order
            req = {
                **base,                           # Copy all the base settings
                "year": [str(year)],              # Add this specific year
                "month": [str(m).zfill(2)],       # Add this specific month
            }
            requests.append(req)      # Add this request to the list
    return requests
```

`**base` is Python's "spread" operator — it copies all key-value pairs from `base` into the new dictionary. So each request gets the variable, area, days, AND a specific year/month.

This builds a list like:
```python
[
    {"variable": "10m_wind_speed", "area": [...], "day": ["01"..."31"], "year": ["2026"], "month": ["03"]},
    {"variable": "10m_wind_speed", "area": [...], "day": ["01"..."31"], "year": ["2026"], "month": ["04"]},
]
```

One request per month. The API will ignore impossible days (like Feb 30 or future days).

---

## BLOCK 11 — `retrieve_with_retry()` — Call the API with retries

```python
def retrieve_with_retry(client, dataset, request, target):
    """CDS API retrieve with exponential backoff."""
    for attempt in range(CDS_MAX_RETRIES + 1):     # Try up to 4 times (0, 1, 2, 3)
        try:
            client.retrieve(dataset, request, target)   # THE ACTUAL API CALL
            return                                       # Success! Exit the function

        except Exception as e:                # Something went wrong
            error_str = str(e).lower()        # Convert error message to lowercase text

            if "400" in error_str or "bad request" in error_str:
                raise   # Bad request = our fault (wrong parameters). Don't retry.
            if "401" in error_str or "unauthorized" in error_str:
                raise   # Wrong password. Don't retry.
            if attempt == CDS_MAX_RETRIES:
                raise   # We've used all retries. Give up.

            # Calculate wait time: 60s -> 120s -> 240s (doubles each time)
            delay = CDS_BASE_DELAY * (2 ** attempt)
            #   60 * 2^0 = 60s, 60 * 2^1 = 120s, 60 * 2^2 = 240s
            jitter = delay * 0.25 * (random.random() * 2 - 1)  # Random +/-25%
            wait = delay + jitter
            print(f"      Retry {attempt + 1}/{CDS_MAX_RETRIES}: {e} -- waiting {wait:.0f}s")
            time.sleep(wait)     # Pause, then the loop tries again
```

**Exponential backoff** = wait longer each time. Why? If the server is overloaded, hammering it with quick retries makes things worse. Waiting longer gives it time to recover.

**Jitter** = randomness. If 100 users all retry at exactly 60 seconds, the server gets slammed again. Random +/-25% spreads the retries out.

`try/except` is Python's error handling: "TRY this code. If it crashes, EXCEPT (catch) the error and do something else."

`raise` = "re-throw this error upward" — the function gives up and lets the caller deal with it.

---

## BLOCK 12 — File helpers

```python
def extract_nc_files(download_path):
    """Handle both zip and bare .nc downloads. Returns list of .nc paths."""
    if zipfile.is_zipfile(download_path):          # Is it a zip?
        with zipfile.ZipFile(download_path) as z:  # Open the zip
            z.extractall(EXTRACT_DIR)              # Unzip everything into our folder
        os.remove(download_path)                   # Delete the zip (we have the contents now)
    else:
        nc_dest = download_path + '.nc'            # It's a bare file — just rename it
        os.rename(download_path, nc_dest)

    return sorted(                                 # Return all .nc files in the folder, sorted
        os.path.join(EXTRACT_DIR, f)               # Full path: "include/dataset/file.nc"
        for f in os.listdir(EXTRACT_DIR)           # List all files in the folder
        if f.endswith('.nc')                        # Keep only .nc files
    )


def cleanup_nc_files():
    """Remove leftover .nc/.zip files from extract directory."""
    for f in os.listdir(EXTRACT_DIR):              # For each file in the folder
        if f.endswith('.nc') or f.endswith('.zip'): # If it's a climate file
            try:
                os.remove(os.path.join(EXTRACT_DIR, f))   # Delete it
            except OSError:
                pass   # If deletion fails (file in use?), ignore and move on
```

`os.path.join(EXTRACT_DIR, f)` safely combines folder + filename: `"include/dataset" + "data.nc"` -> `"include/dataset/data.nc"`.

`with zipfile.ZipFile(...) as z:` — the `with` keyword means "open this, do stuff, then automatically close it when done." Prevents files from staying open accidentally.

---

## BLOCK 13 — The DAG definition

```python
@dag(
    start_date=datetime(2026, 1, 1),   # When this DAG "starts" (Airflow metadata)
    schedule=None,                      # No automatic schedule — manual trigger only
    catchup=False,                      # Don't run for past dates we missed
    tags=['duckdb', 'agriculture', 'era5'],   # Labels visible in Airflow UI
    max_active_tasks=1,                 # Only 1 task at a time (no parallel tasks)
)
def agri_era5_in_taskflow():
```

`@dag(...)` is a **decorator** — it's like wrapping a gift. It takes the plain function `agri_era5_in_taskflow()` and turns it into an Airflow DAG (a pipeline that Airflow knows how to schedule and monitor).

Think of it as telling Airflow: "Hey, this Python function is a pipeline. Here are its settings."

---

## BLOCK 14 — The main task (where ALL the work happens)

```python
    @task(execution_timeout=timedelta(hours=30))
    def retrieve_and_ingest_AgriEra5():
```

`@task` = another decorator. Tells Airflow: "this function is a task inside the DAG."

`execution_timeout=timedelta(hours=30)` = "if this task runs for more than 30 hours, kill it." Safety net for full historical loads.

```python
        cutoff_year, cutoff_month = get_agro_cutoff()
        #   ^ e.g., (2026, 3) — the latest month with available data

        download_target = os.path.join(EXTRACT_DIR, 'agri_era5_download')
        #   ^ The file path where CDS will save the downloaded file

        client = cdsapi.Client("https://cds.climate.copernicus.eu/api", CDSAPI_KEY)
        #   ^ Create a connection to the Copernicus API (like logging in)

        conn = duckdb.connect("md:Climate_Era5", config={"motherduck_token": MOTHERDUCK_TOKEN})
        #   ^ Connect to MotherDuck database. "md:" prefix = MotherDuck cloud
        #     This connection stays open for the entire run (idle time = 0 CU cost)
```

```python
        failed = []       # List of variables that crashed
        succeeded = []    # List of variables that finished OK
        skipped = []      # List of variables already up to date
```

These are empty lists. As we process each variable, we add its name to one of these lists. At the end, we print a summary.

```python
        # Process cheapest variables first — maximize success before heavy ones
        sorted_specs = sorted(AGRO_VARIABLE_SPECS, key=variable_cost)
```

`sorted(list, key=function)` = "sort this list, using the function to decide the order." So variables with cost 1 (precipitation, wind) come first, temperature with cost 7 comes last. If the API runs out of quota halfway through, at least the cheap ones are already saved.

---

## BLOCK 15 — The variable loop

```python
        try:
            for spec_idx, spec in enumerate(sorted_specs):
                var_name = spec["variable"]       # e.g., "10m_wind_speed"
                table_name = spec["table"]        # e.g., "Ag_wind_10m"
```

`enumerate()` gives us both the index number AND the item. `spec_idx` = 0, 1, 2... and `spec` = the dictionary for that variable.

The outer `try` ensures that even if everything crashes, we still clean up files and close the database (see the `finally` block later).

```python
                try:
                    loaded_months, max_date = get_loaded_info(conn, table_name)
                    #   ^ ONE query to MotherDuck. Returns:
                    #     loaded_months = {(2021,1), (2021,2), ...} — all months in DB
                    #     max_date = 2026-03-13 — the exact last day in the DB

                    already_exists = len(loaded_months) > 0
                    #   ^ If the set has items -> table exists. If empty -> new table.
```

```python
                    # The latest month may be partial — remove it from skip set
                    # so it gets re-downloaded and we only INSERT the new days
                    if max_date:
                        loaded_months.discard((max_date.year, max_date.month))
                    #   ^ THIS IS THE KEY FIX!
                    #   Example: max_date = 2026-03-13
                    #   loaded_months had (2026, 3) in it -> REMOVE it
                    #   Now when we check "is March loaded?" -> NO -> so we re-download it
                    #   discard() = "remove if present, do nothing if not" (safer than remove())
```

```python
                    missing = get_missing_months(max_date, cutoff_year, cutoff_month)
                    #   ^ Returns [(2026, 3)] — March is "missing" because we removed it
                    #     from loaded_months, and get_missing_months starts from
                    #     max_date's own month

                    if not missing:
                        print(f"  {var_name}: up to date")
                        skipped.append(var_name)
                        continue      # Skip to next variable
                    #   ^ "continue" = jump back to the "for" loop and go to the next item
```

```python
                    requests = build_agro_cds_requests(missing, spec)
                    #   ^ Build the list of API calls: one per missing month

                    need_create = not already_exists
                    #   ^ True if table doesn't exist yet (first ever load)
                    #     False if table exists (we'll INSERT INTO it, not CREATE it)

                    mode = "INCREMENTAL" if already_exists else "FULL LOAD"
                    print(f"\n{'='*60}")
                    print(f"  {var_name} [{mode}] -> {table_name}")
                    if max_date:
                        print(f"  Latest in DB: {max_date}")
                    #   ^ Now shows the EXACT date (e.g., "2026-03-13")
                    #     instead of just "2026-03" — so you can see the partial month
                    print(f"  {len(missing)} month(s) to download, {len(requests)} batch(es)")
```

---

## BLOCK 16 — The batch loop (downloading month by month)

```python
                    for batch_idx, request in enumerate(requests, start=1):
                        batch_year = request["year"][0]      # e.g., "2026"
                        batch_month = request["month"][0]    # e.g., "03"
                        print(f"    Batch {batch_idx}/{len(requests)}: {batch_year}-{batch_month}")
```

`enumerate(requests, start=1)` = same as before but counting starts at 1 instead of 0 (for prettier logging: "Batch 1/5" not "Batch 0/5").

```python
                        # -- Skip if month already loaded (Python set lookup — no query)
                        if (int(batch_year), int(batch_month)) in loaded_months:
                            print(f"      already loaded -- skipping")
                            continue
                        #   ^ "Is (2021, 5) in our set of loaded months?"
                        #     This check happens IN PYTHON, not in MotherDuck.
                        #     A set lookup is instant (O(1)). Zero CU cost.
                        #     The old code ran a SQL query here — 1 CU per check!
```

```python
                        # -- Download with retry
                        retrieve_with_retry(client, AGRO_DATASET, request, download_target)
                        #   ^ Calls the CDS API. Downloads the data to disk.
                        #     If it fails, retries up to 3 times with increasing wait.

                        # -- Extract
                        nc_paths = extract_nc_files(download_target)
                        #   ^ Unzips if needed. Returns list of .nc file paths.
```

---

## BLOCK 17 — Ingesting data (NetCDF -> Database)

```python
                        # -- Ingest all .nc files
                        for nc_path in nc_paths:
                            ds = xr.open_dataset(nc_path)
                            #   ^ Open the NetCDF file with xarray
                            #     ds = "dataset" — a multi-dimensional array of climate data

                            df = ds.to_dataframe().reset_index()
                            #   ^ Convert to a pandas DataFrame (a flat table, like a spreadsheet)
                            #     .reset_index() flattens the multi-dimensional structure
                            #     Now df looks like:
                            #     | time       | latitude | longitude | wind_speed |
                            #     | 2026-03-01 | 11.0     | 42.0      | 5.3        |
                            #     | 2026-03-01 | 11.1     | 42.0      | 4.8        |
                            #     | ...

                            ds.close()
                            #   ^ Close the NetCDF file (free memory)
```

```python
                            # Filter out days already loaded (partial month)
                            if max_date and not need_create:
                                df = df[df['time'] > pd.Timestamp(max_date)]
                                #   ^ THIS IS THE PARTIAL MONTH FIX!
                                #
                                #   Example: max_date = 2026-03-13
                                #   df has rows for March 1, 2, 3, ..., 31
                                #   df['time'] > March 13 -> keeps only March 14, 15, ..., 31
                                #   So we only INSERT the NEW days, no duplicates!
                                #
                                #   pd.Timestamp(max_date) converts the date to pandas format
                                #   so the comparison works correctly
                                #
                                #   df[condition] = "filter: keep only rows where condition is True"
                                #   This is like SQL: SELECT * FROM df WHERE time > '2026-03-13'

                                if len(df) == 0:
                                    print(f"      no new days -- skipping")
                                    os.remove(nc_path)
                                    continue
                                #   ^ If ALL rows were filtered out (no new days), skip this file
                                #     This happens when the month is actually fully loaded

                                print(f"      partial month: {len(df)} new rows (after {max_date})")
                                #   ^ Tell us how many new rows we're inserting
                                #     e.g., "partial month: 5832 new rows (after 2026-03-13)"
```

```python
                            if need_create:
                                conn.sql(f"""
                                    CREATE TABLE
                                        Climate_Era5.Bronze."{table_name}"
                                    AS SELECT * FROM df
                                """)
                                need_create = False
                                #   ^ FIRST TIME ONLY: create the table from the DataFrame
                                #     "CREATE TABLE ... AS SELECT * FROM df" = make a new table
                                #     with the same columns as the DataFrame, filled with its data
                                #     Then set need_create = False so next batches use INSERT
                            else:
                                conn.sql(f"""
                                    INSERT INTO Climate_Era5.Bronze."{table_name}"
                                    SELECT * FROM df
                                """)
                                #   ^ ALL OTHER TIMES: append rows to the existing table
                                #     "INSERT INTO ... SELECT * FROM df" = add all rows from df

                            os.remove(nc_path)
                            #   ^ Delete the .nc file (data is now safely in MotherDuck)
```

A key thing: `SELECT * FROM df` — DuckDB can read a pandas DataFrame directly as if it were a table! No need to save to CSV, no temp files. The data goes from Python's memory straight into MotherDuck. This is very efficient.

```python
                        # Track this month so re-runs within same task skip it
                        loaded_months.add((int(batch_year), int(batch_month)))
                        #   ^ After successful ingestion, add this month to our set
                        #     So if the code loops again (shouldn't, but safety), it skips it
```

---

## BLOCK 18 — Between variables and error handling

```python
                    succeeded.append(var_name)
                    print(f"  {var_name}: done")

                    # Cooldown between variables to let CDS quota reset
                    if spec_idx < len(sorted_specs) - 1:
                        time.sleep(INTER_VARIABLE_DELAY)
                    #   ^ Wait 15 seconds, but NOT after the last variable
                    #     This is for the CDS API (rate limits), NOT MotherDuck
                    #     MotherDuck charges ZERO for idle time — only for queries

                except Exception as e:
                    print(f"  FAILED: {var_name} -- {e}")
                    failed.append((var_name, str(e)))
                    cleanup_nc_files()
                    #   ^ If ANY error happens for this variable:
                    #     1. Log it
                    #     2. Add to failed list
                    #     3. Clean up leftover files
                    #     4. The "for" loop CONTINUES to the next variable!
                    #        One failure doesn't stop the others.
```

This is the "continue-on-failure" pattern. If wind_speed fails, the code still tries cloud_cover, precipitation, etc. The failed ones will be automatically retried on the next DAG run (because their table's max_date hasn't changed).

---

## BLOCK 19 — Cleanup and summary

```python
        finally:
            cleanup_nc_files()
            conn.close()
        #   ^ "finally" = this code runs NO MATTER WHAT
        #     Even if the whole thing crashes, we:
        #     1. Delete leftover .nc files
        #     2. Close the MotherDuck connection
        #     This prevents leaked connections and disk clutter

        # -- Summary
        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"  Succeeded : {len(succeeded)}")
        print(f"  Skipped   : {len(skipped)} (already up to date)")
        print(f"  Failed    : {len(failed)}")
        if failed:
            for v, e in failed:
                print(f"  - {v}: {e}")
            print("Next run will retry only the failed variables.")
```

---

## BLOCK 20 — Wire it all together

```python
    retrieve_and_ingest_AgriEra5()    # Tell Airflow: "this task exists in the DAG"

agri_era5_in_taskflow()               # Tell Airflow: "register this DAG"
```

These two lines are required by Airflow. The first one says "the DAG contains this task." The second one actually creates the DAG object so Airflow can find it.

---

## The complete flow in plain English

```
You click "Trigger DAG" in Airflow
    |
    v
Connect to MotherDuck + Copernicus API
    |
    v
For each of the 11 variables (cheapest first):
    |
    |-- Ask MotherDuck: "what months + what's the last date?" (1 query)
    |
    |-- Remove the latest month from "done" list (it might be partial)
    |
    |-- Calculate: which months still need downloading?
    |
    |-- Nothing missing? -> print "up to date", next variable
    |
    |-- For each missing month:
    |    |
    |    |-- Already in "done" set? -> skip (instant Python check, 0 CU)
    |    |
    |    |-- Download from Copernicus API (with up to 3 retries)
    |    |
    |    |-- Convert NetCDF -> pandas table
    |    |
    |    |-- Is this the partial month? -> filter out days we already have
    |    |
    |    |-- INSERT new rows into MotherDuck
    |    |
    |    +-- Delete the downloaded file
    |
    |-- Wait 15 seconds (for API, costs 0 CU)
    |
    +-- If error -> log it, continue to next variable
    |
    v
Print summary: 8 succeeded, 2 skipped, 1 failed
Close database connection
```

---

## Key Python concepts used in this DAG

| Concept | What it means | Example in this code |
|---------|--------------|---------------------|
| `import` | Bring in a library | `import duckdb` |
| `def` | Define a function | `def get_agro_cutoff():` |
| `@decorator` | Wrap a function with extra behavior | `@dag(...)`, `@task(...)` |
| `try/except` | Handle errors without crashing | `try: ... except Exception: ...` |
| `try/finally` | Cleanup code that always runs | `finally: conn.close()` |
| `for x in list` | Loop over items | `for spec in sorted_specs:` |
| `if/else` | Conditional logic | `if need_create: ... else: ...` |
| `continue` | Skip to next iteration of loop | `continue` (skip this month) |
| `raise` | Re-throw an error | `raise` (give up retrying) |
| `f"..."` | Formatted string (variables inside `{}`) | `f"Latest: {max_date}"` |
| `{...}` (set) | Collection with no duplicates, fast lookup | `loaded_months = {(2021,1), ...}` |
| `[...]` (list) | Ordered collection | `failed = []` |
| `{key: value}` (dict) | Labeled data | `{"variable": "2m_temperature"}` |
| `**dict` | Spread/unpack a dictionary | `{**base, "year": ["2026"]}` |
| `lambda/key=` | Sort by custom rule | `sorted(specs, key=variable_cost)` |
| `with X as Y` | Auto-close resource when done | `with zipfile.ZipFile(...) as z:` |
| `os.getenv()` | Read secret from environment | `os.getenv("MOTHERDUCK_TOKEN")` |

## MotherDuck CU billing — what you need to know

- **Each query costs minimum 1 CU second** (even if it runs in 0.001 seconds)
- **Idle connections cost $0** — `time.sleep()` between variables is free
- **This DAG runs ~11 queries to check state** (1 per variable) instead of ~700+ in the old version
- **INSERT queries are unavoidable** — you need 1 per month per variable to save the data
- The biggest CU savings come from **avoiding unnecessary small queries**, not from connection management
