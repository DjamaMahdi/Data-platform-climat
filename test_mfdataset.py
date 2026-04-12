"""Test: xr.open_mfdataset for multi-statistic variable (7 stats temperature)."""
import cdsapi
import zipfile
import os
import xarray as xr

CDSAPI_KEY = os.getenv("CDSAPI_KEY")
EXTRACT_DIR = "/tmp/test_mf"
os.makedirs(EXTRACT_DIR, exist_ok=True)

client = cdsapi.Client("https://cds.climate.copernicus.eu/api", CDSAPI_KEY)
target = os.path.join(EXTRACT_DIR, "test.zip")

# Temperature with 7 statistics — the hardest case
request = {
    "variable": "2m_temperature",
    "statistic": [
        "24_hour_maximum", "24_hour_mean", "24_hour_minimum",
        "day_time_maximum", "day_time_mean",
        "night_time_mean", "night_time_minimum",
    ],
    "version": "2_0",
    "area": [12.7, 41.7, 10.9, 43.5],
    "year": ["2025"],
    "month": ["01"],
    "day": [str(d).zfill(2) for d in range(1, 32)],
    "data_format": "netcdf",
    "download_format": "zip",
}

print("Downloading 2m_temperature (7 stats, Jan 2025)...")
client.retrieve("sis-agrometeorological-indicators", request, target)

with zipfile.ZipFile(target) as z:
    nc_names = [n for n in z.namelist() if n.endswith('.nc')]
    print(f"ZIP: {len(nc_names)} .nc files")
    z.extractall(EXTRACT_DIR)
os.remove(target)

nc_paths = sorted(
    os.path.join(EXTRACT_DIR, f)
    for f in os.listdir(EXTRACT_DIR)
    if f.endswith('.nc')
)

# Show first few file names to see the pattern
print(f"\nFirst 10 .nc files:")
for p in nc_paths[:10]:
    print(f"  {os.path.basename(p)}")

# open_mfdataset
print(f"\nUsing xr.open_mfdataset(combine='by_coords')...")
ds = xr.open_mfdataset(nc_paths, combine="by_coords")
df = ds.to_dataframe().reset_index()
df = df.drop(columns=["crs"], errors="ignore")
ds.close()

print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"Distinct dates: {df['time'].nunique()}")
print(f"Grid points: {len(df) // df['time'].nunique()}")

# NULL check per column
print(f"\nNULL counts:")
for col in df.columns:
    nulls = df[col].isnull().sum()
    print(f"  {col}: {nulls}/{len(df)} ({100*nulls/len(df):.1f}%)")

print(f"\nFirst 3 rows:")
print(df.head(3).to_string(index=False))

# Also test single-stat variable (precipitation_flux)
print(f"\n{'='*60}")
print("Also testing precipitation_flux (single var)...")
target2 = os.path.join(EXTRACT_DIR, "test2.zip")
req2 = {
    "variable": "precipitation_flux",
    "version": "2_0",
    "area": [12.7, 41.7, 10.9, 43.5],
    "year": ["2025"],
    "month": ["01"],
    "day": [str(d).zfill(2) for d in range(1, 32)],
    "data_format": "netcdf",
    "download_format": "zip",
}
client.retrieve("sis-agrometeorological-indicators", req2, target2)
with zipfile.ZipFile(target2) as z:
    z.extractall(EXTRACT_DIR)
os.remove(target2)
nc2 = sorted(os.path.join(EXTRACT_DIR, f) for f in os.listdir(EXTRACT_DIR) if f.endswith('.nc'))
ds2 = xr.open_mfdataset(nc2, combine="by_coords")
df2 = ds2.to_dataframe().reset_index().drop(columns=["crs"], errors="ignore")
ds2.close()
nulls2 = df2[df2.columns[-1]].isnull().sum()
print(f"Shape: {df2.shape}, NULLs: {nulls2}/{len(df2)} ({100*nulls2/len(df2):.1f}%)")
print(f"Columns: {list(df2.columns)}")

for f in os.listdir(EXTRACT_DIR):
    if f.endswith('.nc'):
        os.remove(os.path.join(EXTRACT_DIR, f))

print("\nDONE")
