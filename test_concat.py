"""Quick test: xr.concat vs xr.merge on 1 month of precipitation_flux."""
import cdsapi
import zipfile
import os
import xarray as xr

CDSAPI_KEY = os.getenv("CDSAPI_KEY")
EXTRACT_DIR = "/tmp/test_concat"
os.makedirs(EXTRACT_DIR, exist_ok=True)

client = cdsapi.Client("https://cds.climate.copernicus.eu/api", CDSAPI_KEY)
target = os.path.join(EXTRACT_DIR, "test.zip")

request = {
    "variable": "precipitation_flux",
    "version": "2_0",
    "area": [12.7, 41.7, 10.9, 43.5],
    "year": ["2025"],
    "month": ["01"],
    "day": [str(d).zfill(2) for d in range(1, 32)],
    "data_format": "netcdf",
    "download_format": "zip",
}

print("Downloading 1 month of precipitation_flux...")
client.retrieve("sis-agrometeorological-indicators", request, target)

with zipfile.ZipFile(target) as z:
    z.extractall(EXTRACT_DIR)
os.remove(target)

nc_paths = sorted(
    os.path.join(EXTRACT_DIR, f)
    for f in os.listdir(EXTRACT_DIR)
    if f.endswith('.nc')
)
print(f"{len(nc_paths)} .nc files")

datasets = [xr.open_dataset(nc) for nc in nc_paths]

# Test xr.merge (old — broken)
merge_ds = xr.merge(datasets, join="outer", compat="override")
merge_df = merge_ds.to_dataframe().reset_index().drop(columns=["crs"], errors="ignore")
merge_nulls = merge_df["Precipitation_Flux"].isnull().sum()
print(f"\nxr.merge(join='outer'): {merge_df.shape}, NULLs={merge_nulls}/{len(merge_df)} ({100*merge_nulls/len(merge_df):.1f}%)")
print(f"  columns: {list(merge_df.columns)}")

# Test xr.concat (new — fix)
concat_ds = xr.concat(datasets, dim="time")
concat_df = concat_ds.to_dataframe().reset_index().drop(columns=["crs"], errors="ignore")
concat_nulls = concat_df["Precipitation_Flux"].isnull().sum()
print(f"\nxr.concat(dim='time'): {concat_df.shape}, NULLs={concat_nulls}/{len(concat_df)} ({100*concat_nulls/len(concat_df):.1f}%)")
print(f"  columns: {list(concat_df.columns)}")
print(f"  distinct dates: {concat_df['time'].nunique()}")
print(f"  date range: {concat_df['time'].min()} to {concat_df['time'].max()}")

merge_ds.close()
concat_ds.close()
for ds in datasets:
    ds.close()
for nc in nc_paths:
    os.remove(nc)

print("\nDONE")
