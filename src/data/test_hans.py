from arraylake import Client
import xarray as xr
client = Client()
repo_name = "brightband/ecmwf"
group_name = "forecast-archive/ewb-hres"
branch_name = "main"
repo = client.get_repo(repo_name)
session = repo.readonly_session(branch_name)
ds = xr.open_zarr(session.store, group=group_name)