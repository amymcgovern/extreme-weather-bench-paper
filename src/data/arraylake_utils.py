from dataclasses import dataclass

import xarray as xr
from arraylake import Client
from extremeweatherbench import inputs

BB_metadata_variable_mapping = {
    "isobaricInhPa": "level",
    "t2m": "surface_air_temperature",
    "t": "air_temperature",
    "q": "specific_humidity",
    "u": "eastward_wind",
    "v": "northward_wind",
    "p": "air_pressure",
    "z": "geopotential",
    "r": "relative_humidity",
    "u10": "surface_eastward_wind",
    "v10": "surface_northward_wind",
    "u100": "100m_eastward_wind",
    "v100": "100m_northward_wind",
    "msl": "air_pressure_at_mean_sea_level",
}


@dataclass
class ArraylakeForecast(inputs.ForecastBase):
    prefetch: bool = True

    def _prefetch_data(self):
        client = Client()
        repo = client.get_repo(f"{self.org_name}/{self.repo_name}")
        session = repo.readonly_session(self.branch_name)
        self.ds = xr.open_zarr(session.store, group=self.group_name)
        self.ds = self.ds.assign_coords(
            {"lead_time": self.ds.lead_time.astype("timedelta64[h]")}
        )

    def __post_init__(self):
        # source should be like arraylake://org_name/repo_name@branch_name/group/name/goes/here
        if not self.source.startswith("arraylake://"):
            raise ValueError("source must start with arraylake://")
        bits = self.source.split("://")[1].split("/")
        self.org_name = bits[0]
        if "@" not in bits[1]:
            self.branch_name = "main"
            self.repo_name = bits[1]
        else:
            self.repo_name, self.branch_name = bits[1].split("@")
        self.group_name = "/".join(bits[2:])
        if self.prefetch:
            self._prefetch_data()

    def _open_data_from_source(self) -> xr.Dataset:
        if self.ds is None:
            self._prefetch_data()
        return self.ds
