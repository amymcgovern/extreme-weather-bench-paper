"""Utilities for running EWB over internal MLWP data."""

import dataclasses
import datetime
from typing import Callable

import icechunk
import xarray as xr
from extremeweatherbench import inputs


@dataclasses.dataclass
class DateInterval:
    """Lightweight class for arithmetic on data intervals.

    Attributes:
        start_date: The start date of the interval.
        end_date: The end date of the interval.
    """

    start_date: datetime.date
    end_date: datetime.date

    def contains(self, other: "DateInterval") -> bool:
        """Return whether _this_ interval completely contains the other interval."""
        return (self.start_date <= other.start_date) and (
            self.end_date >= other.end_date
        )

    def overlaps(self, other: "DateInterval") -> bool:
        """Return whether _this_ interval overlaps with the other interval."""
        return (self.start_date <= other.end_date) and (
            self.end_date >= other.start_date
        )


class InMemoryForecast(inputs.ForecastBase):
    ds: xr.Dataset

    def __init__(
        self,
        ds: xr.Dataset,
        variables: list[str] | None = None,
        variable_mapping: dict[str, str] | None = None,
        source: str = "memory",
        name: str = "in-memory dataset",
        preprocess: Callable[[xr.Dataset], xr.Dataset] = inputs._default_preprocess,
        **kwargs,
    ):
        if variables is None:
            variables = []
        if variable_mapping is None:
            variable_mapping = {}
        super().__init__(
            source=source,
            name=name,
            variables=variables,
            variable_mapping=variable_mapping,
            **kwargs,
        )
        self.ds = ds

    def _open_data_from_source(self) -> xr.Dataset:
        return self.ds


BB_MLWP_VARIABLE_MAPPING = {
    "2m_temperature": "surface_air_temperature",
    "2m_dewpoint_temperature": "surface_dewpoint_temperature",
    "2m_relative_humidity": "surface_relative_humidity",
    "2m_wind_speed": "surface_wind_speed",
    "2m_wind_from_direction": "surface_wind_from_direction",
    "2m_wind_gust": "surface_wind_gust",
    "2m_wind_gust_direction": "surface_wind_gust_direction",
    "2m_wind_gust_speed": "surface_wind_gust_speed",
    "temperature": "air_temperature",
    "u_component_of_wind": "eastward_wind",
    "v_component_of_wind": "northward_wind",
    "10m_u_component_of_wind": "surface_eastward_wind",
    "10m_v_component_of_wind": "surface_northward_wind",
    "mean_sea_level_pressure": "air_pressure_at_mean_sea_level",
}

# TODO: Change me as needed
# Default Icechunk configuration - "Small" dataset
# DEFAULT_ICECHUNK_BUCKET = "extremeweatherbench"
# DEFAULT_ICECHUNK_PREFIX = "aifs-small-icechunk"
# DEFAULT_SOURCE_CREDENTIALS_PREFIX =
# "gs://brightband-scratch/darothen/ewb-forecast-archive/aifs-single/"
# # Credentials prefix must match the exact prefix stored in the repository config
# REF_INTERVAL = DateInterval(
#     start_date=datetime.date(2024, 3, 1),
#     end_date=datetime.date(2024, 6, 30),
# )

# Alternative Icechunk configuration - Full 4-year archive
DEFAULT_ICECHUNK_BUCKET = "extremeweatherbench"
AIFS_ICECHUNK_PREFIX = "aifs-single_20210102-20241231_icechunk"
GRAPHCAST_ICECHUNK_PREFIX = "graphcast-20210102-20241231_icechunk"
PANGU_ICECHUNK_PREFIX = "panguweather-20210102-20241231_icechunk"
# Credentials prefix must match the exact prefix stored in the repository config
AIFS_SOURCE_CREDENTIALS_PREFIX = "gs://brightband-scratch/darothen/aifs-single-archive/"
GRAPHCAST_SOURCE_CREDENTIALS_PREFIX = (
    "gs://brightband-scratch/darothen/graphcast-archive/"
)
PANGU_SOURCE_CREDENTIALS_PREFIX = (
    "gs://brightband-scratch/darothen/panguweather-archive/"
)
REF_INTERVAL = DateInterval(
    start_date=datetime.date(2021, 1, 2),
    end_date=datetime.date(2024, 12, 31),
)


def open_icechunk_dataset(
    bucket: str = DEFAULT_ICECHUNK_BUCKET,
    prefix: str = AIFS_ICECHUNK_PREFIX,
    variable_mapping: dict[str, str] | None = None,
    chunks: str | dict | None = "auto",
    source_credentials_prefix: str = AIFS_SOURCE_CREDENTIALS_PREFIX,
) -> xr.Dataset:
    """Open a dataset from an Icechunk repository with preprocessing.

    The repository config already knows where the virtual chunks are located.
    We just need to provide credentials broad enough to cover that location.

    Args:
        bucket: GCS bucket containing the Icechunk repository.
        prefix: Prefix within the bucket for the repository.
        variable_mapping: Dictionary mapping source variable names to target names.
        chunks: Chunk specification for xarray (default: "auto").
        source_credentials_prefix: GCS prefix for virtual chunk credentials.
            Should be broad enough to cover wherever the source data lives.

    Returns:
        Preprocessed xarray Dataset ready for evaluation.
    """
    print(f"Opening Icechunk repository at gs://{bucket}/{prefix}")

    # Set up storage
    storage = icechunk.gcs_storage(bucket=bucket, prefix=prefix)

    # Set up credentials for virtual chunks.
    # The repo config knows the exact location; we just provide credentials
    # broad enough to cover it.
    gcs_credentials = icechunk.gcs_from_env_credentials()
    virtual_credentials = icechunk.containers_credentials(
        {source_credentials_prefix: gcs_credentials}
    )

    # Open repository
    repo = icechunk.Repository.open(
        storage, authorize_virtual_chunk_access=virtual_credentials
    )
    session = repo.readonly_session("main")

    # Open dataset
    ds = xr.open_dataset(session.store, engine="zarr", chunks=chunks)
    print(f"Opened dataset with variables: {list(ds.data_vars)}")

    # Apply variable renaming if specified
    if variable_mapping:
        rename_dict = {k: v for k, v in variable_mapping.items() if k in ds.data_vars}
        if rename_dict:
            print(f"Renaming variables: {rename_dict}")
            ds = ds.rename(rename_dict)

    return ds
