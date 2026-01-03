"""Utilities for running EWB over internal MLWP data."""

import dataclasses
import datetime
from typing import Callable

import xarray as xr
from extremeweatherbench import inputs

#: Mapping from MLWP lexicon -> EWB lexicon
MLWP_MODEL_VARIABLE_MAPPING = {
    "2m_temperature": "surface_air_temperature",
}


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
