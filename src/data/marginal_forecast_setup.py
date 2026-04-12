from pathlib import Path
import extremeweatherbench as ewb
import operator
import xarray as xr
from src.data.aifs_util import (
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    InMemoryForecast,
)  # noqa: E402
from check_icechunk import open_mlwp_archive_icechunk_dataset

from src.data.arraylake_utils import ArraylakeForecast  # noqa: E402
from src.data.model_name_setup import (
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX,
    BB_MODEL_NAME_TO_PREFIX,
    CIRA_MODEL_NAME_TO_SOURCE,
)

marginal_temperature_metrics = [
    ewb.metrics.RootMeanSquaredError(),
]

def my_preprocess_marginal_temperature_cira_forecast_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = ewb.defaults.preprocess_cira_kerchunk_forecast_dataset(ds)
    ds = ewb.defaults.preprocess_heatwave_forecast_dataset(ds)
    return ds

class MarginalTemperatureForecastSetup:
    def __init__(self):
        pass

    def get_cira_marginal_temperature_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_marginal_temperature_forecast = ewb.inputs.KerchunkForecast(
            source=source_str,
            variables=["surface_air_temperature"],
            variable_mapping={"t2": "surface_air_temperature"},
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=my_preprocess_marginal_temperature_cira_forecast_dataset,
            name=name_str,
        )
        return cira_marginal_temperature_forecast

    def get_hres_marginal_temperature_forecast(self):
        hres_marginal_temperature_forecast = ewb.inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=["surface_air_temperature"],
            variable_mapping=ewb.inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )
        return hres_marginal_temperature_forecast

    def get_bb_hres_marginal_temperature_forecast(self):
        bb_hres_marginal_temperature_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=["surface_air_temperature"],
            variable_mapping={
                "t2m": "surface_air_temperature",
            },
            name="ECMWF HRES",
        )
        return bb_hres_marginal_temperature_forecast

    def get_bb_marginal_temperature_forecast(self, model_name):
        bb_marginal_temperature_ds = open_mlwp_archive_icechunk_dataset(
            model=model_name,
        )
        bb_marginal_temperature_forecast = InMemoryForecast(
            bb_marginal_temperature_ds,
            name=f"BB {model_name}",
            variables=["surface_air_temperature"],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
        )
        return bb_marginal_temperature_forecast


class MarginalTemperatureEvaluationSetup:
    def __init__(self):
        pass

    def get_marginal_temperature_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                ewb.inputs.EvaluationObject(
                    event_type="marginal_temperature",
                    metric_list=marginal_temperature_metrics,
                    target=ewb.defaults.ghcn_heatwave_target,
                    forecast=forecast,
                )
            )
            evaluation_objects.append(
                ewb.inputs.EvaluationObject(
                    event_type="marginal_temperature",
                    metric_list=marginal_temperature_metrics,
                    target=ewb.defaults.era5_heatwave_target,
                    forecast=forecast,
                )
            )
        return evaluation_objects
