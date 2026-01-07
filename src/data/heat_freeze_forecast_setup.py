from pathlib import Path

from extremeweatherbench import defaults, inputs, metrics

from src.data.aifs_util import (
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    InMemoryForecast,
    open_icechunk_dataset,
)  # noqa: E402
from src.data.arraylake_utils import ArraylakeForecast  # noqa: E402
from src.data.model_name_setup import (
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX,
    BB_MODEL_NAME_TO_PREFIX,
    CIRA_MODEL_NAME_TO_SOURCE,
)

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

heat_metrics = [
    metrics.MaximumMeanAbsoluteError,
    metrics.RootMeanSquaredError,
    metrics.MaximumLowestMeanAbsoluteError,
    # metrics.MeanSquaredError(
    #     name="threshold_weighted_mse", interval_where_one=(313.15, np.inf)
    # ),
]

freeze_metrics = [
    metrics.MinimumMeanAbsoluteError,
    metrics.RootMeanSquaredError,
]


class heat_freeze_forecast_setup:
    def __init__(self):
        pass

    def get_cira_heat_freeze_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_heat_freeze_forecast = inputs.KerchunkForecast(
            source=source_str,
            variables=["surface_air_temperature"],
            variable_mapping={"t2": "surface_air_temperature"},
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=defaults._preprocess_bb_cira_forecast_dataset,
            name=name_str,
        )
        return cira_heat_freeze_forecast

    def get_hres_heat_freeze_forecast(self):
        hres_heat_freeze_forecast = inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=["surface_air_temperature"],
            variable_mapping=inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )
        return hres_heat_freeze_forecast

    def get_bb_hres_heat_freeze_forecast(self):
        bb_hres_heat_freeze_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=["surface_air_temperature"],
            variable_mapping={
                "t2m": "surface_air_temperature",
            },
            name="ECMWF HRES",
        )
        return bb_hres_heat_freeze_forecast

    def get_bb_heat_freeze_forecast(self, model_name):
        bb_heat_freeze_ds = open_icechunk_dataset(
            bucket=DEFAULT_ICECHUNK_BUCKET,
            prefix=BB_MODEL_NAME_TO_PREFIX[model_name],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            chunks="auto",
            source_credentials_prefix=BB_MODEL_NAME_TO_CREDENTIALS_PREFIX[model_name],
        )
        bb_heat_freeze_forecast = InMemoryForecast(
            bb_heat_freeze_ds,
            name=f"BB {model_name}",
            variables=["surface_air_temperature"],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
        )
        return bb_heat_freeze_forecast


class heat_freeze_evaluation_setup:
    def __init__(self):
        pass

    def get_heat_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="heat_wave",
                    metric_list=heat_metrics,
                    target=defaults.ghcn_heatwave_target,
                    forecast=forecast,
                )
            )
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="heat_wave",
                    metric_list=heat_metrics,
                    target=defaults.era5_heatwave_target,
                    forecast=forecast,
                )
            )
        return evaluation_objects

    def get_freeze_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="freeze",
                    metric_list=freeze_metrics,
                    target=defaults.ghcn_heatwave_target,
                    forecast=forecast,
                )
            )
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="freeze",
                    metric_list=freeze_metrics,
                    target=defaults.era5_freeze_target,
                    forecast=forecast,
                )
            )

        return evaluation_objects
