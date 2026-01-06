from pathlib import Path

from ewb_paper.data.aifs_util import (
    AIFS_ICECHUNK_PREFIX,
    AIFS_SOURCE_CREDENTIALS_PREFIX,
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    GRAPHCAST_ICECHUNK_PREFIX,
    GRAPHCAST_SOURCE_CREDENTIALS_PREFIX,
    PANGU_ICECHUNK_PREFIX,
    PANGU_SOURCE_CREDENTIALS_PREFIX,
    InMemoryForecast,
    open_icechunk_dataset,
)
from ewb_paper.data.arraylake_utils import ArraylakeForecast
from extremeweatherbench import defaults, inputs, metrics

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
    INIT_TYPES = ["IFS", "GFS"]
    CIRA_MODEL_NAMES = ["Fourv2", "Graphcast", "Pangu"]
    CIRA_MODEL_NAME_TO_SOURCE = {
        "Fourv2": "FOUR_v200",
        "Graphcast": "GRAP_v100",
        "Pangu": "PANG_v100",
    }

    BB_MODEL_NAMES = ["AIFS", "Graphcast", "Pangu"]
    BB_MODEL_NAME_TO_PREFIX = {
        "AIFS": AIFS_ICECHUNK_PREFIX,
        "Graphcast": GRAPHCAST_ICECHUNK_PREFIX,
        "Pangu": PANGU_ICECHUNK_PREFIX,
    }
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX = {
        "AIFS": AIFS_SOURCE_CREDENTIALS_PREFIX,
        "Graphcast": GRAPHCAST_SOURCE_CREDENTIALS_PREFIX,
        "Pangu": PANGU_SOURCE_CREDENTIALS_PREFIX,
    }

    def __init__(self):
        pass

    def get_cira_heat_freeze_forecast(self, model_name, init_type):
        model_str = self.CIRA_MODEL_NAMES.index(model_name)
        init_str = self.INIT_TYPES.index(init_type)
        source_str = f"gs://extremeweatherbench/{model_str}_{init_str}.parq"
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
            prefix=self.BB_MODEL_NAME_TO_PREFIX[model_name],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            chunks="auto",
            source_credentials_prefix=self.BB_MODEL_NAME_TO_CREDENTIALS_PREFIX[
                model_name
            ],
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

        return evaluation_objects
