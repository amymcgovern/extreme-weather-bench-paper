from extremeweatherbench import (  # noqa: E402
    defaults,
    derived,
    inputs,
    metrics,
)

from src.data.aifs_util import (
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    InMemoryForecast,
    open_icechunk_dataset,
)
from src.data.arraylake_utils import (  # noqa: E402
    ArraylakeForecast,
    BB_metadata_variable_mapping,
)
from src.data.model_name_setup import (
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX,
    BB_MODEL_NAME_TO_PREFIX,
    CIRA_MODEL_NAME_TO_SOURCE,
)

# Define threshold metrics
pph_metrics = [
    metrics.ThresholdMetric(
        metrics=[
            metrics.CriticalSuccessIndex(),
            metrics.FalseAlarmRatio(),
        ],
        forecast_threshold=15000,
        target_threshold=0.01,
    ),
    metrics.EarlySignal(threshold=15000),
]

# Define LSR metrics
lsr_metrics = [
    metrics.ThresholdMetric(
        metrics=[
            metrics.TruePositives(),
            metrics.FalseNegatives(),
        ],
        forecast_threshold=15000,
        target_threshold=0.5,
    )
]


class SevereForecastSetup:
    def __init__(self):
        pass

    def get_cira_severe_convection_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_severe_convection_forecast = inputs.KerchunkForecast(
            source=source_str,
            variables=[derived.CravenBrooksSignificantSevere()],
            variable_mapping=inputs.CIRA_metadata_variable_mapping,
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            name=name_str,
            preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
        )
        return cira_severe_convection_forecast

    def get_hres_severe_convection_forecast(self):
        hres_severe_convection_forecast = inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=[derived.CravenBrooksSignificantSevere()],
            variable_mapping=inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )
        return hres_severe_convection_forecast

    def get_bb_hres_severe_convection_forecast(self):
        bb_hres_severe_convection_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=[derived.CravenBrooksSignificantSevere()],
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
            variable_mapping=BB_metadata_variable_mapping,
        )
        return bb_hres_severe_convection_forecast

    def get_bb_severe_convection_forecast(self, model_name):
        bb_severe_ds = open_icechunk_dataset(
            bucket=DEFAULT_ICECHUNK_BUCKET,
            prefix=BB_MODEL_NAME_TO_PREFIX[model_name],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            chunks="auto",
            source_credentials_prefix=BB_MODEL_NAME_TO_CREDENTIALS_PREFIX[model_name],
        )

        bb_severe_convection_forecast = InMemoryForecast(
            ds=bb_severe_ds,
            variables=[derived.CravenBrooksSignificantSevere()],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            name=f"BB {model_name}",
        )
        return bb_severe_convection_forecast


class SevereEvaluationSetup:
    def __init__(self):
        pass

    def get_severe_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="severe_convection",
                    metric_list=lsr_metrics,
                    target=defaults.lsr_target,
                    forecast=forecast,
                )
            )
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="severe_convection",
                    metric_list=pph_metrics,
                    target=defaults.pph_target,
                    forecast=forecast,
                )
            )
        return evaluation_objects
