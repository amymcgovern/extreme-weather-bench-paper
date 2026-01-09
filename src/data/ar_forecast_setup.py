from pathlib import Path

from extremeweatherbench import defaults, inputs, metrics, derived

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

ar_metrics = [
    metrics.CriticalSuccessIndex(),
    metrics.SpatialDisplacement(),
    metrics.EarlySignal(),
]

class AtmosphericRiverForecastSetup:  
    def __init__(self):
        pass

    def get_cira_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_forecast = inputs.KerchunkForecast(
            source=source_str,
            variables=[
            derived.AtmosphericRiverVariables(
                output_variables=["atmospheric_river_land_intersection"]
            )
            ],
            variable_mapping=inputs.CIRA_metadata_variable_mapping,
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
            name=name_str,
        )
        return cira_forecast

    def get_hres_forecast(self):
        hres_forecast = inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=[
                derived.AtmosphericRiverVariables(
                    output_variables=["atmospheric_river_land_intersection"]
                )
            ],
            variable_mapping=inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )
        return hres_forecast

    def get_bb_hres_forecast(self):
        bb_hres_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=[derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])],
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
            variable_mapping=inputs.HRES_metadata_variable_mapping,
        )
        return bb_hres_forecast

    def get_bb_ar_forecast(self, model_name):
        bb_ar_ds = open_icechunk_dataset(
            bucket=DEFAULT_ICECHUNK_BUCKET,
            prefix=BB_MODEL_NAME_TO_PREFIX[model_name],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            chunks="auto",
            source_credentials_prefix=BB_MODEL_NAME_TO_CREDENTIALS_PREFIX[model_name],
        )

        bb_severe_convection_forecast = InMemoryForecast(
            ds=bb_ar_ds,
            variables=[derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            name=f"BB {model_name}",
        )
        return bb_severe_convection_forecast
        

class AtmosphericRiverEvaluationSetup:
    def __init__(self):
        pass
    
    def get_ar_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                inputs.EvaluationObject(
                    event_type="atmospheric_river",
                    metric_list=ar_metrics,
                    target=defaults.era5_atmospheric_river_target,
                    forecast=forecast,
                )
            )
        return evaluation_objects
