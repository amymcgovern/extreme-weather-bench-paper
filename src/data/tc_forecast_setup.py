from pathlib import Path
import xarray as xr
from extremeweatherbench import defaults, inputs, metrics, derived

from src.data.aifs_util import (
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    InMemoryForecast,
    open_icechunk_dataset,
)  # noqa: E402
from src.data.arraylake_utils import (
    ArraylakeForecast,
    BB_metadata_variable_mapping,
) # noqa: E402

from src.data.model_name_setup import (
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX,
    BB_MODEL_NAME_TO_PREFIX,
    CIRA_MODEL_NAME_TO_SOURCE,
)

composite_landfall_metrics = [
    metrics.LandfallMetric(
        metrics=[
            metrics.LandfallIntensityMeanAbsoluteError,
            metrics.LandfallTimeMeanError,
            metrics.LandfallDisplacement,
        ],
        approach="next",
    )
]

# Preprocessing function for MLWP data that includes geopotential thickness calculation
# required for tropical cyclone tracks
def preprocess_mlwp_tc_dataset(ds: xr.Dataset) -> xr.Dataset:
    """A function to process the MLWP dataset for tropical cyclone tracks.
    """

    # Calculate the geopotential thickness required for tropical cyclone tracks
    ds["geopotential_thickness"] = ds["geopotential"] / 9.81
    return ds

class TropicalCycloneForecastSetup:
    def __init__(self):
        pass

    def get_cira_tc_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_tc_forecast = inputs.KerchunkForecast(
            source=source_str,
            variables=[derived.TropicalCycloneTrackVariables()],            
            variable_mapping=inputs.CIRA_metadata_variable_mapping,
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=defaults._preprocess_bb_cira_tc_forecast_dataset,
            name=name_str,
        )
        return cira_tc_forecast

    def get_hres_forecast(self):
        hres_tc_forecast = inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=[derived.TropicalCycloneTrackVariables()],            
            preprocess=defaults._preprocess_bb_hres_tc_forecast_dataset,
            variable_mapping=inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )

        return hres_tc_forecast

    def get_bb_hres_forecast(self):
        bb_hres_tc_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=[derived.TropicalCycloneTrackVariables()],
            preprocess=defaults._preprocess_bb_hres_tc_forecast_dataset,
            variable_mapping=BB_metadata_variable_mapping,
            name="ECMWF HRES",
        )
        return bb_hres_tc_forecast

    def get_bb_tc_forecast(self, model_name):
        bb_tc_ds = open_icechunk_dataset(
            bucket=DEFAULT_ICECHUNK_BUCKET,
            prefix=BB_MODEL_NAME_TO_PREFIX[model_name],
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
            chunks="auto",
            source_credentials_prefix=BB_MODEL_NAME_TO_CREDENTIALS_PREFIX[model_name],
        )
        bb_tc_forecast = InMemoryForecast(
            bb_tc_ds,
            name=f"BB {model_name}",
            variables=[derived.TropicalCycloneTrackVariables()],
            preprocess=preprocess_mlwp_tc_dataset,
            variable_mapping=BB_MLWP_VARIABLE_MAPPING,
        )
        return bb_tc_forecast


class TropicalCycloneEvaluationSetup:
    def __init__(self):
        pass

    def get_tc_evaluation_objects(self, forecasts):
        evaluation_objects = []
        for forecast in forecasts:
            evaluation_objects.append(
                inputs.EvaluationObject(
                            event_type="tropical_cyclone",
                            metric_list=composite_landfall_metrics,
                            target=defaults.ibtracs_target,
                            forecast=forecast,
                        )
                    )
        return evaluation_objects

    


