from pathlib import Path
import xarray as xr

import extremeweatherbench as ewb

from src.data.aifs_util import (
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    InMemoryForecast,
)  # noqa: E402
from src.data.arraylake_utils import (
    ArraylakeForecast, BB_metadata_variable_mapping,
) # noqa: E402
from check_icechunk import open_mlwp_archive_icechunk_dataset

from src.data.model_name_setup import (
    BB_MODEL_NAME_TO_CREDENTIALS_PREFIX,
    BB_MODEL_NAME_TO_PREFIX,
    CIRA_MODEL_NAME_TO_SOURCE,
)

composite_landfall_metrics = [
    ewb.metrics.LandfallMetric(
        metrics=[
            ewb.metrics.LandfallIntensityMeanAbsoluteError(),
            ewb.metrics.LandfallTimeMeanError(),
            ewb.metrics.LandfallDisplacement(),
        ],
        approach="first",
    )
]

# Preprocessing function for MLWP data that includes geopotential thickness calculation
# required for tropical cyclone tracks
def preprocess_mlwp_tc_dataset(ds: xr.Dataset) -> xr.Dataset:
    """A function to process the MLWP dataset for tropical cyclone tracks.
    """

    # Calculate the geopotential thickness required for tropical cyclone tracks
    ds["geopotential_thickness"] = (
        ewb.calc.geopotential_thickness(
            ds["geopotential"], top_level=300, bottom_level=500
        ) / 9.81
    )
    return ds

# Preprocessing function for MLWP data that includes geopotential thickness calculation
# required for tropical cyclone tracks
def preprocess_bb_hres_tc_dataset(ds: xr.Dataset) -> xr.Dataset:
    """A function to process the MLWP dataset for tropical cyclone tracks.
    """

    # Calculate the geopotential thickness required for tropical cyclone tracks
    ds["geopotential_thickness"] = ewb.calc.geopotential_thickness(
        ds["z"], top_level=300, bottom_level=500, pressure_dim="isobaricInhPa"
    ) / 9.81
    return ds

class TropicalCycloneForecastSetup:
    def __init__(self):
        pass

    def get_cira_tc_forecast(self, model_name, init_type):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        cira_tc_forecast = ewb.inputs.KerchunkForecast(
            source=source_str,
            variables=[ewb.derived.TropicalCycloneTrackVariables()],            
            variable_mapping=ewb.inputs.CIRA_metadata_variable_mapping,
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=ewb.defaults.preprocess_cira_kerchunk_tc_forecast_dataset,
            name=name_str,
        )
        return cira_tc_forecast

    def get_hres_forecast(self):
        hres_tc_forecast = ewb.inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=[ewb.derived.TropicalCycloneTrackVariables()],            
            preprocess=ewb.defaults.preprocess_hres_tc_forecast_dataset,
            variable_mapping=ewb.inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )

        return hres_tc_forecast

    def get_bb_hres_forecast(self):
        bb_hres_tc_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=[ewb.derived.TropicalCycloneTrackVariables()],
            preprocess=preprocess_bb_hres_tc_dataset,
            storage_options={"remote_options": {"anon": True}},
            variable_mapping=BB_metadata_variable_mapping,
            name="ECMWF HRES",
        )
        return bb_hres_tc_forecast

    def get_bb_tc_forecast(self, model_name):
        bb_tc_ds = open_mlwp_archive_icechunk_dataset(
            model=model_name,
        )

        bb_tc_forecast = InMemoryForecast(
            bb_tc_ds,
            name=f"BB {model_name}",
            variables=[ewb.derived.TropicalCycloneTrackVariables()],
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
                ewb.inputs.EvaluationObject(
                            event_type="tropical_cyclone",
                            metric_list=composite_landfall_metrics,
                            target=ewb.defaults.ibtracs_target,
                            forecast=forecast,
                        )
                    )
        return evaluation_objects

    


