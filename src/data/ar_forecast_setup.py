from pathlib import Path

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

ar_metrics = [
    ewb.metrics.CriticalSuccessIndex(),
    ewb.metrics.SpatialDisplacement(),
    ewb.metrics.EarlySignal(spatial_aggregation='half'),
]

class AtmosphericRiverForecastSetup:  
    def __init__(self):
        pass

    def get_cira_forecast(self, model_name, init_type, include_ivt=False):
        model_str = CIRA_MODEL_NAME_TO_SOURCE[model_name]
        source_str = f"gs://extremeweatherbench/{model_str}_{init_type}.parq"
        name_str = f"CIRA {model_name} {init_type}"

        if include_ivt:
            my_variables = [ewb.derived.AtmosphericRiverVariables()]
        else:
            my_variables = [ewb.derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])]

        cira_forecast = ewb.inputs.KerchunkForecast(
            source=source_str,
            variables=my_variables,
            variable_mapping=ewb.inputs.CIRA_metadata_variable_mapping,
            storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
            preprocess=ewb.defaults._preprocess_bb_ar_cira_forecast_dataset,
            name=name_str,
        )
        return cira_forecast

    def get_era5(self, include_ivt=False):
        if include_ivt:
            my_variables = [ewb.derived.AtmosphericRiverVariables()]
        else:
            my_variables = [ewb.derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])]
        
        era5 = ewb.inputs.ERA5(variables=my_variables)
        return era5

    def get_hres_forecast(self, include_ivt=False):
        if include_ivt:
            my_variables = [ewb.derived.AtmosphericRiverVariables()]
        else:
            my_variables = [ewb.derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])]

        hres_forecast = ewb.inputs.ZarrForecast(
            source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
            variables=my_variables,
            variable_mapping=ewb.inputs.HRES_metadata_variable_mapping,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
        )
        return hres_forecast
        

    def get_bb_hres_forecast(self, include_ivt=False):
        if include_ivt:
            my_variables = [ewb.derived.AtmosphericRiverVariables()]
        else:
            my_variables = [ewb.derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])]

        bb_hres_forecast = ArraylakeForecast(
            source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
            variables=my_variables,
            storage_options={"remote_options": {"anon": True}},
            name="ECMWF HRES",
            variable_mapping=BB_metadata_variable_mapping,
        )
        return bb_hres_forecast

    def get_bb_ar_forecast(self, model_name, include_ivt=False):
        if include_ivt:
            my_variables = [ewb.derived.AtmosphericRiverVariables()]
        else:
            my_variables = [ewb.derived.AtmosphericRiverVariables(output_variables=["atmospheric_river_land_intersection"])]

        bb_ar_ds = open_mlwp_archive_icechunk_dataset(
            model=model_name,
        )

        bb_severe_convection_forecast = InMemoryForecast(
            ds=bb_ar_ds,
            variables=my_variables,
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
                ewb.inputs.EvaluationObject(
                    event_type="atmospheric_river",
                    metric_list=ar_metrics,
                    target=ewb.defaults.era5_atmospheric_river_target,
                    forecast=forecast,
                )
            )
        return evaluation_objects
