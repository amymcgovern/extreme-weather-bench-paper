# setup all the imports

from pathlib import Path

from extremeweatherbench import cases, defaults, derived, evaluate, inputs, metrics

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"


# setup the templates to load in the data

# Forecast Examples

cira_AR_FOURv2_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=[
        derived.AtmosphericRiverVariables(
            output_variables=["atmospheric_river_land_intersection"]
        )
    ],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA FOURv2 GFS",
)

cira_AR_GC_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=[
        derived.AtmosphericRiverVariables(
            output_variables=["atmospheric_river_land_intersection"]
        )
    ],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA GC GFS",
)

cira_AR_PANG_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=[
        derived.AtmosphericRiverVariables(
            output_variables=["atmospheric_river_land_intersection"]
        )
    ],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA PANG GFS",
)

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


ar_metrics = [
    metrics.CriticalSuccessIndex(),
    metrics.SpatialDisplacement(),
    metrics.EarlySignal(),
]


FOURv2_AR_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="atmospheric_river",
        metric_list=ar_metrics,
        target=defaults.era5_atmospheric_river_target,
        forecast=cira_AR_FOURv2_GFSforecast,
    ),
]

GC_AR_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="atmospheric_river",
        metric_list=ar_metrics,
        target=defaults.era5_atmospheric_river_target,
        forecast=cira_AR_GC_GFSforecast,
    ),
]

PANG_AR_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="atmospheric_river",
        metric_list=ar_metrics,
        target=defaults.era5_atmospheric_river_target,
        forecast=cira_AR_PANG_GFSforecast,
    ),
]

HRES_AR_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="atmospheric_river",
        metric_list=ar_metrics,
        target=defaults.era5_atmospheric_river_target,
        forecast=hres_forecast,
    ),
]

# load in all of the events in the yaml file
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
ewb_cases = ewb_cases.select_cases("event_type", "atmospheric_river")

bad_case_ids = [114, 115, 116, 117, 118, 119]
my_cases = [
    case
    for case in ewb_cases.cases
    if case.case_id_number < 120
    and case.case_id_number >= 110
    and case.case_id_number not in bad_case_ids
]
ewb_cases = cases.IndividualCaseCollection(cases=my_cases)


ewb_fourv2 = evaluate.ExtremeWeatherBench(ewb_cases, FOURv2_AR_EVALUATION_OBJECTS)
ewb_gc = evaluate.ExtremeWeatherBench(ewb_cases, GC_AR_EVALUATION_OBJECTS)
ewb_pang = evaluate.ExtremeWeatherBench(ewb_cases, PANG_AR_EVALUATION_OBJECTS)
ewb_hres = evaluate.ExtremeWeatherBench(ewb_cases, HRES_AR_EVALUATION_OBJECTS)


# load in the results for all heat waves in parallel
# this will take awhile to run if you do them all in one code box
# if you have already saved them (from running this once), then skip this box
parallel_config = {"backend": "loky", "n_jobs": 24}

# fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
# gc_results = ewb_gc.run(parallel_config=parallel_config)
# pang_results = ewb_pang.run(parallel_config=parallel_config)
hres_results = ewb_hres.run(n_jobs=1)

# save the results to make it more efficient
# fourv2_results.to_pickle(basepath + "saved_data/fourv2_ar_results.pkl")
# gc_results.to_pickle(basepath + "saved_data/gc_ar_results.pkl")
# pang_results.to_pickle(basepath + "saved_data/pang_ar_results.pkl")
hres_results.to_pickle(basepath + "saved_data/hres_ar_results.pkl")
