# setup all the imports
from pathlib import Path  # noqa: E402

from extremeweatherbench import cases, defaults, evaluate, inputs, metrics

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

import argparse

import pandas as pd
from aifs_util import (
    AIFS_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    DEFAULT_ICECHUNK_PREFIX,
    DEFAULT_SOURCE_CREDENTIALS_PREFIX,
    InMemoryForecast,
    open_icechunk_dataset,
)
from arraylake_utils import ArraylakeForecast

# setup the templates to load in the data

# Forecast Examples
cira_freeze_forecast_FOURv2_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA FOURv2 IFS",
)

cira_freeze_forecast_GC_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA GC IFS",
)

cira_freeze_forecast_PANG_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA PANG IFS",
)

cira_freeze_forecast_FOURv2_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA FOURv2 GFS",
)

cira_freeze_forecast_GC_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA GC GFS",
)

cira_freeze_forecast_PANG_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA PANG GFS",
)

hres_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    variables=["surface_air_temperature"],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
)

bb_hres_forecast = ArraylakeForecast(
    source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
    variables=["surface_air_temperature"],
    variable_mapping={
        "t2m": "surface_air_temperature",
    },
    name="ECMWF HRES",
)
ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=DEFAULT_ICECHUNK_PREFIX,
    variable_mapping=AIFS_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=DEFAULT_SOURCE_CREDENTIALS_PREFIX,
)

aifs_forecast = InMemoryForecast(
    ds,
    name="AIFS",
    variables=["surface_air_temperature"],
    variable_mapping=AIFS_VARIABLE_MAPPING,
)


freeze_metrics = [
    metrics.MinimumMeanAbsoluteError,
    metrics.RootMeanSquaredError,
]

BB_HRES_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_freeze_target,
        forecast=bb_hres_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_freeze_target,
        forecast=bb_hres_forecast,
    ),
]


FOURv2_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_FOURv2_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_FOURv2_GFS,
    ),
]

GC_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_GC_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_GC_GFS,
    ),
]

PANG_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_freeze_forecast_PANG_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_freeze_forecast_PANG_GFS,
    ),
]

HRES_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=hres_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=hres_forecast,
    ),
]

AIFS_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=aifs_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=aifs_forecast,
    ),
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run heat wave evaluation against ExtremeWeatherBench cases."
    )

    # run whichever cases the user specifies

    # Icechunk options
    parser.add_argument(
        "--run_aifs",
        action="store_true",
        default=False,
        help="Run AIFS evaluation (default: False)",
    )

    parser.add_argument(
        "--run_hres",
        action="store_true",
        default=False,
        help="Run HRES evaluation (default: False)",
    )

    parser.add_argument(
        "--run_pangu",
        action="store_true",
        default=False,
        help="Run Pangu evaluation (default: False)",
    )

    parser.add_argument(
        "--run_fourv2",
        action="store_true",
        default=False,
        help="Run FOURv2 evaluation (default: False)",
    )

    parser.add_argument(
        "--run_gc",
        action="store_true",
        default=False,
        help="Run GC evaluation (default: False)",
    )

    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "freeze")

    # load in the results for all freeze cases in parallel
    # this will take awhile to run if you do them all in one code box
    # if you have already saved them (from running this once), then skip this box
    parallel_config = {"backend": "loky", "n_jobs": 32}

    if args.run_aifs:
        print("running AIFS evaluation")
        ewb_aifs = evaluate.ExtremeWeatherBench(
            ewb_cases, AIFS_FREEZE_EVALUATION_OBJECTS
        )
        aifs_results = ewb_aifs.run(parallel_config=parallel_config)
        aifs_results.to_pickle(basepath + "saved_data/aifs_freeze_results.pkl")
        print("AIFS evaluation complete. Results saved to pickle.")

    if args.run_hres:
        print("running HRES evaluation")
        early_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.end_date < pd.Timestamp("2023-01-01")]
        )
        later_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.start_date > pd.Timestamp("2023-01-01")]
        )

        ewb_hres_early = evaluate.ExtremeWeatherBench(
            early_cases, HRES_FREEZE_EVALUATION_OBJECTS
        )
        ewb_hres_later = evaluate.ExtremeWeatherBench(
            later_cases, BB_HRES_FREEZE_EVALUATION_OBJECTS
        )
        hres_results_early = ewb_hres_early.run(parallel_config=parallel_config)
        bb_hres_results_later = ewb_hres_later.run(parallel_config=parallel_config)
        hres_results = pd.concat([hres_results_early, bb_hres_results_later])
        hres_results.to_pickle(basepath + "saved_data/hres_freeze_results.pkl")
        print("HRES evaluation complete. Results saved to pickle.")

    if args.run_pangu:
        print("running Pangu evaluation")
        ewb_pang = evaluate.ExtremeWeatherBench(
            ewb_cases, PANG_FREEZE_EVALUATION_OBJECTS
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/pang_freeze_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")

    if args.run_fourv2:
        print("running FOURv2 evaluation")
        ewb_fourv2 = evaluate.ExtremeWeatherBench(
            ewb_cases, FOURv2_FREEZE_EVALUATION_OBJECTS
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/fourv2_freeze_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_gc:
        print("running GC evaluation")
        ewb_gc = evaluate.ExtremeWeatherBench(ewb_cases, GC_FREEZE_EVALUATION_OBJECTS)
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/gc_freeze_results.pkl")
        print("GC evaluation complete. Results saved to pickle.")
