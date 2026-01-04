# setup all the imports
import argparse  # noqa: E402
from pathlib import Path  # noqa: E402

import pandas as pd  # noqa: E402
from aifs_util import (
    AIFS_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    DEFAULT_ICECHUNK_PREFIX,
    DEFAULT_SOURCE_CREDENTIALS_PREFIX,
    InMemoryForecast,
    open_icechunk_dataset,
)
from arraylake_utils import (  # noqa: E402
    ArraylakeForecast,
    BB_metadata_variable_mapping,
)
from extremeweatherbench import (  # noqa: E402
    cases,
    defaults,
    derived,
    evaluate,
    inputs,
    metrics,
)

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

# setup the templates to load in the data
cira_severe_convection_forecast_FOURV2_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA FOURv2 GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

cira_severe_convection_forecast_GC_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA GC GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

cira_severe_convection_forecast_PANG_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA PANG GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

hres_severe_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
)

bb_hres_forecast = ArraylakeForecast(
    source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
    variables=[derived.CravenBrooksSignificantSevere()],
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
    variable_mapping=BB_metadata_variable_mapping,
)

ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=DEFAULT_ICECHUNK_PREFIX,
    variable_mapping=AIFS_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=DEFAULT_SOURCE_CREDENTIALS_PREFIX,
)

aifs_forecast = InMemoryForecast(
    ds=ds,
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=AIFS_VARIABLE_MAPPING,
    name="AIFS",
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

FOURv2_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_FOURV2_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_FOURV2_GFS,
    ),
]

GC_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_GC_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_GC_GFS,
    ),
]

PANG_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_PANG_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_PANG_GFS,
    ),
]

HRES_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=hres_severe_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=hres_severe_forecast,
    ),
]

BB_HRES_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=bb_hres_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=bb_hres_forecast,
    ),
]

AIFS_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=aifs_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=aifs_forecast,
    ),
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run severe evaluation against ExtremeWeatherBench cases."
    )
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
    parser.add_argument(
        "--run_pangu",
        action="store_true",
        default=False,
        help="Run PANG evaluation (default: False)",
    )
    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "severe_convection")

    parallel_config = {"backend": "loky", "n_jobs": 24}

    if args.run_aifs:
        print("running AIFS evaluation")
        ewb_aifs = evaluate.ExtremeWeatherBench(
            ewb_cases, AIFS_SEVERE_EVALUATION_OBJECTS
        )
        aifs_results = ewb_aifs.run(parallel_config=parallel_config)
        aifs_results.to_pickle(basepath + "saved_data/aifs_severe_results.pkl")
        print("AIFS evaluation complete. Results saved to pickle.")

    if args.run_hres:
        print("running HRES evaluation")
        ewb_hres = evaluate.ExtremeWeatherBench(
            ewb_cases, HRES_SEVERE_EVALUATION_OBJECTS
        )
        ewb_hres_bb = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_HRES_SEVERE_EVALUATION_OBJECTS
        )
        hres_results1 = ewb_hres.run(parallel_config=parallel_config)
        print("running HRES part 2")
        hres_results2 = ewb_hres_bb.run(parallel_config=parallel_config)
        print("concatenating the results")
        hres_results = pd.concat([hres_results1, hres_results2])
        hres_results.to_pickle(basepath + "saved_data/hres_severe_results.pkl")
        print("HRES evaluation complete. Results saved to pickle.")

    if args.run_fourv2:
        print("running FOURv2 evaluation")
        ewb_fourv2 = evaluate.ExtremeWeatherBench(
            ewb_cases, FOURv2_SEVERE_EVALUATION_OBJECTS
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/fourv2_severe_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_gc:
        print("running GC evaluation")
        ewb_gc = evaluate.ExtremeWeatherBench(ewb_cases, GC_SEVERE_EVALUATION_OBJECTS)
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/gc_severe_results.pkl")
        print("GC evaluation complete. Results saved to pickle.")

    if args.run_pangu:
        print("running PANGU evaluation")
        ewb_pang = evaluate.ExtremeWeatherBench(
            ewb_cases, PANG_SEVERE_EVALUATION_OBJECTS
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/pang_severe_results.pkl")
        print("PANG evaluation complete. Results saved to pickle.")
