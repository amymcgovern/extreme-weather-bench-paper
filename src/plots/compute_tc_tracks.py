# setup all the imports
import argparse
import pickle
import sys
from pathlib import Path

import joblib
from extremeweatherbench import cases, evaluate, inputs, utils
from tqdm.dask import TqdmCallback

from src.data.tc_forecast_setup import TropicalCycloneForecastSetup

sys.path.append(str(Path.home() / "code" / "extreme-weather-bench-paper"))
# make the basepath - change this to your local path
basepath = Path.home() / "code" /"extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

# Define IBTrACS target
ibtracs_target = inputs.IBTrACS()


def process_case(case, forecast, model_name):
    """Process a single case for a given forecast model.

    Args:
        case: The case metadata object.
        forecast: The forecast input data source.
        model_name: The name of the model being processed.

    Returns:
        Tuple of (case_id, model_name, target_data, forecast_data)
    """
    with TqdmCallback(
        desc=f"Running target pipeline for case {case.case_id_number}"
    ):
        target_data = evaluate.run_pipeline(
            case_metadata=case, input_data=ibtracs_target
        )
    with TqdmCallback(
        desc=f"Running {model_name} forecast pipeline for case {case.case_id_number}"
    ):
        forecast_data = evaluate.run_pipeline(
            case_metadata=case, input_data=forecast, _target_dataset=target_data
        )
    return case.case_id_number, model_name, target_data, forecast_data


def run_tc_tracker(ewb_cases, forecast, model_name, parallel_config):
    """Run parallel evaluation for all cases with a given forecast model.

    Args:
        ewb_cases: The case collection to evaluate.
        forecast: The forecast input data source.
        model_name: The name of the model being evaluated.

    Returns:
        List of results from process_case.
    """
    with joblib.parallel_config(**parallel_config):
        results = utils.ParallelTqdm(total_tasks=len(ewb_cases.cases))(
            joblib.delayed(process_case)(case, forecast, model_name)
            for case in ewb_cases.cases
        )
    return results


def update_tc_dict(tc_dict, results, model_name):
    """Update the nested dictionary with results from a model evaluation.

    Args:
        tc_dict: The nested dictionary to update.
        results: List of (case_id, model_name, target_data, forecast_data) tuples.
        model_name: The name of the model.

    Returns:
        Updated tc_dict.
    """
    for case_id, _, target_data, forecast_data in results:
        if case_id not in tc_dict:
            tc_dict[case_id] = {"target_data": None, "forecast_data": {}}
        # Only update target_data if not already set (same for all models)
        if tc_dict[case_id]["target_data"] is None:
            tc_dict[case_id]["target_data"] = target_data
        tc_dict[case_id]["forecast_data"][model_name] = forecast_data

    # Update the tc_tracks.pkl file   
    pickle.dump(tc_dict, open(basepath + "saved_data/tc_tracks.pkl", "wb"))
    return tc_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run tropical cyclone track evaluation against EWB cases."
    )
    parser.add_argument(
        "--run_hres",
        action="store_true",
        default=False,
        help="Run HRES evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_fourv2",
        action="store_true",
        default=False,
        help="Run CIRA FOURv2 evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_gc",
        action="store_true",
        default=False,
        help="Run CIRA Graphcast evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run CIRA Pangu evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_aifs",
        action="store_true",
        default=False,
        help="Run BB AIFS evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_graphcast",
        action="store_true",
        default=False,
        help="Run BB Graphcast evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_pangu",
        action="store_true",
        default=False,
        help="Run BB Pangu evaluation (default: False)",
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=20,
        help="Number of jobs to run in parallel (default: 20)",
    )
    args = parser.parse_args()

    # Load in all of the events in the yaml file and filter for tropical cyclones
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "tropical_cyclone")

    # Initialize the TC forecast setup class
    tc_forecast_setup = TropicalCycloneForecastSetup()

    # Nested dictionary: case_id -> {target_data, forecast_data -> {model: data}}
    tc_dict = {}

    # Cache forecast objects to avoid re-opening data sources
    hres_forecast = None
    bb_hres_forecast = None
    cira_fourv2_forecast = None
    cira_gc_forecast = None
    cira_pangu_forecast = None
    bb_aifs_forecast = None
    bb_graphcast_forecast = None
    bb_pangu_forecast = None

    if args.n_jobs:
        n_jobs = args.n_jobs
    else:
        n_jobs = 20
    parallel_config = {"backend": "loky", "n_jobs": n_jobs}
    if args.run_hres:
        print("Running HRES TC track evaluation")
        if hres_forecast is None:
            hres_forecast = tc_forecast_setup.get_hres_forecast()
        results = run_tc_tracker(ewb_cases, hres_forecast, "HRES", parallel_config)
        # Check if any results are empty and fallback to BB HRES
        empty_cases = [r for r in results if len(r[3]) == 0]
        if empty_cases:
            print("Some cases empty, trying BB HRES for fallback")
            if bb_hres_forecast is None:
                bb_hres_forecast = tc_forecast_setup.get_bb_hres_forecast()
            # Re-run only for empty cases
            empty_case_ids = [r[0] for r in empty_cases]
            empty_ewb_cases = ewb_cases.select_cases(
                "case_id_number", empty_case_ids
            )
            bb_results = run_tc_tracker(
                empty_ewb_cases, bb_hres_forecast, "HRES", parallel_config
            )
            # Merge results
            results = [r for r in results if len(r[3]) > 0] + bb_results
        tc_dict = update_tc_dict(tc_dict, results, "HRES")

    if args.run_cira_fourv2:
        print("Running CIRA FOURv2 TC track evaluation")
        if cira_fourv2_forecast is None:
            cira_fourv2_forecast = tc_forecast_setup.get_cira_tc_forecast(
                "Fourv2", "IFS"
            )
        results = run_tc_tracker(ewb_cases, cira_fourv2_forecast, "CIRA_FOURv2", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "CIRA_FOURv2")

    if args.run_cira_gc:
        print("Running CIRA Graphcast TC track evaluation")
        if cira_gc_forecast is None:
            cira_gc_forecast = tc_forecast_setup.get_cira_tc_forecast(
                "Graphcast", "GFS"
            )
        results = run_tc_tracker(ewb_cases, cira_gc_forecast, "CIRA_Graphcast", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "CIRA_Graphcast")

    if args.run_cira_pangu:
        print("Running CIRA Pangu TC track evaluation")
        if cira_pangu_forecast is None:
            cira_pangu_forecast = tc_forecast_setup.get_cira_tc_forecast("Pangu", "IFS")
        results = run_tc_tracker(ewb_cases, cira_pangu_forecast, "CIRA_Pangu", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "CIRA_Pangu")

    if args.run_bb_aifs:
        print("Running BB AIFS TC track evaluation")
        if bb_aifs_forecast is None:
            bb_aifs_forecast = tc_forecast_setup.get_bb_tc_forecast("AIFS")
        results = run_tc_tracker(ewb_cases, bb_aifs_forecast, "BB_AIFS", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "BB_AIFS")

    if args.run_bb_graphcast:
        print("Running BB Graphcast TC track evaluation")
        if bb_graphcast_forecast is None:
            bb_graphcast_forecast = tc_forecast_setup.get_bb_tc_forecast("Graphcast")
        results = run_tc_tracker(ewb_cases, bb_graphcast_forecast, "BB_Graphcast", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "BB_Graphcast")

    if args.run_bb_pangu:
        print("Running BB Pangu TC track evaluation")
        if bb_pangu_forecast is None:
            bb_pangu_forecast = tc_forecast_setup.get_bb_tc_forecast("Pangu")
        results = run_tc_tracker(ewb_cases, bb_pangu_forecast, "BB_Pangu", parallel_config)
        tc_dict = update_tc_dict(tc_dict, results, "BB_Pangu")

    # Save the results
    print("Saving TC track results")
    pickle.dump(tc_dict, open(basepath + "saved_data/tc_tracks.pkl", "wb"))
