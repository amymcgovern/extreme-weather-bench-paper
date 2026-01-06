# setup all the imports
import argparse  # noqa: E402
from pathlib import Path

from extremeweatherbench import cases, evaluate
from heat_freeze_forecast_setup import (
    BB_FREEZE_AIFS_EVALUATION_OBJECTS,
    BB_FREEZE_GRAPHCAST_EVALUATION_OBJECTS,
    BB_FREEZE_PANGU_EVALUATION_OBJECTS,
    BB_HRES_FREEZE_EVALUATION_OBJECTS,
    CIRA_GC_FREEZE_EVALUATION_OBJECTS,
    CIRA_PANGU_FREEZE_EVALUATION_OBJECTS,
    HRES_FREEZE_EVALUATION_OBJECTS,
    CIRA_FOURv2_FREEZE_EVALUATION_OBJECTS,
)

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

import pandas as pd  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run heat wave evaluation against ExtremeWeatherBench cases."
    )

    # run whichever cases the user specifies

    # Icechunk options
    parser.add_argument(
        "--run_hres",
        action="store_true",
        default=False,
        help="Run HRES evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run CIRA Pangu evaluation (default: False)",
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
        help="Run CIRA GC evaluation (default: False)",
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

    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "freeze")

    # load in the results for all freeze cases in parallel
    # this will take awhile to run if you do them all in one code box
    # if you have already saved them (from running this once), then skip this box
    parallel_config = {"backend": "loky", "n_jobs": 32}

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

    if args.run_cira_pangu:
        print("running Pangu evaluation")
        ewb_pang = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_PANGU_FREEZE_EVALUATION_OBJECTS
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/cira_pang_freeze_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")

    if args.run_cira_fourv2:
        print("running FOURv2 evaluation")
        ewb_fourv2 = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_FOURv2_FREEZE_EVALUATION_OBJECTS
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/cira_fourv2_freeze_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_cira_gc:
        print("running GC evaluation")
        ewb_gc = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_GC_FREEZE_EVALUATION_OBJECTS
        )
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/cira_gc_freeze_results.pkl")
        print("GC evaluation complete. Results saved to pickle.")

    if args.run_bb_aifs:
        print("running AIFS evaluation")
        ewb_aifs = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_FREEZE_AIFS_EVALUATION_OBJECTS
        )
        aifs_results = ewb_aifs.run(parallel_config=parallel_config)
        aifs_results.to_pickle(basepath + "saved_data/bb_aifs_freeze_results.pkl")
        print("AIFS evaluation complete. Results saved to pickle.")

    if args.run_bb_graphcast:
        print("running Graphcast evaluation")
        ewb_graphcast = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_FREEZE_GRAPHCAST_EVALUATION_OBJECTS
        )
        graphcast_results = ewb_graphcast.run(parallel_config=parallel_config)
        graphcast_results.to_pickle(
            basepath + "saved_data/bb_graphcast_freeze_results.pkl"
        )
        print("Graphcast evaluation complete. Results saved to pickle.")

    if args.run_bb_pangu:
        print("running Pangu evaluation")
        ewb_pang = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_FREEZE_PANGU_EVALUATION_OBJECTS
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/bb_pang_freeze_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")
