# setup all the imports
import argparse
from pathlib import Path

from extremeweatherbench import cases, evaluate
from heat_freeze_forecast_setup import (
    BB_HEAT_AIFS_EVALUATION_OBJECTS,
    BB_HEAT_GRAPHCAST_EVALUATION_OBJECTS,
    BB_HEAT_PANGU_EVALUATION_OBJECTS,
    BB_HRES_HEAT_EVALUATION_OBJECTS,
    CIRA_GC_HEAT_EVALUATION_OBJECTS,
    CIRA_PANG_HEAT_EVALUATION_OBJECTS,
    HRES_HEAT_EVALUATION_OBJECTS,
    CIRA_FOURv2_HEAT_EVALUATION_OBJECTS,
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
        help="Run Pangu evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_fourv2",
        action="store_true",
        default=False,
        help="Run FOURv2 evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_gc",
        action="store_true",
        default=False,
        help="Run GC evaluation (default: False)",
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

    # load in the events
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection().select_cases(
        "event_type", "heat_wave"
    )

    parallel_config = {"backend": "loky", "n_jobs": 32}

    if args.run_hres:
        print("running HRES evaluation")
        ewb_hres = evaluate.ExtremeWeatherBench(ewb_cases, HRES_HEAT_EVALUATION_OBJECTS)
        # split the cases into early and later for HRES (just for ease of evaluation)
        early_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.end_date < pd.Timestamp("2023-01-01")]
        )
        later_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.start_date > pd.Timestamp("2023-01-01")]
        )

        ewb_hres_early = evaluate.ExtremeWeatherBench(
            early_cases, HRES_HEAT_EVALUATION_OBJECTS
        )
        ewb_hres_later = evaluate.ExtremeWeatherBench(
            later_cases, BB_HRES_HEAT_EVALUATION_OBJECTS
        )

        hres_results_early = ewb_hres_early.run(
            parallel_config=parallel_config, preserve_dims=["lead_time", "init_time"]
        )
        bb_hres_results_later = ewb_hres_later.run(
            parallel_config=parallel_config, preserve_dims=["lead_time", "init_time"]
        )
        hres_results = pd.concat([hres_results_early, bb_hres_results_later])
        hres_results.to_pickle(basepath + "saved_data/hres_heat_results.pkl")
        print("HRES evaluation complete. Results saved to pickle.")

    if args.run_cira_fourv2:
        print("running FOURv2 evaluation")
        ewb_fourv2 = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_FOURv2_HEAT_EVALUATION_OBJECTS
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/fourv2_heat_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_cira_gc:
        print("running GC evaluation")
        ewb_gc = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_GC_HEAT_EVALUATION_OBJECTS
        )
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/gc_heat_results.pkl")
        print("GC evaluation complete. Results saved to pickle.")

    if args.run_cira_pangu:
        print("running Pangu evaluation")
        ewb_pang = evaluate.ExtremeWeatherBench(
            ewb_cases, CIRA_PANG_HEAT_EVALUATION_OBJECTS
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/pang_heat_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")

    if args.run_bb_aifs:
        print("running BB AIFS evaluation")
        ewb_bb_aifs = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_HEAT_AIFS_EVALUATION_OBJECTS
        )
        bb_aifs_results = ewb_bb_aifs.run(parallel_config=parallel_config)
        bb_aifs_results.to_pickle(basepath + "saved_data/bb_aifs_heat_results.pkl")
        print("BB AIFS evaluation complete. Results saved to pickle.")

    if args.run_bb_graphcast:
        print("running BB Graphcast evaluation")
        ewb_bb_graphcast = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_HEAT_GRAPHCAST_EVALUATION_OBJECTS
        )
        bb_graphcast_results = ewb_bb_graphcast.run(parallel_config=parallel_config)
        bb_graphcast_results.to_pickle(
            basepath + "saved_data/bb_graphcast_heat_results.pkl"
        )
        print("BB Graphcast evaluation complete. Results saved to pickle.")

    if args.run_bb_pangu:
        print("running BB Pangu evaluation")
        ewb_bb_pangu = evaluate.ExtremeWeatherBench(
            ewb_cases, BB_HEAT_PANGU_EVALUATION_OBJECTS
        )
        bb_pangu_results = ewb_bb_pangu.run(parallel_config=parallel_config)
        bb_pangu_results.to_pickle(basepath + "saved_data/bb_pangu_heat_results.pkl")
        print("BB Pangu evaluation complete. Results saved to pickle.")
