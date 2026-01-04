# setup all the imports
import argparse  # noqa: E402
from pathlib import Path  # noqa: E402

import pandas as pd  # noqa: E402
from extremeweatherbench import (  # noqa: E402
    cases,
    evaluate,
)
from severe_forecast_setup import (
    AIFS_SEVERE_EVALUATION_OBJECTS,
    BB_HRES_SEVERE_EVALUATION_OBJECTS,
    GC_SEVERE_EVALUATION_OBJECTS,
    HRES_SEVERE_EVALUATION_OBJECTS,
    PANG_SEVERE_EVALUATION_OBJECTS,
    FOURv2_SEVERE_EVALUATION_OBJECTS,
)

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

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
