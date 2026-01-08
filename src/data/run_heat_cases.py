# setup all the imports
import argparse
from pathlib import Path

import pandas as pd  # noqa: E402
from extremeweatherbench import cases, evaluate

from src.data.heat_freeze_forecast_setup import (
    HeatFreezeEvaluationSetup,
    HeatFreezeForecastSetup,
)

if __name__ == "__main__":
    # make the basepath for saving the results - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

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

    heat_freeze_forecast_setup = HeatFreezeForecastSetup()
    heat_freeze_evaluation_setup = HeatFreezeEvaluationSetup()

    if args.run_hres:
        print("running HRES evaluation")
        hres_heat_forecast = heat_freeze_forecast_setup.get_hres_heat_freeze_forecast()
        hres_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [hres_heat_forecast]
            )
        )

        bb_hres_heat_forecast = (
            heat_freeze_forecast_setup.get_bb_hres_heat_freeze_forecast()
        )
        bb_hres_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [bb_hres_heat_forecast]
            )
        )

        ewb_hres = evaluate.ExtremeWeatherBench(ewb_cases, hres_heat_evaluation_objects)
        # split the cases into early and later for HRES (just for ease of evaluation)
        early_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.end_date < pd.Timestamp("2023-01-01")]
        )
        later_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.start_date > pd.Timestamp("2023-01-01")]
        )

        ewb_hres_early = evaluate.ExtremeWeatherBench(
            early_cases, hres_heat_evaluation_objects
        )
        ewb_hres_later = evaluate.ExtremeWeatherBench(
            later_cases, bb_hres_heat_evaluation_objects
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
        fourv2_heat_ifs_forecast = (
            heat_freeze_forecast_setup.get_cira_heat_freeze_forecast("Fourv2", "IFS")
        )
        fourv2_heat_gfs_forecast = (
            heat_freeze_forecast_setup.get_cira_heat_freeze_forecast("Fourv2", "GFS")
        )
        fourv2_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [fourv2_heat_ifs_forecast, fourv2_heat_gfs_forecast]
            )
        )
        ewb_fourv2 = evaluate.ExtremeWeatherBench(
            ewb_cases, fourv2_heat_evaluation_objects
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/cira_fourv2_heat_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_cira_gc:
        print("running GC evaluation")
        gc_heat_ifs_forecast = heat_freeze_forecast_setup.get_cira_heat_freeze_forecast(
            "Graphcast", "IFS"
        )
        gc_heat_gfs_forecast = heat_freeze_forecast_setup.get_cira_heat_freeze_forecast(
            "Graphcast", "GFS"
        )
        gc_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [gc_heat_ifs_forecast, gc_heat_gfs_forecast]
            )
        )
        ewb_gc = evaluate.ExtremeWeatherBench(ewb_cases, gc_heat_evaluation_objects)
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/cira_gc_heat_results.pkl")
        print("GC evaluation complete. Results saved to pickle.")

    if args.run_cira_pangu:
        print("running Pangu evaluation")
        pang_heat_ifs_forecast = (
            heat_freeze_forecast_setup.get_cira_heat_freeze_forecast("Pangu", "IFS")
        )
        pang_heat_gfs_forecast = (
            heat_freeze_forecast_setup.get_cira_heat_freeze_forecast("Pangu", "GFS")
        )
        pang_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [pang_heat_ifs_forecast, pang_heat_gfs_forecast]
            )
        )
        ewb_pang = evaluate.ExtremeWeatherBench(ewb_cases, pang_heat_evaluation_objects)
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/cira_pang_heat_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")

    if args.run_bb_aifs:
        print("running BB AIFS evaluation")
        bb_aifs_heat_forecast = heat_freeze_forecast_setup.get_bb_heat_freeze_forecast(
            "AIFS"
        )
        bb_aifs_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [bb_aifs_heat_forecast]
            )
        )
        ewb_bb_aifs = evaluate.ExtremeWeatherBench(
            ewb_cases, bb_aifs_heat_evaluation_objects
        )
        bb_aifs_results = ewb_bb_aifs.run(parallel_config=parallel_config)
        bb_aifs_results.to_pickle(basepath + "saved_data/bb_aifs_heat_results.pkl")
        print("BB AIFS evaluation complete. Results saved to pickle.")

    if args.run_bb_graphcast:
        bb_graphcast_heat_forecast = (
            heat_freeze_forecast_setup.get_bb_heat_freeze_forecast("Graphcast")
        )
        bb_graphcast_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [bb_graphcast_heat_forecast]
            )
        )
        print("running BB Graphcast evaluation")
        ewb_bb_graphcast = evaluate.ExtremeWeatherBench(
            ewb_cases, bb_graphcast_heat_evaluation_objects
        )
        bb_graphcast_results = ewb_bb_graphcast.run(parallel_config=parallel_config)
        bb_graphcast_results.to_pickle(
            basepath + "saved_data/bb_graphcast_heat_results.pkl"
        )
        print("BB Graphcast evaluation complete. Results saved to pickle.")

    if args.run_bb_pangu:
        print("running BB Pangu evaluation")
        bb_pangu_heat_forecast = heat_freeze_forecast_setup.get_bb_heat_freeze_forecast(
            "Pangu"
        )
        bb_pangu_heat_evaluation_objects = (
            heat_freeze_evaluation_setup.get_heat_evaluation_objects(
                [bb_pangu_heat_forecast]
            )
        )
        ewb_bb_pangu = evaluate.ExtremeWeatherBench(
            ewb_cases, bb_pangu_heat_evaluation_objects
        )
        bb_pangu_results = ewb_bb_pangu.run(parallel_config=parallel_config)
        bb_pangu_results.to_pickle(basepath + "saved_data/bb_pangu_heat_results.pkl")
        print("BB Pangu evaluation complete. Results saved to pickle.")
