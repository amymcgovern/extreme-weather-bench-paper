# setup all the imports
import argparse  # noqa: E402
from pathlib import Path  # noqa: E402

import pandas as pd  # noqa: E402
import extremeweatherbench as ewb

from src.data.severe_forecast_setup import (
    SevereEvaluationSetup,
    SevereForecastSetup,
)

if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    parser = argparse.ArgumentParser(
        description="Run severe evaluation against ExtremeWeatherBench cases."
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
        "--run_cira_graphcast",
        action="store_true",
        default=False,
        help="Run CIRA Graphcast evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run PANG evaluation (default: False)",
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
    ewb_cases = ewb.cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]

    parallel_config = {"backend": "loky", "n_jobs": 12}

    severe_forecast_setup = SevereForecastSetup()
    severe_evaluation_setup = SevereEvaluationSetup()

    if args.run_hres:
        print("running HRES evaluation")

        hres_severe_forecast = (
            severe_forecast_setup.get_hres_severe_convection_forecast()
        )
        hres_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [hres_severe_forecast]
            )
        )
        bb_hres_severe_forecast = (
            severe_forecast_setup.get_bb_hres_severe_convection_forecast()
        )
        bb_hres_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [bb_hres_severe_forecast]
            )
        )
        ewb_hres = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, hres_severe_evaluation_objects
        )
        ewb_hres_bb = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, bb_hres_severe_evaluation_objects
        )

        hres_results1 = ewb_hres.run(parallel_config=parallel_config)
        print("running HRES part 2")
        hres_results2 = ewb_hres_bb.run(parallel_config=parallel_config)
        print("concatenating the results")
        hres_results = pd.concat([hres_results1, hres_results2])
        hres_results.to_pickle(basepath + "saved_data/hres_severe_results.pkl")
        print("HRES evaluation complete. Results saved to pickle.")

    if args.run_cira_fourv2:
        print("running FOURv2 evaluation")

        cira_fourv2_gfs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast("Fourv2", "GFS")
        )
        cira_fourv2_ifs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast("Fourv2", "IFS")
        )
        cira_fourv2_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [cira_fourv2_ifs_severe_forecast, cira_fourv2_gfs_severe_forecast]
            )
        )
        ewb_fourv2 = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, cira_fourv2_severe_evaluation_objects
        )
        fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        fourv2_results.to_pickle(basepath + "saved_data/cira_fourv2_severe_results.pkl")
        print("FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_cira_graphcast:
        print("running Graphcast evaluation")

        cira_gc_gfs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast(
                "Graphcast", "GFS"
            )
        )
        cira_gc_ifs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast(
                "Graphcast", "IFS"
            )
        )
        cira_gc_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [cira_gc_ifs_severe_forecast, cira_gc_gfs_severe_forecast]
            )
        )
        ewb_gc = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, cira_gc_severe_evaluation_objects
        )
        gc_results = ewb_gc.run(parallel_config=parallel_config)
        gc_results.to_pickle(basepath + "saved_data/cira_graphcast_severe_results.pkl")
        print("Graphcast evaluation complete. Results saved to pickle.")

    if args.run_cira_pangu:
        print("running PANGU evaluation")

        cira_pangu_gfs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast("Pangu", "GFS")
        )
        cira_pangu_ifs_severe_forecast = (
            severe_forecast_setup.get_cira_severe_convection_forecast("Pangu", "IFS")
        )
        cira_pangu_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [cira_pangu_ifs_severe_forecast, cira_pangu_gfs_severe_forecast]
            )
        )
        ewb_pang = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, cira_pangu_severe_evaluation_objects
        )
        pang_results = ewb_pang.run(parallel_config=parallel_config)
        pang_results.to_pickle(basepath + "saved_data/cira_pangu_severe_results.pkl")
        print("PANG evaluation complete. Results saved to pickle.")

    if args.run_bb_aifs:
        print("running AIFS evaluation")

        bb_aifs_severe_forecast = (
            severe_forecast_setup.get_bb_severe_convection_forecast("aifs-single")
        )
        bb_aifs_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [bb_aifs_severe_forecast]
            )
        )
        ewb_aifs = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, bb_aifs_severe_evaluation_objects
        )
        aifs_results = ewb_aifs.run(parallel_config=parallel_config)
        aifs_results.to_pickle(basepath + "saved_data/bb_aifs_severe_results.pkl")
        print("AIFS evaluation complete. Results saved to pickle.")

    if args.run_bb_graphcast:
        print("running Graphcast evaluation")

        bb_graphcast_severe_forecast = (
            severe_forecast_setup.get_bb_severe_convection_forecast("graphcast")
        )
        bb_graphcast_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [bb_graphcast_severe_forecast]
            )
        )
        ewb_graphcast = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, bb_graphcast_severe_evaluation_objects
        )
        graphcast_results = ewb_graphcast.run(parallel_config=parallel_config)
        graphcast_results.to_pickle(
            basepath + "saved_data/bb_graphcast_severe_results.pkl"
        )
        print("Graphcast evaluation complete. Results saved to pickle.")

    if args.run_bb_pangu:
        print("running Pangu evaluation")

        bb_pangu_severe_forecast = (
            severe_forecast_setup.get_bb_severe_convection_forecast("panguweather")
        )
        bb_pangu_severe_evaluation_objects = (
            severe_evaluation_setup.get_severe_evaluation_objects(
                [bb_pangu_severe_forecast]
            )
        )
        ewb_pangu = ewb.evaluate.ExtremeWeatherBench(
            ewb_cases, bb_pangu_severe_evaluation_objects
        )
        pangu_results = ewb_pangu.run(parallel_config=parallel_config)
        pangu_results.to_pickle(basepath + "saved_data/bb_pangu_severe_results.pkl")
        print("Pangu evaluation complete. Results saved to pickle.")
