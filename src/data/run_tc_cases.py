# setup all the imports
# setup all the imports
import argparse  # noqa: E402
from pathlib import Path  # noqa: E402

import pandas as pd  # noqa: E402
from extremeweatherbench import (  # noqa: E402
    cases,
    evaluate,
)

from src.data.tc_forecast_setup import (
    TropicalCycloneEvaluationSetup,
    TropicalCycloneForecastSetup,
)


if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    parser = argparse.ArgumentParser(
        description="Run tropical cyclone evaluation against ExtremeWeatherBench cases."
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
        help="Run CIRA GC evaluation (default: False)",
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
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "tropical_cyclone")

    parallel_config = {"backend": "loky", "n_jobs": 24}

    tropical_cyclone_forecast_setup = TropicalCycloneForecastSetup()
    tropical_cyclone_evaluation_setup = TropicalCycloneEvaluationSetup()

    if args.run_hres:
        print("running HRES evaluation")

        hres_tc_forecast = tropical_cyclone_forecast_setup.get_hres_forecast()
        hres_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([hres_tc_forecast])
        print(hres_tc_evaluation_objects)

        bb_hres_tc_forecast = tropical_cyclone_forecast_setup.get_bb_hres_forecast()
        bb_hres_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([bb_hres_tc_forecast])
        print(bb_hres_tc_evaluation_objects)

        early_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.end_date < pd.Timestamp("2023-01-01")]
        )
        later_cases = cases.IndividualCaseCollection(
            [i for i in ewb_cases.cases if i.start_date > pd.Timestamp("2023-01-01")]
        )

        ewb_hres = evaluate.ExtremeWeatherBench(early_cases, hres_tc_evaluation_objects)
        ewb_bb_hres = evaluate.ExtremeWeatherBench(later_cases, bb_hres_tc_evaluation_objects)

        print("running HRES")
        hres_results = ewb_hres.run(parallel_config=parallel_config)
        print("running BB HRES")
        bb_hres_results = ewb_bb_hres.run(parallel_config=parallel_config)
        
        print("concatenating the results")
        hres_combined_results = pd.concat([hres_results, bb_hres_results])
        hres_combined_results.to_pickle(basepath + "saved_data/hres_tc_results.pkl")
        print("HRES evaluation complete. Results saved to pickle.")

    if args.run_cira_fourv2:
        print("running CIRA FOURv2 evaluation")

        cira_fourv2_tc_forecast = tropical_cyclone_forecast_setup.get_cira_tc_forecast("FOURv2", "GFS")
        cira_fourv2_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([cira_fourv2_tc_forecast])

        ewb_fourv2 = evaluate.ExtremeWeatherBench(ewb_cases, cira_fourv2_tc_evaluation_objects)

        print("running CIRA FOURv2")
        cira_fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
        cira_fourv2_results.to_pickle(basepath + "saved_data/cira_fourv2_tc_results.pkl")
        print("CIRA FOURv2 evaluation complete. Results saved to pickle.")

    if args.run_cira_gc:
        print("running CIRA GC evaluation")

        cira_gc_tc_forecast = tropical_cyclone_forecast_setup.get_cira_tc_forecast("GC", "GFS")
        cira_gc_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([cira_gc_tc_forecast])

        ewb_cira_gc = evaluate.ExtremeWeatherBench(ewb_cases, cira_gc_tc_evaluation_objects)

        print("running CIRA GC")
        cira_gc_results = ewb_cira_gc.run(parallel_config=parallel_config)
        cira_gc_results.to_pickle(basepath + "saved_data/cira_gc_tc_results.pkl")
        print("CIRA GC evaluation complete. Results saved to pickle.")

    if args.run_cira_pangu:
        print("running CIRA PANGU evaluation")

        cira_pangu_tc_forecast = tropical_cyclone_forecast_setup.get_cira_tc_forecast("Pangu", "GFS")
        cira_pangu_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([cira_pangu_tc_forecast])

        ewb_cira_pangu = evaluate.ExtremeWeatherBench(ewb_cases, cira_pangu_tc_evaluation_objects)

        print("running CIRA PANGU")
        cira_pangu_results = ewb_cira_pangu.run(parallel_config=parallel_config)
        cira_pangu_results.to_pickle(basepath + "saved_data/cira_pangu_tc_results.pkl")
        print("CARA PANGU evaluation complete. Results saved to pickle.")

    if args.run_bb_aifs:
        print("running BB AIFS evaluation")

        bb_aifs_tc_forecast = tropical_cyclone_forecast_setup.get_bb_tc_forecast("AIFS")
        bb_aifs_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([bb_aifs_tc_forecast])

        ewb_bb_aifs = evaluate.ExtremeWeatherBench(ewb_cases, bb_aifs_tc_evaluation_objects)

        print("running BB AIFS")
        bb_aifs_results = ewb_bb_aifs.run(parallel_config=parallel_config)
        bb_aifs_results.to_pickle(basepath + "saved_data/bb_aifs_tc_results.pkl")
        print("BB AIFS evaluation complete. Results saved to pickle.")

    if args.run_bb_graphcast:
        print("running BB Graphcast evaluation")

        bb_graphcast_tc_forecast = tropical_cyclone_forecast_setup.get_bb_tc_forecast("Graphcast")
        bb_graphcast_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([bb_graphcast_tc_forecast])

        ewb_bb_graphcast = evaluate.ExtremeWeatherBench(ewb_cases, bb_graphcast_tc_evaluation_objects)

        print("running BB Graphcast")
        bb_graphcast_results = ewb_bb_graphcast.run(parallel_config=parallel_config)
        bb_graphcast_results.to_pickle(basepath + "saved_data/bb_graphcast_tc_results.pkl")
        print("BB Graphcast evaluation complete. Results saved to pickle.")

    if args.run_bb_pangu:
        print("running BB PANGU evaluation")

        bb_pangu_tc_forecast = tropical_cyclone_forecast_setup.get_bb_tc_forecast("Pangu")
        bb_pangu_tc_evaluation_objects = tropical_cyclone_evaluation_setup.get_tc_evaluation_objects([bb_pangu_tc_forecast])

        ewb_bb_pangu = evaluate.ExtremeWeatherBench(ewb_cases, bb_pangu_tc_evaluation_objects)

        print("running BB PANGU")
        bb_pangu_results = ewb_bb_pangu.run(parallel_config=parallel_config)
        bb_pangu_results.to_pickle(basepath + "saved_data/bb_pangu_tc_results.pkl")
        print("BB PANGU evaluation complete. Results saved to pickle.")