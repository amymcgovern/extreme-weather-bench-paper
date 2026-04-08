# setup all the imports
import argparse
import pickle
from pathlib import Path
import importlib

import extremeweatherbench as ewb
from extremeweatherbench import data

from src.data.severe_forecast_setup import (
    SevereEvaluationSetup,
    SevereForecastSetup,
)

# to plot the targets, we need to run the pipeline for each case and target

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"


def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = ewb.inputs.PPH()
    pph = ewb.evaluate.run_pipeline(ewb_case, pph_target)
    cbss = ewb.evaluate.run_pipeline(ewb_case, forecast_source)

    return cbss, pph


if __name__ == "__main__":
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
        help="Run FOURv2 evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_gc",
        action="store_true",
        default=False,
        help="Run GC evaluation (default: False)",
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
        help="Run AIFS evaluation (default: False)",
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
        "--case_ids",
        nargs="+",
        default=[],
        help="Case IDs to run (default: all)",
    )

    parser.add_argument(
        "--run_marginal",
        action="store_true",
        default=False,
        help="Use the marginal severe cases instead",
    )

    args = parser.parse_args()

    # convert the case ids to integers
    if len(args.case_ids) > 0:
        # split the list by commas and convert to integers
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

    print(args.case_ids)

    # load in all of the events in the yaml file

    if args.run_marginal:
        events_yaml_file = importlib.resources.files(data).joinpath(
                "marginal-severe-convection-cases.yaml"
            )

        ewb_cases = ewb.cases.load_individual_cases_from_yaml(events_yaml_file)
    else:
        ewb_cases = ewb.cases.load_ewb_events_yaml_into_case_list()
        ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]

    # if we are subsetting the cases, do it here
    if args.case_ids is not None:
        ewb_cases = [n for n in ewb_cases if n.case_id_number in args.case_ids]

    # initialize the graphics dictionaries
    gc_graphics = dict()
    pang_graphics = dict()
    fourv2_graphics = dict()
    hres_graphics = dict()
    aifs_graphics = dict()

    severe_forecast_setup = SevereForecastSetup()
    severe_evaluation_setup = SevereEvaluationSetup()

    # this is a hack to handle only opening icechunk once
    hres_severe_forecast = None
    bb_hres_severe_forecast = None
    cira_fourv2_severe_forecast = None
    gc_severe_forecast = None
    pang_severe_forecast = None
    bb_graphcast_severe_forecast = None
    bb_pangu_severe_forecast = None
    bb_aifs_severe_forecast = None

    for my_case in ewb_cases:
        # compute CBSS and PPH for all the AI models and HRES for the case we chose
        my_id = my_case.case_id_number
        print(my_id)
        # my_case = [n for n in ewb_cases if n.case_id_number == my_id][0]

        if args.run_hres:
            print("Computing CBSS and PPH for HRES")
            if hres_severe_forecast is None:
                hres_severe_forecast = severe_forecast_setup.get_hres_severe_convection_forecast()
            if bb_hres_severe_forecast is None:
                bb_hres_severe_forecast = severe_forecast_setup.get_bb_hres_severe_convection_forecast()
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, hres_severe_forecast)
            if len(cbss) == 0:
                print("Computing CBSS and PPH for BB HRES")
                [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_hres_severe_forecast)
            
            hres_graphics[my_id, "cbss"] = cbss
            hres_graphics[my_id, "pph"] = pph

        if args.run_cira_fourv2:
            print("Computing CBSS and PPH for FOURV2")
            if cira_fourv2_severe_forecast is None:
                cira_fourv2_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Fourv2", "IFS")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, cira_fourv2_severe_forecast)
            fourv2_graphics[my_id, "cbss"] = cbss
            fourv2_graphics[my_id, "pph"] = pph

        if args.run_cira_gc:
            print("Computing CBSS and PPH for GC")
            if gc_severe_forecast is None:  
                gc_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Graphcast", "IFS")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, gc_severe_forecast)
            gc_graphics[my_id, "cbss"] = cbss
            gc_graphics[my_id, "pph"] = pph

        if args.run_cira_pangu:
            print("Computing CBSS and PPH for PANG")
            if pang_severe_forecast is None:
                pang_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Pangu", "IFS")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, pang_severe_forecast)
            pang_graphics[my_id, "cbss"] = cbss
            pang_graphics[my_id, "pph"] = pph

        if args.run_bb_graphcast:
            print("Computing CBSS and PPH for Graphcast")
            if bb_graphcast_severe_forecast is None:
                bb_graphcast_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("graphcast")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_graphcast_severe_forecast)
            gc_graphics[my_id, "cbss"] = cbss
            gc_graphics[my_id, "pph"] = pph

        if args.run_bb_pangu:
            print("Computing CBSS and PPH for Pangu")
            if bb_pangu_severe_forecast is None:
                bb_pangu_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("panguweather")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_pangu_severe_forecast)
            pang_graphics[my_id, "cbss"] = cbss
            pang_graphics[my_id, "pph"] = pph

        if args.run_bb_aifs:
            print("Computing CBSS and PPH for AIFS")
            if bb_aifs_severe_forecast is None:
                bb_aifs_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("aifs-single")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_aifs_severe_forecast)
            aifs_graphics[my_id, "cbss"] = cbss
            aifs_graphics[my_id, "pph"] = pph

    print("Saving the graphics objects")
    if args.case_ids is not None:
        suffix = "_paper"
    else:
        suffix = ""

    if args.run_marginal:
        suffix = suffix +"_marginal"
    else:
        suffix = suffix + ""

    if args.run_hres:
        pickle.dump(
            hres_graphics, open(basepath + "saved_data/hres_graphics_severe" + suffix + ".pkl", "wb")
        )
    if args.run_cira_fourv2:
        pickle.dump(
            fourv2_graphics, open(basepath + "saved_data/fourv2_cira_severe_graphics" + suffix + ".pkl", "wb")
        )
    if args.run_cira_gc:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_cira_severe_graphics" + suffix + ".pkl", "wb")
        )
    if args.run_cira_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_cira_severe_graphics" + suffix + ".pkl", "wb")
        )  
    if args.run_bb_graphcast:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_bb_severe_graphics" + suffix + ".pkl", "wb")
        )
    if args.run_bb_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_bb_severe_graphics" + suffix + ".pkl", "wb")
        )
    if args.run_bb_aifs:
        pickle.dump(
            aifs_graphics, open(basepath + "saved_data/aifs_bb_severe_graphics" + suffix + ".pkl", "wb")
        )