# setup all the imports
import argparse
import pickle
from pathlib import Path

from extremeweatherbench import (
    cases,
    evaluate,
    inputs,
)

from src.data.severe_forecast_setup import (
    SevereEvaluationSetup,
    SevereForecastSetup,
)

# to plot the targets, we need to run the pipeline for each case and target

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"


def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = inputs.PPH()
    pph = evaluate.run_pipeline(ewb_case, pph_target)
    cbss = evaluate.run_pipeline(ewb_case, forecast_source)

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

    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "severe_convection")

    my_ids = [36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 
        269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 
        283, 284, 285, 286, 287, 288, 316, 317, 318, 319, 320, 321, 322, 323, 
        324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337]

    # hres_graphics = dict()
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

    for my_id in my_ids:
        # compute CBSS and PPH for all the AI models and HRES for the case we chose
        print(my_id)
        my_case = ewb_cases.select_cases("case_id_number", my_id).cases[0]

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
                cira_fourv2_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("FOURv2", "IFS")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, cira_fourv2_severe_forecast)
            fourv2_graphics[my_id, "cbss"] = cbss
            fourv2_graphics[my_id, "pph"] = pph

        if args.run_cira_gc:
            print("Computing CBSS and PPH for GC")
            if gc_severe_forecast is None:  
                gc_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Graphcast", "GFS")
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
                bb_graphcast_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("Graphcast")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_graphcast_severe_forecast)
            gc_graphics[my_id, "cbss"] = cbss
            gc_graphics[my_id, "pph"] = pph

        if args.run_bb_pangu:
            print("Computing CBSS and PPH for Pangu")
            if bb_pangu_severe_forecast is None:
                bb_pangu_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("Pangu")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_pangu_severe_forecast)
            pang_graphics[my_id, "cbss"] = cbss
            pang_graphics[my_id, "pph"] = pph

        if args.run_bb_aifs:
            print("Computing CBSS and PPH for AIFS")
            if bb_aifs_severe_forecast is None:
                bb_aifs_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("AIFS")
            [cbss, pph] = get_cbss_and_pph_outputs(my_case, bb_aifs_severe_forecast)
            aifs_graphics[my_id, "cbss"] = cbss
            aifs_graphics[my_id, "pph"] = pph

    print("Saving the graphics objects")
    if args.run_hres:
        pickle.dump(
            hres_graphics, open(basepath + "saved_data/hres_graphics.pkl", "wb")
        )
    if args.run_cira_fourv2:
        pickle.dump(
            fourv2_graphics, open(basepath + "saved_data/fourv2_cira_graphics.pkl", "wb")
        )
    if args.run_cira_gc:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_cira_graphics.pkl", "wb")
        )
    if args.run_cira_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_cira_graphics.pkl", "wb")
        )  
    if args.run_bb_graphcast:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_bb_graphics.pkl", "wb")
        )
    if args.run_bb_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_bb_graphics.pkl", "wb")
        )
    if args.run_bb_aifs:
        pickle.dump(
            aifs_graphics, open(basepath + "saved_data/aifs_bb_graphics.pkl", "wb")
        )