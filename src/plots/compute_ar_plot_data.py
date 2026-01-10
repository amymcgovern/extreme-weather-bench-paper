# setup all the imports
import argparse
import pickle
from pathlib import Path

from extremeweatherbench import (
    cases,
    evaluate,
    inputs,
)

from src.data.ar_forecast_setup import (
    AtmosphericRiverEvaluationSetup,
    AtmosphericRiverForecastSetup,
)

# to plot the targets, we need to run the pipeline for each case and target
def get_ivt(ewb_case, forecast_source):
    ivt = evaluate.run_pipeline(ewb_case, forecast_source)

    return ivt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run atmospheric river evaluation against ExtremeWeatherBench cases."
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
        "--run_era5",
        action="store_true",
        default=False,
        help="Run ERA5 evaluation (default: False)",
    )

    args = parser.parse_args()

    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"


    atmospheric_river_forecast_setup = AtmosphericRiverForecastSetup()
    atmospheric_river_evaluation_setup = AtmosphericRiverEvaluationSetup()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "atmospheric_river")

    hres_graphics = dict()
    gc_graphics = dict()
    pang_graphics = dict()
    fourv2_graphics = dict()
    hres_graphics = dict()
    aifs_graphics = dict()
    era5_graphics = dict()

    atmospheric_river_forecast_setup = AtmosphericRiverForecastSetup()
    atmospheric_river_evaluation_setup = AtmosphericRiverEvaluationSetup()

    # this is a hack to handle only opening icechunk once
    hres_ar_forecast = None
    bb_hres_ar_forecast = None
    cira_fourv2_ar_forecast = None
    gc_ar_forecast = None
    pang_ar_forecast = None
    bb_graphcast_ar_forecast = None
    bb_pangu_ar_forecast = None
    bb_aifs_ar_forecast = None
    era5 = None

    #ewb_cases = ewb_cases.select_cases("case_id_number", 95)
    for my_case in ewb_cases.cases:
        # compute IVT for all the AI models and HRES for the case we chose
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        # my_case = ewb_cases.select_cases("case_id_number", my_id).cases[0]

        if args.run_hres:
            print("Computing IVT for HRES")
            if hres_ar_forecast is None:
                hres_ar_forecast = atmospheric_river_forecast_setup.get_hres_forecast(include_ivt=True)
            if bb_hres_ar_forecast is None:
                bb_hres_ar_forecast = atmospheric_river_forecast_setup.get_bb_hres_forecast(include_ivt=True)
            ivt = get_ivt(my_case, hres_ar_forecast)
            if len(ivt) == 0:
                print("Computing IVT for BB HRES")
                ivt = get_ivt(my_case, bb_hres_ar_forecast)
            
            hres_graphics[my_id, "ivt"] = ivt

        if args.run_cira_fourv2:
            print("Computing IVT for FOURV2")
            if cira_fourv2_ar_forecast is None:
                cira_fourv2_ar_ifs_forecast = atmospheric_river_forecast_setup.get_cira_ar_forecast("FOURv2", "IFS", include_ivt=True)

            ivt = get_ivt(my_case, cira_fourv2_ar_forecast)
            fourv2_graphics[my_id, "ivt"] = ivt

        if args.run_cira_gc:
            print("Computing IVT for GC")
            if gc_ar_forecast is None:  
                gc_ar_forecast = atmospheric_river_forecast_setup.get_cira_gc_ar_forecast("Graphcast", "IFS", include_ivt=True)
            ivt = get_ivt(my_case, gc_ar_forecast)
            gc_graphics[my_id, "ivt"] = ivt

        if args.run_cira_pangu:
            print("Computing IVT for PANG")
            if pang_ar_forecast is None:
                pang_ar_forecast = atmospheric_river_forecast_setup.get_cira_ar_forecast("Pangu", "IFS", include_ivt=True)
            ivt = get_ivt(my_case, pang_ar_forecast)
            pang_graphics[my_id, "ivt"] = ivt

        if args.run_bb_graphcast:
            print("Computing IVT for Graphcast")
            if bb_graphcast_ar_forecast is None:
                bb_graphcast_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("Graphcast", include_ivt=True)
            ivt = get_ivt(my_case, bb_graphcast_ar_forecast)
            gc_graphics[my_id, "ivt"] = ivt

        if args.run_bb_pangu:
            print("Computing IVT for Pangu")
            if bb_pangu_ar_forecast is None:
                bb_pangu_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("Pangu", include_ivt=True)
            ivt = get_ivt(my_case, bb_pangu_ar_forecast)
            pang_graphics[my_id, "ivt"] = ivt

        if args.run_bb_aifs:
            print("Computing IVT for AIFS")
            if bb_aifs_ar_forecast is None:
                bb_aifs_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("AIFS", include_ivt=True)
            ivt = get_ivt(my_case, bb_aifs_ar_forecast)
            aifs_graphics[my_id, "ivt"] = ivt

        if args.run_era5:
            print("Computing IVT for ERA5")
            if era5 is None:
                era5 = atmospheric_river_forecast_setup.get_era5(include_ivt=True)
            ivt = get_ivt(my_case, era5)
            era5_graphics[my_id, "ivt"] = ivt

    print("Saving the graphics objects")
    if args.run_hres:
        pickle.dump(
            hres_graphics, open(basepath + "saved_data/hres_ar_graphics.pkl", "wb")
        )
    if args.run_cira_fourv2:
        pickle.dump(
            fourv2_graphics, open(basepath + "saved_data/fourv2_cira_ar_graphics.pkl", "wb")
        )
    if args.run_cira_gc:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_cira_ar_graphics.pkl", "wb")
        )
    if args.run_cira_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_cira_ar_graphics.pkl", "wb")
        )  
    if args.run_bb_graphcast:
        pickle.dump(
            gc_graphics, open(basepath + "saved_data/gc_bb_ar_graphics.pkl", "wb")
        )
    if args.run_bb_pangu:
        pickle.dump(
            pang_graphics, open(basepath + "saved_data/pang_bb_ar_graphics.pkl", "wb")
        )
    if args.run_bb_aifs:
        pickle.dump(
            aifs_graphics, open(basepath + "saved_data/aifs_bb_ar_graphics.pkl", "wb")
        )
    if args.run_era5:
        pickle.dump(
            era5_graphics, open(basepath + "saved_data/era5_ar_graphics.pkl", "wb")
        )

    print("Done")