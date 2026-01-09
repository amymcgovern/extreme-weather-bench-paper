# setup all the imports
import argparse
import pickle
from pathlib import Path
from joblib import Parallel, delayed  # noqa: E402
from joblib.externals.loky import get_reusable_executor  # noqa: E402
import pandas as pd    
import matplotlib.pyplot as plt
import cartopy.crs as ccrs  # noqa: E402

from extremeweatherbench import (
    cases,
    evaluate,
    inputs,defaults,
)

import src.plots.plotting_utils as plot_utils  # noqa: E402
import src.plots.results_utils as results_utils  # noqa: E402
import src.plots.severe_convection_utils as severe_utils
import src.plots.plotting_styles as ps

# to plot the targets, we need to run the pipeline for each case and target

def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = inputs.PPH()
    pph = evaluate.run_pipeline(ewb_case, pph_target)
    cbss = evaluate.run_pipeline(ewb_case, forecast_source)

    return cbss, pph

def get_lsr_from_case_op(my_case, case_operators_with_targets_established):
    for (id, case_info) in case_operators_with_targets_established:
        if id == my_case.case_id_number:
            if case_info.attrs["source"] == "local_storm_reports":
                return case_info
   
def plot_cbss_pph_panel(cbss, pph, my_case, lsrs, ax=None, title=None, lead_time_hours=0, gridlines_kwargs={}, geographic_features_kwargs={}):
    my_bbox = dict()
    my_bbox["latitude_min"] = my_case.location.latitude_min
    my_bbox["latitude_max"] = my_case.location.latitude_max
    my_bbox["longitude_min"] = my_case.location.longitude_min
    my_bbox["longitude_max"] = my_case.location.longitude_max

    # grab the valid time to plot
    valid_time = cbss.craven_brooks_significant_severe.valid_time
    my_pph = pph.sel(valid_time=valid_time).practically_perfect_hindcast.squeeze()

    # grab the lsrs and convert to a dataframe
    lsrs = lsrs.sel(valid_time=valid_time)
    non_sparse_lsrs = plot_utils.stack_dataarray_from_dims(
                        lsrs["report_type"], ["latitude", "longitude"]
                    ).squeeze()
    hail_data = non_sparse_lsrs[non_sparse_lsrs == 2]
    tornado_data = non_sparse_lsrs[non_sparse_lsrs == 3]
    hail_data = hail_data.to_dataframe().reset_index()
    tornado_data = tornado_data.to_dataframe().reset_index()

    ax, mappable = severe_utils.plot_cbss_forecast_panel(
        cbss_data=cbss.craven_brooks_significant_severe.squeeze(),
        target_date=my_case.start_date,
        lead_time_hours=lead_time_hours,
        bbox=my_bbox,
        ax=ax,
        pph_data=my_pph,
        tornado_reports=tornado_data,
        hail_reports=hail_data,
        title=title,
        alpha=0.6,
        gridlines_kwargs=gridlines_kwargs,
        geographic_features_kwargs=geographic_features_kwargs,
    )
    return ax, mappable

def get_stats(results, forecast_source, my_case, lead_time_hours=0):
    # list the statistics for each case
    tp_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='TruePositives', 
        init_time='zeroz', case_id_list=[my_case.case_id_number])

    tp_mean = tp_all["value"].mean("case_id_number")
    lead_time_td = pd.Timedelta(hours=lead_time_hours)
    tp = tp_mean.sel(lead_time=lead_time_td, method="nearest")

    fn_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='FalseNegatives', 
        init_time='zeroz', case_id_list=[my_case.case_id_number])

    fn_mean = fn_all["value"].mean("case_id_number")
    fn = fn_mean.sel(lead_time=lead_time_td, method="nearest")
    
    csi_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='CriticalSuccessIndex', 
        init_time='zeroz', case_id_list=[my_case.case_id_number])

    csi_mean = csi_all["value"].mean("case_id_number")
    csi = csi_mean.sel(lead_time=lead_time_td, method="nearest")

    far_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='FalseAlarmRatio', 
        init_time='zeroz', case_id_list=[my_case.case_id_number])

    far_mean = far_all["value"].mean("case_id_number")
    far = far_mean.sel(lead_time=lead_time_td, method="nearest")

    es_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='EarlySignal', 
        init_time='zeroz', case_id_list=[my_case.case_id_number])

    es_mean = es_all["value"].mean("case_id_number")
    es = es_mean.sel(lead_time=lead_time_td, method="nearest")
    
    return [tp.values, fn.values, csi.values, far.values, es.values]

if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "severe_convection")
    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = cases.build_case_operators(
        ewb_cases, defaults.get_brightband_evaluation_objects()
    )

    # load in all the case info (note this takes awhile in non-parallel form as it has to
    # run all the target information for each case)
    # this will return a list of tuples with the case id and the target dataset
    print("running the pipeline for each case and target")
    parallel = Parallel(n_jobs=32, return_as="generator", backend="loky")
    case_operators_with_targets_established_generator = parallel(
        delayed(
            lambda co: (
                co.case_metadata.case_id_number,
                evaluate.run_pipeline(co.case_metadata, co.target),
            )
        )(case_operator)
        for case_operator in case_operators
    )
    case_operators_with_targets_established = list(
        case_operators_with_targets_established_generator
    )
    # this will throw a bunch of errors below but they're not consequential. this releases
    # the memory as it shuts down the workers
    get_reusable_executor().shutdown(wait=True)

    print("Loading in the results")
    # load in the results
    hres_severe_results = pd.read_pickle(basepath + "saved_data/hres_severe_results.pkl")
    gc_severe_results = pd.read_pickle(basepath + "saved_data/bb_graphcast_severe_results.pkl")
    pang_severe_results = pd.read_pickle(basepath + "saved_data/bb_pangu_severe_results.pkl")
    aifs_severe_results = pd.read_pickle(basepath + "saved_data/bb_aifs_severe_results.pkl")

    print("Loading in the graphics objects")
    # load in the graphics objects
    hres_graphics = pickle.load(open(basepath + "saved_data/hres_graphics.pkl", "rb"))
    bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_graphics.pkl", "rb"))
    bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_graphics.pkl", "rb"))
    bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_graphics.pkl", "rb"))

        # plot all of the cases where we had all three models
    for my_case in ewb_cases.cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        my_lsr = get_lsr_from_case_op(my_case, case_operators_with_targets_established)
        
        cbss_hres, pph_hres = hres_graphics[my_id, "cbss"], hres_graphics[my_id, "pph"]
        cbss_gc, pph_gc = bb_graphcast_graphics[my_id, "cbss"], bb_graphcast_graphics[my_id, "pph"]
        cbss_pang, pph_pang = bb_pangu_graphics[my_id, "cbss"], bb_pangu_graphics[my_id, "pph"]
        cbss_aifs, pph_aifs = bb_aifs_graphics[my_id, "cbss"], bb_aifs_graphics[my_id, "pph"]

        # make a subplot for each model and ensure it is a cartopy plot
        fig, axs = plt.subplots(2, 4, figsize=(10, 4), subplot_kw={'projection': ccrs.PlateCarree()})

        plot_cbss_pph_panel(cbss_hres, pph_hres, my_case, lsrs=my_lsr, ax=axs[0,0], title="HRES", lead_time_hours=48)
        plot_cbss_pph_panel(cbss_gc, pph_gc, my_case, lsrs=my_lsr, ax=axs[0,1], title="GraphCast", lead_time_hours=48)
        plot_cbss_pph_panel(cbss_pang, pph_pang, my_case, lsrs=my_lsr, ax=axs[0,2], title="Pangu", lead_time_hours=48)
        plot_cbss_pph_panel(cbss_aifs, pph_aifs, my_case, lsrs=my_lsr, ax=axs[0,3], title="AIFS", lead_time_hours=48)


        # now plot the stats
        [tp, fn, csi, far, es] = get_stats(hres_severe_results, 
            ps.hres_ifs_settings['forecast_source'], my_case, lead_time_hours=48)

        axs[1,0].text(0.5, 0.5, f"CSI: {csi:.2f}\n"
                f"FAR: {far:.2f}\n"
                f"ES: {es:.2f}\n"
                f"TP: {tp:.2f}\n"
                f"FN: {fn:.2f}",
                transform=axs[1,0].transAxes,
                ha='center', va='center')

        [tp, fn, csi, far, es] = get_stats(gc_severe_results, 
            ps.gc_bb_ifs_settings['forecast_source'], my_case, lead_time_hours=48)

        axs[1,1].text(0.5, 0.5, f"CSI: {csi:.2f}\n"
                f"FAR: {far:.2f}\n"
                f"ES: {es:.2f}\n"
                f"TP: {tp:.2f}\n"
                f"FN: {fn:.2f}",
                transform=axs[1,1].transAxes,
                ha='center', va='center')

        [tp, fn, csi, far, es] = get_stats(pang_severe_results, 
            ps.pangu_bb_ifs_settings['forecast_source'], my_case, lead_time_hours=48)

        axs[1,2].text(0.5, 0.5, f"CSI: {csi:.2f}\n"
                f"FAR: {far:.2f}\n"
                f"ES: {es:.2f}\n"
                f"TP: {tp:.2f}\n"
                f"FN: {fn:.2f}",
                transform=axs[1,2].transAxes,
                ha='center', va='center')

        [tp, fn, csi, far, es] = get_stats(aifs_severe_results, 
            ps.aifs_bb_ifs_settings['forecast_source'], my_case, lead_time_hours=48)

        axs[1,3].text(0.5, 0.5, f"CSI: {csi:.2f}\n"
                f"FAR: {far:.2f}\n"
                f"ES: {es:.2f}\n"
                f"TP: {tp:.2f}\n"
                f"FN: {fn:.2f}",
                transform=axs[1,3].transAxes,
                ha='center', va='center')
        

        fig.suptitle(f"Case {my_id}")
        fig.savefig(basepath + f"saved_data/severe_case_{my_id}.png", dpi=300, bbox_inches="tight")