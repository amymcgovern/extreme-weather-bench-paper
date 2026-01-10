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
    inputs,defaults,utils,
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

    
    try:
        # grab the valid time to plot and get the pph and lsrs for that time
        valid_time = cbss.craven_brooks_significant_severe.valid_time
        my_pph = pph.sel(valid_time=valid_time).practically_perfect_hindcast.squeeze()

        # grab the lsrs and convert to a dataframe
        lsrs = lsrs.sel(valid_time=valid_time)

        non_sparse_lsrs = utils.stack_dataarray_from_dims(
                            lsrs["report_type"], ["latitude", "longitude"]
                        ).squeeze()
        
        # Handle empty or single-element cases
        if non_sparse_lsrs.size == 0:
            # Create empty dataframes with expected columns
            hail_data = pd.DataFrame(columns=['latitude', 'longitude'])
            tornado_data = pd.DataFrame(columns=['latitude', 'longitude'])
        else:
            # Check if we have the expected structure (should have a dimension for indexing)
            try:
                hail_data = non_sparse_lsrs[non_sparse_lsrs == 2]
                tornado_data = non_sparse_lsrs[non_sparse_lsrs == 3]
                
                # Convert to dataframe, handling empty results
                if hail_data.size > 0:
                    hail_data = hail_data.to_dataframe().reset_index()
                else:
                    hail_data = pd.DataFrame(columns=['latitude', 'longitude'])
                
                if tornado_data.size > 0:
                    tornado_data = tornado_data.to_dataframe().reset_index()
                else:
                    tornado_data = pd.DataFrame(columns=['latitude', 'longitude'])
            except (IndexError, ValueError, AttributeError) as e:
                # Handle cases where the xarray structure is unexpected (e.g., single report)
                print(f"Warning: Unexpected LSR structure using empty dataframes. Error: {e}")
                hail_data = pd.DataFrame(columns=['latitude', 'longitude'])
                tornado_data = pd.DataFrame(columns=['latitude', 'longitude'])

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
    except Exception as e:
        # Fallback if stack_dataarray_from_dims fails
        print(f"Warning: Failed to process LSRs or missing CBSS/PPH data, using empty dataframes. Error: {e}")
        hail_data = pd.DataFrame(columns=['latitude', 'longitude'])
        tornado_data = pd.DataFrame(columns=['latitude', 'longitude'])
        return None, None

    

def get_stats(results, forecast_source, my_case, lead_time_days=[1, 3, 5, 7, 10]):
    # list the statistics for each case
    tp_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='TruePositives', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    tp_mean = tp_all["value"].mean("case_id_number")

    fn_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='FalseNegatives', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    fn_mean = fn_all["value"].mean("case_id_number")
    
    csi_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='CriticalSuccessIndex', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    csi_mean = csi_all["value"].mean("case_id_number")

    far_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='FalseAlarmRatio', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    far_mean = far_all["value"].mean("case_id_number")

    es_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='EarlySignal', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)
    es_mean = es_all["value"].mean("case_id_number")
    
    return [tp_mean.values, fn_mean.values, csi_mean.values, far_mean.values, es_mean.values]

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

    # print("Loading in the results")
    # # load in the results
    # hres_severe_results = pd.read_pickle(basepath + "saved_data/hres_severe_results.pkl")
    # gc_severe_results = pd.read_pickle(basepath + "saved_data/bb_graphcast_severe_results.pkl")
    # pang_severe_results = pd.read_pickle(basepath + "saved_data/bb_pangu_severe_results.pkl")
    # aifs_severe_results = pd.read_pickle(basepath + "saved_data/bb_aifs_severe_results.pkl")

    print("Loading in the graphics objects")
    # load in the graphics objects
    print("Loading in the HRES graphics object")
    hres_graphics = pickle.load(open(basepath + "saved_data/hres_graphics.pkl", "rb"))
    print("Loading in the GraphCast graphics object")
    bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_graphics.pkl", "rb"))
    print("Loading in the Pangu graphics object")
    bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_graphics.pkl", "rb"))
    print("Loading in the AIFS graphics object")
    bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_graphics.pkl", "rb"))

    lead_times_to_plot = [24, 3*24, 5*24, 7*24, 10*24]

    # for debugging, downselect the cases
    print("Plotting the cases")
    for my_case in ewb_cases.cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        my_lsr = get_lsr_from_case_op(my_case, case_operators_with_targets_established)

        # make a subplot for each model and ensure it is a cartopy plot
        fig, axs = plt.subplots(len(lead_times_to_plot), 4, figsize=(20, 3 * len(lead_times_to_plot)), subplot_kw={'projection': ccrs.PlateCarree()})

        # Check if the graphics data exists for this case
        if (my_id, "cbss") in hres_graphics and (my_id, "pph") in hres_graphics:
            cbss_hres, pph_hres = hres_graphics[my_id, "cbss"], hres_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    title = f"HRES\n{lead_time_hours} hours"
                else:
                    title = f"{lead_time_hours} hours"
                plot_cbss_pph_panel(cbss_hres, pph_hres, my_case, lsrs=my_lsr, 
                    ax=axs[i, 0], title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": True, "show_bottom_labels": True})
        #     [tp, fn, csi, far, es] = get_stats(hres_severe_results, 
        #         ps.hres_ifs_settings['forecast_source'], my_case, lead_time_days=[int(lead_time_hours / 24)])
        # # print(f"HRES: {tp[0]:.2f}, {fn[0]:.2f}, {csi[0]:.2f}, {far[0]:.2f}, {es[0]:.2f}")
        #     if (len(csi) > 0 and len(far) > 0 and len(es) > 0 and len(tp) > 0 and len(fn) > 0):
        #         axs[1,0].text(0.5, 0.5, f"CSI: {csi[0]:.2f}\n"
        #                 f"FAR: {far[0]:.2f}\n"
        #                 f"ES: {es[0]:.2f}\n"
        #                 f"TP: {tp[0]:.2f}\n"
        #                 f"FN: {fn[0]:.2f}",
        #                 transform=axs[1,0].transAxes,
        #                 ha='center', va='center')
        #     else:
        #         print(f"Skipping HRES for case {my_id}: missing stats data")
        else:
            print(f"Skipping HRES for case {my_id}: missing cbss or pph data in graphics object")
        

        if (my_id, "cbss") in bb_graphcast_graphics and (my_id, "pph") in bb_graphcast_graphics:
            cbss_gc, pph_gc = bb_graphcast_graphics[my_id, "cbss"], bb_graphcast_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    title = f"GraphCast\n{lead_time_hours} hours"
                else:
                    title = f"{lead_time_hours} hours"
                plot_cbss_pph_panel(cbss_gc, pph_gc, my_case, lsrs=my_lsr, ax=axs[i, 1], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False})
            
            # [tp, fn, csi, far, es] = get_stats(gc_severe_results, 
            #     ps.gc_bb_ifs_settings['forecast_source'], my_case, lead_time_days=[int(lead_time_hours / 24)])
            # # print(f"GraphCast: {tp[0]:.2f}, {fn[0]:.2f}, {csi[0]:.2f}, {far[0]:.2f}, {es[0]:.2f}")
            # if (len(csi) > 0 and len(far) > 0 and len(es) > 0 and len(tp) > 0 and len(fn) > 0):
            #     axs[1,1].text(0.5, 0.5, f"CSI: {csi[0]:.2f}\n"
            #                 f"FAR: {far[0]:.2f}\n"
            #                 f"ES: {es[0]:.2f}\n"
            #                 f"TP: {tp[0]:.2f}\n"
            #                 f"FN: {fn[0]:.2f}",
            #                 transform=axs[1,1].transAxes,
            #                 ha='center', va='center')
            # else:
            #     print(f"Skipping GraphCast for case {my_id}: missing stats data")
        else:
            print(f"Skipping GraphCast for case {my_id}: missing cbss or pph data in graphics object")


        if (my_id, "cbss") in bb_pangu_graphics and (my_id, "pph") in bb_pangu_graphics:
            cbss_pang, pph_pang = bb_pangu_graphics[my_id, "cbss"], bb_pangu_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    title = f"Pangu\n{lead_time_hours} hours"
                else:
                    title = f"{lead_time_hours} hours"
                plot_cbss_pph_panel(cbss_pang, pph_pang, my_case, lsrs=my_lsr, ax=axs[i, 2], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False})
            # [tp, fn, csi, far, es] = get_stats(pang_severe_results, 
            #     ps.pangu_bb_ifs_settings['forecast_source'], my_case, lead_time_days=[int(lead_time_hours / 24)])
            # print(f"Pangu: {tp[0]:.2f}, {fn[0]:.2f}, {csi[0]:.2f}, {far[0]:.2f}, {es[0]:.2f}")
            # if (len(csi) > 0 and len(far) > 0 and len(es) > 0 and len(tp) > 0 and len(fn) > 0):
            #     axs[1,2].text(0.5, 0.5, f"CSI: {csi[0]:.2f}\n"
            #             f"FAR: {far[0]:.2f}\n"
            #             f"ES: {es[0]:.2f}\n"
            #             f"TP: {tp[0]:.2f}\n"
            #             f"FN: {fn[0]:.2f}",
            #             transform=axs[1,2].transAxes,
            #             ha='center', va='center')
            # else:
            #     print(f"Skipping Pangu for case {my_id}: missing stats data")
        else:
            print(f"Skipping Pangu for case {my_id}: missing cbss or pph data in graphics object")

        if (my_id, "cbss") in bb_aifs_graphics and (my_id, "pph") in bb_aifs_graphics:
            cbss_aifs, pph_aifs = bb_aifs_graphics[my_id, "cbss"], bb_aifs_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    title = f"AIFS\n{lead_time_hours} hours"
                else:
                    title = f"{lead_time_hours} hours"
                plot_cbss_pph_panel(cbss_aifs, pph_aifs, my_case, lsrs=my_lsr, ax=axs[i, 3], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False})
            # [tp, fn, csi, far, es] = get_stats(aifs_severe_results, 
            #     ps.aifs_ifs_settings['forecast_source'], my_case, lead_time_days=[2])
            # # print(f"AIFS: {tp[0]:.2f}, {fn[0]:.2f}, {csi[0]:.2f}, {far[0]:.2f}, {es[0]:.2f}")
            # if (len(csi) > 0 and len(far) > 0 and len(es) > 0 and len(tp) > 0 and len(fn) > 0):
            #     axs[1,3].text(0.5, 0.5, f"CSI: {csi[0]:.2f}\n"
            #             f"FAR: {far[0]:.2f}\n"
            #             f"ES: {es[0]:.2f}\n"
            #             f"TP: {tp[0]:.2f}\n"
            #             f"FN: {fn[0]:.2f}",
            #             transform=axs[1,3].transAxes,
            #             ha='center', va='center')
            # else:
            #     print(f"Skipping AIFS for case {my_id}: missing stats data")
        else:
            print(f"Skipping AIFS for case {my_id}: missing cbss or pph data in graphics object")

        # make the overall title and save it        
        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32)
        fig.savefig(basepath + f"saved_data/severe_case_{my_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)