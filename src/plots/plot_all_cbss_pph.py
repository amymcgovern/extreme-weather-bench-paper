# setup all the imports
import argparse
import pickle
from pathlib import Path

import cartopy.crs as ccrs  # noqa: E402
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import extremeweatherbench as ewb
from joblib import Parallel, delayed  # noqa: E402
from joblib.externals.loky import get_reusable_executor  # noqa: E402

import src.plots.plotting_utils as plot_utils  # noqa: E402
import src.plots.results_utils as results_utils  # noqa: E402
import src.plots.severe_convection_utils as severe_utils

# to plot the targets, we need to run the pipeline for each case and target

def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = ewb.inputs.PPH()
    pph = ewb.evaluate.run_pipeline(ewb_case, pph_target)
    cbss = ewb.evaluate.run_pipeline(ewb_case, forecast_source)

    return cbss, pph

def get_lsr_from_case_op(my_case, case_operators_with_targets_established):
    for (id, case_info) in case_operators_with_targets_established:
        if id == my_case.case_id_number:
            if case_info.attrs["source"] == "local_storm_reports":
                return case_info
   
def plot_cbss_pph_panel(cbss, pph, my_case, lsrs, ax=None, title=None, lead_time_hours=0, 
    gridlines_kwargs={}, geographic_features_kwargs={}, left_label=None):
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

        non_sparse_lsrs = ewb.utils.stack_dataarray_from_dims(
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
                    left_label=left_label,
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
    ewb_cases = ewb.cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]

    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = ewb.cases.build_case_operators(
        ewb_cases, ewb.defaults.get_brightband_evaluation_objects()
    )

    # uncomment this for debugging and faster plotting
    parser = argparse.ArgumentParser(
            description="Plot all CBSS and PPH cases."
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        default=False,
        help="Plot for paper (default: False)",
    )

    parser.add_argument(
        "--marginal",
        action="store_true",
        default=False,
        help="Plot for marginal cases (default: False)",
    )

    args = parser.parse_args()
    paper = args.paper
    
    if (args.marginal):
        # load the marginal severe cases
        marginal_severe_yaml_path = Path(ewb.__file__).parent / "data" / "marginal_severe_convection_cases.yaml"
        marginal_severe_cases = ewb.cases.load_individual_cases_from_yaml(marginal_severe_yaml_path)
        marginal_severe_cases = [n for n in marginal_severe_cases if n.event_type == "severe_convection"]
        marginal_severe_case_operators = ewb.cases.build_case_operators(
            marginal_severe_cases, ewb.defaults.get_brightband_evaluation_objects()
        )
        ewb_cases = marginal_severe_cases
        case_operators = marginal_severe_case_operators


    if paper:
        ewb_cases = [n for n in ewb_cases if n.case_id_number in [316, 269]]
    
    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = ewb.cases.build_case_operators(
        ewb_cases, ewb.defaults.get_brightband_evaluation_objects()
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
                ewb.evaluate.run_pipeline(co.case_metadata, co.target),
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

    print("Loading in the graphics objects")
    # load in the graphics objects
    if paper:
        hres_graphics = pickle.load(open(basepath + "saved_data/hres_severe_graphics_paper.pkl", "rb"))
        bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_severe_graphics_paper.pkl", "rb"))
        bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_severe_graphics_paper.pkl", "rb"))
        bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_severe_graphics_paper.pkl", "rb"))
    elif (args.marginal):
        print("Loading in the HRES graphics object")
        hres_graphics = pickle.load(open(basepath + "saved_data/hres_graphics_severe_marginal.pkl", "rb"))
        print("Loading in the GraphCast graphics object")
        bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_severe_graphics_marginal.pkl", "rb"))
        print("Loading in the Pangu graphics object")
        bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_severe_graphics_marginal.pkl", "rb"))
        print("Loading in the AIFS graphics object")
        bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_severe_graphics_marginal.pkl", "rb"))
    else:
        print("Loading in the HRES graphics object")
        hres_graphics = pickle.load(open(basepath + "saved_data/hres_graphics_severe_marginal.pkl", "rb"))
        print("Loading in the GraphCast graphics object")
        bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_severe_graphics.pkl", "rb"))
        print("Loading in the Pangu graphics object")
        bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_severe_graphics.pkl", "rb"))
        print("Loading in the AIFS graphics object")
        bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_severe_graphics.pkl", "rb"))

    lead_times_to_plot = [10*24, 7*24, 5*24, 3*24, 24]

    # for debugging, downselect the cases
    print("Plotting the cases")
    for my_case in ewb_cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        my_lsr = get_lsr_from_case_op(my_case, case_operators_with_targets_established)

        # make a subplot for each model and ensure it is a cartopy plot
        # Use gridspec for better control over spacing
        n_cols = len(lead_times_to_plot)
        n_rows = 4
        
        # Define figure size (can be adjusted independently of subplot spacing)
        width_per_col = 3
        height_per_row = 3
        total_width = width_per_col * n_cols
        total_height = height_per_row * n_rows
        
        fig = plt.figure(figsize=(total_width, total_height))
        
        # Create gridspec with adjustable spacing
        # wspace: width space between subplots (as fraction of subplot width)
        # hspace: height space between subplots (as fraction of subplot height)
        gs = gridspec.GridSpec(n_rows, n_cols, figure=fig, 
                               wspace=0.1, hspace=0.1,
                               left=0.05, right=0.95, top=0.90, bottom=0.1)
        
        # Create axes with cartopy projection
        axs = [[fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree()) 
                for j in range(n_cols)] for i in range(n_rows)]
        axs = np.array(axs)  # Convert to numpy array for easier indexing

        # Check if the graphics data exists for this case
        if (my_id, "cbss") in hres_graphics and (my_id, "pph") in hres_graphics:
            cbss_hres, pph_hres = hres_graphics[my_id, "cbss"], hres_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                title = f"{lead_time_hours} hours"
                if (i == 0):
                    left_label = "HRES"
                else:
                    left_label = None
                plot_cbss_pph_panel(cbss_hres, pph_hres, my_case, lsrs=my_lsr, 
                    ax=axs[0, i], title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label)
        
        else:
            print(f"Skipping HRES for case {my_id}: missing cbss or pph data in graphics object")
        

        if (my_id, "cbss") in bb_graphcast_graphics and (my_id, "pph") in bb_graphcast_graphics:
            cbss_gc, pph_gc = bb_graphcast_graphics[my_id, "cbss"], bb_graphcast_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    left_label = "GraphCast"
                else:
                    left_label = None
                title = ""

                plot_cbss_pph_panel(cbss_gc, pph_gc, my_case, lsrs=my_lsr, ax=axs[1, i], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label)
        else:
            print(f"Skipping GraphCast for case {my_id}: missing cbss or pph data in graphics object")


        if (my_id, "cbss") in bb_pangu_graphics and (my_id, "pph") in bb_pangu_graphics:
            cbss_pang, pph_pang = bb_pangu_graphics[my_id, "cbss"], bb_pangu_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    left_label = "Pangu"
                else:
                    left_label = None
                title = ""
                plot_cbss_pph_panel(cbss_pang, pph_pang, my_case, lsrs=my_lsr, ax=axs[2, i], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label)
        else:
            print(f"Skipping Pangu for case {my_id}: missing cbss or pph data in graphics object")

        if (my_id, "cbss") in bb_aifs_graphics and (my_id, "pph") in bb_aifs_graphics:
            cbss_aifs, pph_aifs = bb_aifs_graphics[my_id, "cbss"], bb_aifs_graphics[my_id, "pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                if (i == 0):
                    left_label = "AIFS"
                else:
                    left_label = None
                title = ""
                plot_cbss_pph_panel(cbss_aifs, pph_aifs, my_case, lsrs=my_lsr, ax=axs[3, i], 
                    title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label)
        else:
            print(f"Skipping AIFS for case {my_id}: missing cbss or pph data in graphics object")

        # plot the colorbar at the bottom of the figure
        cmap, norm, levels = severe_utils.setup_cbss_colormap_and_levels()
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])  # Empty array, we just need the colormap/norm

        plot_utils.add_horizontal_colorbar_below(
            fig,
            sm,
            [axs[n_rows - 1, j] for j in range(n_cols)],
            n_subplots=n_cols,
            levels=levels,
            label=r"Craven-Brooks Significant Severe (m$^{3}$/s$^{3}$)",
            label_fontsize=24,
            tick_labelsize=18,
        )


        # make the overall title and save it        
        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32, y=0.98)
        fig.savefig(basepath + f"graphics/severe/severe_case_{my_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)