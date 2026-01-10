# setup all the imports
import argparse
import pickle
from pathlib import Path
from joblib import Parallel, delayed  # noqa: E402
from joblib.externals.loky import get_reusable_executor  # noqa: E402
import pandas as pd    
import matplotlib.pyplot as plt
import cartopy.crs as ccrs  # noqa: E402
from matplotlib.cm import ScalarMappable  # noqa: E402
import xarray as xr

from extremeweatherbench import (
    cases,
    evaluate,
    inputs,defaults,utils,
)

import src.plots.plotting_utils as plot_utils  # noqa: E402
import src.plots.results_utils as results_utils  # noqa: E402
import src.plots.severe_convection_utils as severe_utils
import src.plots.plotting_styles as ps
import src.plots.atmospheric_river_utils as ar_plot_utils

def select_ivt_and_maks(graphics_obect, lead_time_hours):
    # select the right lead time
    try:
        lead_time_td = pd.Timedelta(hours=lead_time_hours)
        ivt = graphics_obect["integrated_vapor_transport"].sel(lead_time=lead_time_td, method="nearest")
        ar_mask = graphics_obect["atmospheric_river_mask"].sel(lead_time=lead_time_td, method="nearest")

        # select the right valid time (hack for now to always select the first valid time)
        valid_time = graphics_obect["integrated_vapor_transport"].valid_time[0]
        ivt2 = ivt.sel(valid_time=valid_time, method="nearest")
        ar_mask2 = ar_mask.sel(valid_time=valid_time, method="nearest")
        return ivt2, ar_mask2
    except (KeyError, AttributeError) as e:
        case_id = getattr(graphics_obect, 'case_id_number', 'unknown')
        print(f"Skipping {lead_time_hours} hours for case {case_id}: missing data. Error: {e}")
        return None, None
    except Exception as e:
        case_id = getattr(graphics_obect, 'case_id_number', 'unknown')
        print(f"Skipping {lead_time_hours} hours for case {case_id}: missing data. Error: {e}")
        return None, None

def select_ivt_and_maks_era5(graphics_obect):
    ivt = graphics_obect["integrated_vapor_transport"]
    ar_mask = graphics_obect["atmospheric_river_mask"]

    # select the right valid time (hack for now to always select the first valid time)
    valid_time = graphics_obect["integrated_vapor_transport"].valid_time[0]
    ivt = ivt.sel(valid_time=valid_time, method="nearest")
    ar_mask = ar_mask.sel(valid_time=valid_time, method="nearest")
    return ivt, ar_mask

if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
    ewb_cases = ewb_cases.select_cases("event_type", "atmospheric_river")
    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = cases.build_case_operators(
        ewb_cases, defaults.get_brightband_evaluation_objects()
    )

    print("Loading in the results")
    # load in the results
    hres_ar_results = pd.read_pickle(basepath + "saved_data/hres_ar_results.pkl")
    gc_ar_results = pd.read_pickle(basepath + "saved_data/bb_graphcast_ar_results.pkl")
    pang_ar_results = pd.read_pickle(basepath + "saved_data/bb_pangu_ar_results.pkl")
    aifs_ar_results = pd.read_pickle(basepath + "saved_data/bb_aifs_ar_results.pkl")

    print("Loading in the graphics objects")
    # load in the graphics objects
    print("Loading in the HRES graphics object")
    hres_graphics = pickle.load(open(basepath + "saved_data/hres_ar_graphics.pkl", "rb"))
    print("Loading in the GraphCast graphics object")
    bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/gc_bb_ar_graphics.pkl", "rb"))
    print("Loading in the Pangu graphics object")
    bb_pangu_graphics = pickle.load(open(basepath + "saved_data/pang_bb_ar_graphics.pkl", "rb"))
    print("Loading in the AIFS graphics object")
    bb_aifs_graphics = pickle.load(open(basepath + "saved_data/aifs_bb_ar_graphics.pkl", "rb"))
    print("Loading in the ERA5 graphics object")
    era5_graphics = pickle.load(open(basepath + "saved_data/era5_ar_graphics.pkl", "rb"))

    lead_times_to_plot = [24, 3*24, 5*24,7*24,10*24]

    # ewb_cases = ewb_cases.select_cases("case_id_number", 95)
    # for debugging, downselect the cases
    print("Plotting the cases")
    for my_case in ewb_cases.cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        col_length = 5

        # make a subplot for each model and ensure it is a cartopy plot
        fig, axs = plt.subplots(len(lead_times_to_plot), col_length, figsize=(20, 3 * len(lead_times_to_plot)), 
            subplot_kw={'projection': ccrs.PlateCarree()})

        if (my_id, "ivt") in era5_graphics:
            for i in range(len(lead_times_to_plot)):
                era5_ivt, era5_ar_mask = select_ivt_and_maks_era5(era5_graphics[my_id, "ivt"])
                if (i == 0):
                    title = "ERA5\n"
                else:
                    title = None
                ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=era5_ivt, ar_mask=era5_ar_mask, 
                    title=title, ax=axs[i, 0], colorbar=False, show_axes=True)
        else:
            print(f"Skipping ERA5 for case {my_id}: missing ivt or ar mask data in graphics object")

        if (my_id, "ivt") in hres_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                hres_ivt, hres_ar_mask = select_ivt_and_maks(hres_graphics[my_id, "ivt"], lead_time_hours)
                if hres_ivt is not None and hres_ar_mask is not None:
                    if (i == 0):
                        title = f"HRES\n{lead_time_hours} hours"
                    else:
                        title = f"{lead_time_hours} hours"
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=hres_ivt, ar_mask=hres_ar_mask, 
                        title=title, ax=axs[i, 1], colorbar=False)
                else:
                    print(f"Skipping HRES for case {my_id}: missing ivt or ar mask data in graphics object")
            
        else:
            print(f"Skipping HRES for case {my_id}: missing cbss or pph data in graphics object")
        

        if (my_id, "ivt") in bb_graphcast_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                gc_ivt, gc_ar_mask = select_ivt_and_maks(bb_graphcast_graphics[my_id, "ivt"], lead_time_hours)
                if gc_ivt is not None and gc_ar_mask is not None:
                    if (i == 0):
                        title = f"Graphcast\n{lead_time_hours} hours"
                    else:
                        title = f"{lead_time_hours} hours"
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=gc_ivt, ar_mask=gc_ar_mask, 
                        title=title, ax=axs[i, 2], colorbar=False)   
                else:
                    print(f"Skipping GraphCast for case {my_id}: missing ivt or ar mask data in graphics object")
        else:
            print(f"Skipping GraphCast for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_pangu_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                pang_ivt, pang_ar_mask = select_ivt_and_maks(bb_pangu_graphics[my_id, "ivt"], lead_time_hours)
                if pang_ivt is not None and pang_ar_mask is not None:
                    if (i == 0):
                        title = f"Pangu\n{lead_time_hours} hours"
                    else:
                        title = f"{lead_time_hours} hours"
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=pang_ivt, ar_mask=pang_ar_mask, 
                        title=title, ax=axs[i, 3], colorbar=False)
                else:
                    print(f"Skipping Pangu for case {my_id}: missing ivt or ar mask data in graphics object")
        else:
            print(f"Skipping Pangu for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_aifs_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                aifs_ivt, aifs_ar_mask = select_ivt_and_maks(bb_aifs_graphics[my_id, "ivt"], lead_time_hours)
                if aifs_ivt is not None and aifs_ar_mask is not None:
                    if (i == 0):
                        title = f"AIFS\n{lead_time_hours} hours"
                    else:
                        title = f"{lead_time_hours} hours"
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=aifs_ivt, ar_mask=aifs_ar_mask, 
                        title=title, ax=axs[i, 4], colorbar=False)
                else:
                    print(f"Skipping AIFS for case {my_id}: missing ivt or ar mask data in graphics object")
        else:
            print(f"Skipping AIFS for case {my_id}: missing ivt data in graphics object")
        
        # show the colorbar below the bottom row (row 5)
        # Create a ScalarMappable for the colorbar
        cmap, norm = ar_plot_utils.setup_atmospheric_river_colormap_and_levels()
        sm = ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])  # Empty array, we just need the colormap/norm

        # Get the position from bottom row subplots to position colorbar below them
        pos0 = axs[len(lead_times_to_plot) - 1, 0].get_position(fig)
        pos3 = axs[len(lead_times_to_plot) - 1, col_length - 1].get_position(fig)
        # Create axes below row 5 that spans all 4 columns
        # Position it just below row 5, using a small height
        cbar_y = pos0.y0 - pos0.height * 0.3  # Position below bottom row
        cbar_height = pos0.height * 0.15  # Height for colorbar
        cbar_ax = fig.add_axes([pos0.x0, cbar_y, pos3.x1 - pos0.x0, cbar_height])

        # Add horizontal colorbar below bottom row
        cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
        cbar.set_label("Integrated Vapor Transport (kgm^-1s^-1)", size=32)
        cbar.ax.tick_params(labelsize=24)

        # make the overall title and save it        
        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32)
        fig.savefig(basepath + f"saved_data/ar_case_{my_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)