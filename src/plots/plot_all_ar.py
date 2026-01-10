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
    lead_time_td = pd.Timedelta(hours=lead_time_hours)
    ivt = graphics_obect.integrated_vapor_transport.sel(lead_time=lead_time_td, method="nearest")
    ar_mask = graphics_obect.atmospheric_river_mask.sel(lead_time=lead_time_td, method="nearest")

    # select the right valid time (hack for now to always select the first valid time)
    valid_time = graphics_obect.integrated_vapor_transport.valid_time[0]
    ivt2 = ivt.sel(valid_time=valid_time, method="nearest")
    ar_mask2 = ar_mask.sel(valid_time=valid_time, method="nearest")
    return ivt2, ar_mask2

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
    bb_graphcast_graphics = pickle.load(open(basepath + "saved_data/bb_graphcast_ar_graphics.pkl", "rb"))
    print("Loading in the Pangu graphics object")
    bb_pangu_graphics = pickle.load(open(basepath + "saved_data/bb_pangu_ar_graphics.pkl", "rb"))
    print("Loading in the AIFS graphics object")
    bb_aifs_graphics = pickle.load(open(basepath + "saved_data/bb_aifs_ar_graphics.pkl", "rb"))

    lead_time_hours = 72
    ewb_cases = ewb_cases.select_cases("case_id_number", 95)

    # for debugging, downselect the cases
    print("Plotting the cases")
    for my_case in ewb_cases.cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        
        # make a subplot for each model and ensure it is a cartopy plot
        fig, axs = plt.subplots(1, 4, figsize=(20, 4), subplot_kw={'projection': ccrs.PlateCarree()})

        # Check if the graphics data exists for this case
        if (my_id, "ivt") in hres_graphics:
            hres_ivt, hres_ar_mask = select_ivt_and_maks(hres_graphics[my_id, "ivt"], lead_time_hours)
            ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=hres_ivt, ar_mask=hres_ar_mask, 
                title="HRES", ax=axs[0], colorbar=False)
            
        else:
            print(f"Skipping HRES for case {my_id}: missing cbss or pph data in graphics object")
        

        if (my_id, "ivt") in bb_graphcast_graphics:
            gc_ivt, gc_ar_mask = select_ivt_and_maks(bb_graphcast_graphics[my_id, "ivt"], lead_time_hours)
            ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=gc_ivt, ar_mask=gc_ar_mask, 
                title="Graphcast", ax=axs[1], colorbar=False)
        else:
            print(f"Skipping GraphCast for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_pangu_graphics:
            pang_ivt, pang_ar_mask = select_ivt_and_maks(bb_pangu_graphics[my_id, "ivt"], lead_time_hours)
            ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=pang_ivt, ar_mask=pang_ar_mask, 
                title="Pangu", ax=axs[2], colorbar=False)
        else:
            print(f"Skipping Pangu for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_aifs_graphics:
            aifs_ivt, aifs_ar_mask = select_ivt_and_maks(bb_aifs_graphics[my_id, "ivt"], lead_time_hours)
            ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=aifs_ivt, ar_mask=aifs_ar_mask, 
                title="AIFS", ax=axs[3], colorbar=False)
        else:
            print(f"Skipping AIFS for case {my_id}: missing ivt data in graphics object")
        
        # show the colorbar below the bottom row (row 5)
        # Create a ScalarMappable for the colorbar
        cmap, norm = ar_plot_utils.setup_atmospheric_river_colormap_and_levels()
        sm = ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])  # Empty array, we just need the colormap/norm

        # Get the position from row 5 (bottom row) subplots to position colorbar below them
        pos0 = axs[0].get_position(fig)
        pos3 = axs[3].get_position(fig)
        # Create axes below row 5 that spans all 4 columns
        # Position it just below row 5, using a small height
        cbar_y = pos0.y0 - pos0.height * 0.2  # Position below row 5
        cbar_height = pos0.height * 0.15  # Height for colorbar
        cbar_ax = fig.add_axes([pos0.x0, cbar_y, pos3.x1 - pos0.x0, cbar_height])

        # Add horizontal colorbar below bottom row
        cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
        cbar.set_label("Integrated Vapor Transport (kgm^-1s^-1)", size=32)
        cbar.ax.tick_params(labelsize=24)

        # make the overall title and save it        
        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}")
        fig.savefig(basepath + f"saved_data/ar_case_{my_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)