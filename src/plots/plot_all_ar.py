# setup all the imports
import argparse
import pickle
from pathlib import Path

import cartopy.crs as ccrs  # noqa: E402
import matplotlib.pyplot as plt
import pandas as pd
from extremeweatherbench import (
    cases,
    defaults,
)
from matplotlib.cm import ScalarMappable  # noqa: E402

import src.plots.atmospheric_river_utils as ar_plot_utils

if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    parser = argparse.ArgumentParser(
        description="Plot all atmospheric river cases."
    )
    parser.add_argument(
        "--plot_era_separately",
        action="store_true",
        default=False,
        help="Plot ERA5 separately (default: False)",
    )
    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "atmospheric_river"]

    # for debugging, only look at one case (that happens to be lovely)
    # ewb_cases = [n for n in ewb_cases if n.case_id_number == 95]

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

    lead_times_to_plot = [10*24, 7*24, 5*24, 3*24, 24]
    

    # ewb_cases = [n for n in ewb_cases if n.case_id_number == 95]
    # for debugging, downselect the cases
    print("Plotting the cases")

    if args.plot_era_separately:
        fig = plt.figure(figsize=(5,5), projection=ccrs.PlateCarree())

        for my_case in ewb_cases:
            print(my_case.case_id_number)
            my_id = my_case.case_id_number

            era5_ivt, era5_ar_mask = ar_plot_utils.select_ivt_and_maks_era5(era5_graphics[my_id, "ivt"])
            title = "ERA5"
            ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=era5_ivt, ar_mask=era5_ar_mask, 
                title=title, colorbar=True, show_axes=True)
            fig.set_title(f"ERA5 for case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32)
            fig.savefig(basepath + f"graphics/atmospheric_river/era5_case_{my_id}.png", dpi=300, bbox_inches="tight")
            plt.close(fig)
            
    
    for my_case in ewb_cases:
        print(my_case.case_id_number)
        my_id = my_case.case_id_number
        if args.plot_era_separately:
            row_length = 5
        else:
            row_length = 4

        # make a subplot for each model and ensure it is a cartopy plot
        fig, axs = plt.subplots(row_length, len(lead_times_to_plot) + 1, figsize=(18, 2 * len(lead_times_to_plot)), 
            subplot_kw={'projection': ccrs.PlateCarree()})

        if not args.plot_era_separately:
            if (my_id, "ivt") in era5_graphics:
                era5_ivt, era5_ar_mask = ar_plot_utils.select_ivt_and_maks_era5(era5_graphics[my_id, "ivt"])
                title = "ERA5"
                ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=era5_ivt, ar_mask=era5_ar_mask, 
                    title=title, ax=axs[0, len(lead_times_to_plot)], colorbar=False, show_axes=False)
                for i in range(1, row_length):
                    axs[i, len(lead_times_to_plot)].set_visible(False)
            else:
                print(f"Skipping ERA5 for case {my_id}: missing ivt or ar mask data in graphics object")

        if (my_id, "ivt") in hres_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                hres_ivt, hres_ar_mask = ar_plot_utils.select_ivt_and_maks(hres_graphics[my_id, "ivt"], lead_time_hours)
                if hres_ivt is not None and hres_ar_mask is not None:
                    title = f"{lead_time_hours} hours"
                    if (i == 0):
                        left_label = "HRES"
                    else:
                        left_label = None
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=hres_ivt, ar_mask=hres_ar_mask, 
                        title=title, ax=axs[0, i], colorbar=False, left_label=left_label)
                else:
                    print(f"Skipping HRES for case {my_id}: missing ivt or ar mask data in graphics object")
            
        else:
            print(f"Skipping HRES for case {my_id}: missing cbss or pph data in graphics object")
        

        if (my_id, "ivt") in bb_graphcast_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                gc_ivt, gc_ar_mask = ar_plot_utils.select_ivt_and_maks(bb_graphcast_graphics[my_id, "ivt"], lead_time_hours)
                if gc_ivt is not None and gc_ar_mask is not None:
                    title = f"{lead_time_hours} hours"
                    if (i == 0):
                        left_label = "Graphcast"
                    else:
                        left_label = None
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=gc_ivt, ar_mask=gc_ar_mask, 
                        ax=axs[1, i], colorbar=False, left_label=left_label)   
                else:
                    print(f"Skipping GraphCast for case {my_id}: missing ivt or ar mask data in graphics object")
        else:
            print(f"Skipping GraphCast for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_pangu_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                pang_ivt, pang_ar_mask = ar_plot_utils.select_ivt_and_maks(bb_pangu_graphics[my_id, "ivt"], lead_time_hours)
                if pang_ivt is not None and pang_ar_mask is not None:
                    # title = f"{lead_time_hours} hours"
                    if (i == 0):
                        left_label = "Pangu"
                    else:
                        left_label = None
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=pang_ivt, ar_mask=pang_ar_mask, 
                        ax=axs[2, i], colorbar=False, left_label=left_label)
                else:
                    print(f"Skipping Pangu for case {my_id}: missing ivt or ar mask data in graphics object")
        else:
            print(f"Skipping Pangu for case {my_id}: missing ivt data in graphics object")
        
        if (my_id, "ivt") in bb_aifs_graphics:
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                aifs_ivt, aifs_ar_mask = ar_plot_utils.select_ivt_and_maks(bb_aifs_graphics[my_id, "ivt"], lead_time_hours)
                if aifs_ivt is not None and aifs_ar_mask is not None:
                    title = f"{lead_time_hours} hours"
                    if (i == 0):
                        left_label = "AIFS"
                    else:
                        left_label = None
                    ar_plot_utils.plot_ar_mask_single_timestep(ivt_data=aifs_ivt, ar_mask=aifs_ar_mask, 
                        ax=axs[3, i], colorbar=False, left_label=left_label)
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
        pos0 = axs[row_length - 1, 0].get_position(fig)
        pos3 = axs[row_length - 1, len(lead_times_to_plot)].get_position(fig)
        # Create axes below row 5 that spans all 4 columns
        # Position it just below row 5, using a small height
        cbar_y = pos0.y0 - pos0.height * 0.3  # Position below bottom row
        cbar_height = pos0.height * 0.15  # Height for colorbar
        cbar_ax = fig.add_axes([pos0.x0, cbar_y, pos3.x1 - pos0.x0, cbar_height])

        # Add horizontal colorbar below bottom row
        cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
        cbar.set_label(r"Integrated Vapor Transport (kg m$^{-1}$ s$^{-1}$)", size=32)
        cbar.ax.tick_params(labelsize=24)

        # make the overall title and save it        
        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32)
        fig.savefig(basepath + f"graphics/atmospheric_river/ar_case_{my_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)