
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap, ScalarMappable
from matplotlib.lines import Line2D

from src.plots.plotting_utils import (
    generate_plot_extent_bounds,
    setup_colormap_and_levels,
)


def plot_tc_tracks(track_data, analysis_track_data, model_name):
    """Plot the tropical cyclone tracks for a given model.

    Args:
        track_data: The track data for the forecast.
        analysis_track_data: The analysis target track.
        model_name: The name of the model.

    Returns:
        Tuple of (figure, axis) objects.
    """
    valid_init_times_ = []
    valid_groups_ = []

    assert "init_time" in track_data.dims, ("init_time must be a dimension in the track "
    "data. Use utils.convert_valid_time_to_init_time to convert the valid_time "
    "dimension to an init_time dimension before plotting.")

    # Group and iterate over init times
    for init_time, group in track_data.groupby("init_time"):
        # Extract valid storm positions for this init_time
        lats = group.latitude
        lons = group.longitude

        # Flatten the arrays to 1D for plotting
        lats_flat = lats.values.flatten()
        lons_flat = lons.values.flatten()

        # Remove NaN values
        valid_coords = ~(np.isnan(lats_flat) | np.isnan(lons_flat))
        lats_valid = lats_flat[valid_coords]
        lons_valid = lons_flat[valid_coords]

        # Only include if there are valid points
        if len(lats_valid) > 0:
            valid_init_times_.append(init_time)
            valid_groups_.append((lats_valid, lons_valid))


    n_times_ = len(valid_init_times_)

    cmap, _ = setup_colormap_and_levels(np.linspace(0.2, 0.8, n_times_))
    colors_ = cmap(np.linspace(0.2, 1 - 1/(n_times_+1), n_times_))


    fig = plt.figure(figsize=(14, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Add map features
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)
    ax.add_feature(cfeature.LAND, color="lightgray", alpha=0.7)
    ax.add_feature(cfeature.STATES, linewidth=0.3, alpha=0.7)

    # Add gridlines
    gl = ax.gridlines(
        crs=ccrs.PlateCarree(),
        draw_labels=True,
        linewidth=0.5,
        color="gray",
        alpha=0.5,
        linestyle="--",
    )
    gl.top_labels = False
    gl.right_labels = False

    for i, (lats_valid, lons_valid) in enumerate(valid_groups_):
        ax.plot(
            lons_valid,
            lats_valid,
            "-",
            color=colors_[i],
            alpha=0.8,
            markersize=6,
            linewidth=3,
            transform=ccrs.PlateCarree(),
        )
    # Plot IBTRACS data
    ax.plot(
        analysis_track_data.longitude,
        analysis_track_data.latitude,
        "-ok",
        transform=ccrs.PlateCarree(),
        linewidth=2,
        markeredgecolor='white',
    )

    init_datetimes_ = [pd.to_datetime(str(t)) for t in valid_init_times_]

    #  colorbar
    cmap_ = ListedColormap(colors_)
    norm_ = plt.Normalize(vmin=-0.5, vmax=n_times_ - 0.5)
    sm_ = ScalarMappable(cmap=cmap_, norm=norm_)
    sm_.set_array([])


    # Create custom legend elements
    _line = Line2D(
        [0],
        [0],
        color="gray",
        linestyle="-",
        markersize=8,
        linewidth=4,
        label=f"{model_name} Forecast",
    )
    ibtracs_line = Line2D(
        [0],
        [0],
        color="black",
        marker="o",
        linestyle="-",
        markersize=8,
        linewidth=4,
        label="IBTrACS Data",
    )


    # Add legend
    ax.legend(
        handles=[_line, ibtracs_line],
        loc="upper right",
        fontsize=12,
        frameon=True,
        fancybox=True,
        shadow=True,
    )


    # Set extent to focus on the storm region
    extent = generate_plot_extent_bounds(
        min_lon=analysis_track_data.longitude.min(), 
        max_lon=analysis_track_data.longitude.max(), 
        min_lat=analysis_track_data.latitude.min(), 
        max_lat=analysis_track_data.latitude.max(), 
        zoom='auto', 
        aspect_ratio=(16,9)
        )
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    # Get the position from bottom row subplots to position colorbar below them
    pos0 = ax.get_position()

    # Calculate the position and height of the colorbar
    cbar_y = pos0.y0 - pos0.height * 0.15  # Position below bottom row
    cbar_height = pos0.height * 0.05  # Height for colorbar
    cbar_ax = fig.add_axes([pos0.x0, cbar_y, pos0.x1 - pos0.x0, cbar_height])

    # Add horizontal colorbar below bottom
    cbar = fig.colorbar(sm_, cax=cbar_ax, orientation='horizontal')
    cbar.set_label("Initialization Time", fontsize=11, fontweight="bold", labelpad=8)
    tick_positions_ = np.arange(n_times_)
    tick_labels_ = [dt.strftime("%m/%d\n%HZ") for dt in init_datetimes_]
    cbar.set_ticks(tick_positions_)
    cbar.set_ticklabels(tick_labels_, fontsize=9)
    cbar.ax.tick_params(labelsize=9, pad=2)
    cbar.outline.set_linewidth(0.5)
    cbar.outline.set_edgecolor("gray")
    ax.set_title(
        "$\\mathbf{TC\ %s}$\nForecast Tracks" % np.unique(analysis_track_data['tc_name'])[0],
        loc='left',
        fontsize=24,
    )

    return fig, ax