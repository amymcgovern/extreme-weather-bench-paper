# setup all the imports
from typing import Optional

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from cartopy.mpl.gridliner import LatitudeFormatter, LongitudeFormatter
from extremeweatherbench import cases, utils
from matplotlib.patches import Patch
from mpl_toolkits.axes_grid1 import make_axes_locatable


def celsius_colormap_and_normalize() -> tuple[mcolors.Colormap, mcolors.Normalize]:
    """Gets the colormap and normalization for 2m temperature.

    Uses a custom colormap for temperature in Celsius.

    Returns:
        A tuple (cmap, norm) for plotting.
    """
    lo_colors = [
        "#E4C7F4",
        "#E53885",
        "#C17CBE",
        "#694396",
        "#CBCCE9",
        "#6361BD",
        "#77FBFE",
    ]
    hi_colors = [
        "#8CE9B0",
        "#479F31",
        "#F0F988",
        "#AD311B",
        "#ECB9F1",
        "#7F266F",
    ]
    colors = lo_colors + hi_colors

    # Calculate the position where we want the 0C jump
    lo = -67.8
    hi = 54.4
    threshold = 0
    threshold_pos = (threshold - lo) / (hi - lo)  # normalize 0°C position to [0,1]

    # Create positions for colors with a small gap around zero_pos
    positions = np.concatenate(
        [
            np.linspace(0, threshold_pos - 0.02, len(lo_colors)),  # Colors up to white
            # [threshold_pos],  # White position
            np.linspace(threshold_pos + 0.02, 1, len(hi_colors)),  # Colors after white
        ]
    )

    return mcolors.LinearSegmentedColormap.from_list(
        "temp_colormap", list(zip(positions, colors))
    ), mcolors.Normalize(vmin=lo, vmax=hi)


def generate_heatwave_dataset(
    era5: xr.Dataset,
    climatology: xr.Dataset,
    single_case: cases.IndividualCase,
):
    """Calculate times where regional avg temp is above climatology.

    Args:
        era5: ERA5 dataset containing 2m_temperature
        climatology: BB climatology containing
        surface_temperature_85th_percentile
        single_case: cases.IndividualCase object with metadata
    """
    era5_case = era5[["2m_temperature"]].sel(
        valid_time=slice(single_case.start_date, single_case.end_date)
    )
    subset_climatology = utils.convert_day_yearofday_to_time(
        climatology, np.unique(era5_case.valid_time.dt.year.values)[0]
    )
    merged_dataset = xr.merge(
        [
            subset_climatology.rename(
                {"2m_temperature": "surface_temperature_85th_percentile"}
            ),
            era5_case,
        ],
        join="inner",
    )
    if (
        single_case.location.longitude_min < 0
        or single_case.location.longitude_min > 180
    ) and (
        single_case.location.longitude_max > 0
        and single_case.location.longitude_max < 180
    ):
        merged_dataset = utils.convert_longitude_to_180(merged_dataset)
    merged_dataset = merged_dataset.sel(
        latitude=slice(
            single_case.location.latitude_max, single_case.location.latitude_min
        ),
        longitude=slice(
            single_case.location.longitude_min, single_case.location.longitude_max
        ),
    )
    return merged_dataset


def plot_heatwave_case(
    heatwave_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
):
    """Plot the heatwave case
    Args:
        heatwave_dataset: contains 2m_temperature,
        surface_temperature_85th_percentile, time, latitude, longitude
        single_case: cases.IndividualCase object with metadata
        filename: Optional[str] = if not None, the plots will be saved to this filename
        ax: Optional[plt.Axes] = if not None, the axis to plot the case on
    """
    
    time_based_heatwave_dataset = heatwave_dataset.mean(["latitude", "longitude"])
    
    # Plot 1: Min timestep of the heatwave event
    # if the axis is not provided, create a new figure and axis
    if ax is None:
        fig1 = plt.figure(figsize=(12, 6))
        ax1 = plt.axes(projection=ccrs.PlateCarree())
    else:
        # this will be a subplot within a larger figure so use the existing axis
        ax1 = ax
        fig1 = ax.get_figure()

    # Select the timestep with the maximum spatially averaged temp
    subset_timestep = time_based_heatwave_dataset["valid_time"][
        time_based_heatwave_dataset["2m_temperature"].argmax()
    ]
    # Mask places where temp >= 85th percentile climatology
    temp_data = heatwave_dataset["2m_temperature"] - 273.15
    climatology_data = heatwave_dataset["surface_temperature_85th_percentile"] - 273.15

    # Create mask for values where temp > climatology
    # (heatwave condition)
    mask = temp_data > climatology_data

    # Apply mask to temperature data
    masked_temp = temp_data.where(mask)
    cmap, norm = celsius_colormap_and_normalize()
    im = masked_temp.sel(valid_time=subset_timestep).plot(
        ax=ax1,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=norm,
        add_colorbar=False,
    )
    (
        temp_data.sel(valid_time=subset_timestep).plot.contour(
            ax=ax1,
            levels=[0],
            colors="r",
            linewidths=0.75,
            ls=":",
            transform=ccrs.PlateCarree(),
        )
    )
    # Add coastlines and gridlines
    ax1.coastlines()
    ax1.add_feature(cfeature.BORDERS, linestyle=":")
    ax1.add_feature(cfeature.LAND, edgecolor="black")
    ax1.add_feature(cfeature.LAKES, edgecolor="black")
    ax1.add_feature(
        cfeature.RIVERS, edgecolor=[0.59375, 0.71484375, 0.8828125], alpha=0.5
    )
    ax1.add_feature(cfeature.STATES, edgecolor="grey")
    # Add gridlines
    gl = ax1.gridlines(draw_labels=True, alpha=0.25)
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()
    gl.xlabel_style = {"size": 12, "color": "k"}
    gl.ylabel_style = {"size": 12, "color": "k"}
    ax1.set_title("")  # clears the default xarray title
    time_str = (
        heatwave_dataset["valid_time"]
        .sel(valid_time=subset_timestep)
        .dt.strftime("%Y-%m-%d %Hz")
        .values
    )
    ax1.set_title(
        f"Temperature Where > 85th Percentile Climatology\n"
        f"{single_case.title}, Case ID {single_case.case_id_number}\n"
        f"{time_str}",
        loc="left",
    )
    # Add the location coordinate as a dot on the map
    ax1.tick_params(axis="y", which="major", labelsize=12)
    # Create a colorbar with the same height as the plot
    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.1, axes_class=plt.Axes)
    cbar = fig1.colorbar(im, cax=cax, label="Temp > 85th Percentile (C)")
    cbar.set_label("Temperature (C)", size=14)
    cbar.ax.tick_params(labelsize=12)

    # if ax is None:
    #     plt.tight_layout()
    #     if filename is not None:
    #         plt.savefig(filename, transparent=True)
    #     plt.show()


def plot_heatwave_time_series(
    heatwave_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
):
    time_based_heatwave_dataset = heatwave_dataset.mean(["latitude", "longitude"])

    """Plot the heatwave time series."""
    # Plot 2: Average regional temperature time series
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    lss = ["-.", "-"]
    lc = ["k", "tab:red"]
    lws = [0.75, 1.5]
    for i, variable in enumerate(time_based_heatwave_dataset):
        (time_based_heatwave_dataset[variable] - 273.15).plot(
            ax=ax2, label=variable, lw=lws[i], ls=lss[i], c=lc[i]
        )
    ax2.legend(fontsize=12)
    mask = (
        time_based_heatwave_dataset["2m_temperature"]
        > time_based_heatwave_dataset["surface_temperature_85th_percentile"]
    )
    start = None
    for i, val in enumerate(mask.values):
        if val and start is None:
            start = time_based_heatwave_dataset.valid_time[i].values
        elif not val and start is not None:
            ax2.axvspan(
                start,
                time_based_heatwave_dataset.valid_time[i].values,
                color="red",
                alpha=0.1,
            )
            start = None
    if start is not None:
        ax2.axvspan(
            start,
            time_based_heatwave_dataset.valid_time[-1].values,
            color="red",
            alpha=0.1,
        )
    ax2.set_title("")
    ax2.set_title(
        "Spatially Averaged Heatwave Event vs 85th Percentile Climatology",
        fontsize=14,
        loc="left",
    )
    ax2.set_ylabel("Temperature (C)", fontsize=12)
    ax2.set_xlabel("valid_time", fontsize=12)
    ax2.tick_params(axis="x", labelsize=12)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax2.xaxis.set_tick_params(
        rotation=45,
        labelsize=10,
        pad=0.0001,
    )
    ax2.tick_params(axis="y", labelsize=12)

    # Create legend handles including the axvspan
    legend_elements = [
        plt.Line2D(
            [0],
            [0],
            color="k",
            linestyle="-.",
            linewidth=0.75,
            label="2m Temperature, 85th Percentile",
        ),
        plt.Line2D(
            [0],
            [0],
            color="tab:red",
            linestyle="-",
            linewidth=1.5,
            label="2m Temperature",
        ),
        Patch(facecolor="red", alpha=0.1, label="Above 85th Percentile"),
    ]
    ax2.legend(handles=legend_elements, fontsize=12)

    ax2.tick_params(axis="y", which="major", labelsize=12)
    plt.tight_layout()
    if filename is not None:
        plt.savefig(filename, transparent=True)

    plt.show()


def generate_heatwave_plots(
    heatwave_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
):
    """Plot max timestep of heatwave event and avg regional temp
    time series on separate plots.

    Args:
        heatwave_dataset: contains 2m_temperature,
        surface_temperature_85th_percentile, time, latitude, longitude
        single_case: cases.IndividualCase object with metadata
        filename: Optional[str] = if not None, the plots will be saved to this filename
    """
    plot_heatwave_case(heatwave_dataset, single_case, filename)
    plot_heatwave_time_series(heatwave_dataset, single_case, filename)


def generate_freeze_dataset(
    era5: xr.Dataset,
    climatology: xr.Dataset,
    single_case: cases.IndividualCase,
):
    """Calculate times where regional avg temp is below climatology.

    Args:
        era5: ERA5 dataset containing 2m_temperature
        climatology: BB climatology containing
        surface_temperature_15th_percentile
        single_case: cases.IndividualCase object with metadata
    """
    era5_case = era5[["2m_temperature"]].sel(
        valid_time=slice(single_case.start_date, single_case.end_date)
    )
    subset_climatology = utils.convert_day_yearofday_to_time(
        climatology, np.unique(era5_case.valid_time.dt.year.values)[0]
    )
    merged_dataset = xr.merge(
        [
            subset_climatology.rename(
                {"2m_temperature": "surface_temperature_15th_percentile"}
            ),
            era5_case,
        ],
        join="inner",
    )
    if (
        single_case.location.longitude_min < 0
        or single_case.location.longitude_min > 180
    ) and (
        single_case.location.longitude_max > 0
        and single_case.location.longitude_max < 180
    ):
        merged_dataset = utils.convert_longitude_to_180(merged_dataset)
    merged_dataset = merged_dataset.sel(
        latitude=slice(
            single_case.location.latitude_max, single_case.location.latitude_min
        ),
        longitude=slice(
            single_case.location.longitude_min, single_case.location.longitude_max
        ),
    )
    return merged_dataset


def plot_freeze_case(
    freeze_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
):
    """Plot the freeze case
    Args:
        freeze_dataset: contains 2m_temperature,
        surface_temperature_15th_percentile, time, latitude, longitude
        single_case: cases.IndividualCase object with metadata
        filename: Optional[str] = if not None, the plots will be saved to this filename
        ax: Optional[plt.Axes] = if not None, the axis to plot the case on
    """
    # if not provided, create a new figure and axis
    if ax is None:
        fig1 = plt.figure(figsize=(12, 6))
        ax1 = plt.axes(projection=ccrs.PlateCarree())
    else:
        # this will be a subplot within a larger figure so use the existing axis
        ax1 = ax
        fig1 = ax.get_figure()

    time_based_freeze_dataset = freeze_dataset.mean(["latitude", "longitude"])
    # Select the timestep with the maximum spatially averaged temp
    subset_timestep = time_based_freeze_dataset["valid_time"][
        time_based_freeze_dataset["2m_temperature"].argmin()
    ]
    # Mask places where temp >= 15th percentile climatology
    temp_data = freeze_dataset["2m_temperature"] - 273.15
    climatology_data = freeze_dataset["surface_temperature_15th_percentile"] - 273.15

    # Create mask for values where temp < climatology
    # (freeze condition)
    mask = temp_data < climatology_data

    # Apply mask to temperature data
    masked_temp = temp_data.where(mask)
    cmap, norm = celsius_colormap_and_normalize()
    im = masked_temp.sel(valid_time=subset_timestep).plot(
        ax=ax1,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=norm,
        add_colorbar=False,
    )
    (
        temp_data.sel(valid_time=subset_timestep).plot.contour(
            ax=ax1,
            levels=[0],
            colors="r",
            linewidths=0.75,
            ls=":",
            transform=ccrs.PlateCarree(),
        )
    )
    # Add coastlines and gridlines
    ax1.coastlines()
    ax1.add_feature(cfeature.BORDERS, linestyle=":")
    ax1.add_feature(cfeature.LAND, edgecolor="black")
    ax1.add_feature(cfeature.LAKES, edgecolor="black")
    ax1.add_feature(
        cfeature.RIVERS, edgecolor=[0.59375, 0.71484375, 0.8828125], alpha=0.5
    )
    ax1.add_feature(cfeature.STATES, edgecolor="grey")
    # Add gridlines
    gl = ax1.gridlines(draw_labels=True, alpha=0.25)
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()
    gl.xlabel_style = {"size": 12, "color": "k"}
    gl.ylabel_style = {"size": 12, "color": "k"}
    ax1.set_title("")  # clears the default xarray title
    time_str = (
        freeze_dataset["valid_time"]
        .sel(valid_time=subset_timestep)
        .dt.strftime("%Y-%m-%d %Hz")
        .values
    )
    ax1.set_title(
        f"Temperature Where < 15th Percentile Climatology\n"
        f"{single_case.title}, Case ID {single_case.case_id_number}\n"
        f"{time_str}",
        loc="left",
    )
    # Add the location coordinate as a dot on the map
    ax1.tick_params(axis="y", which="major", labelsize=12)
    # Create a colorbar with the same height as the plot
    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.1, axes_class=plt.Axes)
    cbar = fig1.colorbar(im, cax=cax, label="Temp < 15th Percentile (C)")
    cbar.set_label("Temperature (C)", size=14)
    cbar.ax.tick_params(labelsize=12)

    # if ax is None:
    #     plt.tight_layout()
    #     if filename is not None:
    #         plt.savefig(filename, transparent=True)
    #     plt.show()


def plot_freeze_time_series(
    freeze_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
):
    """Plot the freeze time series"""
    time_based_freeze_dataset = freeze_dataset.mean(["latitude", "longitude"])

    # Plot 2: Average regional temperature time series
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    lss = ["-.", "-"]
    lc = ["k", "tab:red"]
    lws = [0.75, 1.5]
    for i, variable in enumerate(time_based_freeze_dataset):
        (time_based_freeze_dataset[variable] - 273.15).plot(
            ax=ax2, label=variable, lw=lws[i], ls=lss[i], c=lc[i]
        )
    ax2.legend(fontsize=12)
    mask = (
        time_based_freeze_dataset["2m_temperature"]
        < time_based_freeze_dataset["surface_temperature_15th_percentile"]
    )
    start = None
    for i, val in enumerate(mask.values):
        if val and start is None:
            start = time_based_freeze_dataset.valid_time[i].values
        elif not val and start is not None:
            ax2.axvspan(
                start,
                time_based_freeze_dataset.valid_time[i].values,
                color="red",
                alpha=0.1,
            )
            start = None
    if start is not None:
        ax2.axvspan(
            start,
            time_based_freeze_dataset.valid_time[-1].values,
            color="red",
            alpha=0.1,
        )
    ax2.set_title("")
    ax2.set_title(
        "Spatially Averaged Freeze Event vs 15th Percentile Climatology",
        fontsize=14,
        loc="left",
    )
    ax2.set_ylabel("Temperature (C)", fontsize=12)
    ax2.set_xlabel("valid_time", fontsize=12)
    ax2.tick_params(axis="x", labelsize=12)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax2.xaxis.set_tick_params(
        rotation=45,
        labelsize=10,
        pad=0.0001,
    )
    ax2.tick_params(axis="y", labelsize=12)

    legend_elements = [
        plt.Line2D(
            [0],
            [0],
            color="k",
            linestyle="-.",
            linewidth=0.75,
            label="2m Temperature, 15th Percentile",
        ),
        plt.Line2D(
            [0],
            [0],
            color="tab:red",
            linestyle="-",
            linewidth=1.5,
            label="2m Temperature",
        ),
        Patch(facecolor="red", alpha=0.1, label="Below 15th Percentile"),
    ]
    ax2.legend(handles=legend_elements, fontsize=12)

    ax2.tick_params(axis="y", which="major", labelsize=12)
    plt.tight_layout()
    if filename is not None:
        plt.savefig(filename, transparent=True)
    plt.show()


def generate_freeze_plots(
    freeze_dataset: xr.Dataset,
    single_case: cases.IndividualCase,
    filename: Optional[str] = None,
):
    """Plot max timestep of freeze event and avg regional temp
    time series on separate plots.

    Args:
        freeze_dataset: contains 2m_temperature,
        surface_temperature_15th_percentile, time, latitude, longitude
        single_case: cases.IndividualCase object with metadata
        filename: Optional[str] = if not None, the plots will be saved to this filename
    """

    plot_freeze_case(freeze_dataset, single_case, filename)
    plot_freeze_time_series(freeze_dataset, single_case, filename)
