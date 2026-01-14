"""General plotting utilities for weather data visualization.

This module provides common plotting functions that can be reused across
different types of weather plots including atmospheric rivers, severe
convection, tropical cyclones, etc.
"""

import logging
from typing import Dict, List, Optional, Tuple

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.mpl.ticker
import matplotlib.colors as colors

# setup all the imports
import matplotlib.colors as mcolors
import matplotlib.font_manager as fm
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shapely
from cartopy.mpl.gridliner import LatitudeFormatter, LongitudeFormatter
from extremeweatherbench import cases, utils
from matplotlib.patches import Patch
from shapely.geometry import Polygon

logger = logging.getLogger(__name__)


lsr_colors = {
    "tornado": "black",
    "hail": "blue",
    "wind": "red",
}


def convert_longitude_for_plotting(lon_data: np.ndarray) -> np.ndarray:
    """Convert longitude from 0-360 to -180-180 for plotting.

    Args:
        lon_data: Longitude array in 0-360 format.

    Returns:
        Longitude array in -180-180 format.
    """
    return np.where(lon_data > 180, lon_data - 360, lon_data)


def convert_bbox_longitude(bbox: Dict[str, float]) -> Tuple[float, float]:
    """Convert bounding box longitude from 0-360 to -180-180.

    Args:
        bbox: Bounding box dictionary with longitude_min/max keys.

    Returns:
        Tuple of (lon_min, lon_max) in -180-180 format.
    """
    lon_min = (
        bbox["longitude_min"] - 360
        if bbox["longitude_min"] > 180
        else bbox["longitude_min"]
    )
    lon_max = (
        bbox["longitude_max"] - 360
        if bbox["longitude_max"] > 180
        else bbox["longitude_max"]
    )
    return lon_min, lon_max


def add_geographic_features(
    ax,
    alpha: float = 0.7,
    coastline_width: float = 0.8,
    border_width: float = 0.5,
    state_width: float = 0.5,
    water_alpha: float = 0.3,
    include_land_ocean: bool = False,
    land_ocean_alpha: float = 0.1,
) -> None:
    """Add standard geographic features to a cartopy axis.

    Args:
        ax: Cartopy axis to add features to.
        alpha: Transparency for state boundaries.
        coastline_width: Line width for coastlines.
        border_width: Line width for country borders.
        state_width: Line width for state boundaries.
        water_alpha: Transparency for lakes and rivers.
        include_land_ocean: Whether to add land/ocean background.
        land_ocean_alpha: Transparency for land/ocean background.
    """
    ax.add_feature(cfeature.COASTLINE, linewidth=coastline_width)
    ax.add_feature(cfeature.BORDERS, linewidth=border_width)
    ax.add_feature(cfeature.STATES, linewidth=state_width, alpha=alpha)
    ax.add_feature(cfeature.LAKES, alpha=water_alpha)
    ax.add_feature(cfeature.RIVERS, alpha=water_alpha)

    if include_land_ocean:
        ax.add_feature(cfeature.LAND, alpha=land_ocean_alpha)
        ax.add_feature(cfeature.OCEAN, alpha=land_ocean_alpha)


def setup_gridlines(
    ax,
    show_left_labels: bool = True,
    show_bottom_labels: bool = True,
    show_top_labels: bool = False,
    show_right_labels: bool = False,
    alpha: float = 0.5,
    linestyle: str = "--",
    number_format: str = "02.1f",
) -> None:
    """Setup gridlines with custom formatting.

    Args:
        ax: Cartopy axis to add gridlines to.
        show_left_labels: Whether to show labels on the left side.
        show_bottom_labels: Whether to show labels on the bottom.
        show_top_labels: Whether to show labels on the top.
        show_right_labels: Whether to show labels on the right.
        alpha: Transparency of gridlines.
        linestyle: Style of gridlines.
        number_format: Format string for coordinate labels.
    """
    gl = ax.gridlines(
        draw_labels=True,
        alpha=alpha,
        linestyle=linestyle,
        x_inline=False,
        y_inline=False,
        crs=ccrs.PlateCarree(),
    )
    gl.top_labels = show_top_labels
    gl.right_labels = show_right_labels
    gl.left_labels = show_left_labels
    gl.bottom_labels = show_bottom_labels
    gl.xformatter = cartopy.mpl.ticker.LongitudeFormatter(
        dms=False, number_format=number_format
    )
    gl.xlabel_style = {"size": 14}
    gl.ylabel_style = {"size": 14}
    gl.yformatter = cartopy.mpl.ticker.LatitudeFormatter(number_format=number_format)


def create_custom_colormap_with_transparent_low(
    base_cmap_name: str, levels: np.ndarray, transparent_below_index: int = 0
) -> Tuple[colors.ListedColormap, colors.BoundaryNorm]:
    """Create a custom colormap with transparent values below a threshold.

    Args:
        base_cmap_name: Name of the base matplotlib colormap.
        levels: Array of contour levels.
        transparent_below_index: Index below which values are transparent.

    Returns:
        Tuple of (custom_colormap, boundary_normalization).
    """
    base_cmap = plt.cm.get_cmap(base_cmap_name)
    colors_list = base_cmap(np.linspace(0, 1, len(levels) - 1))

    # Set colors below threshold to transparent
    for i in range(transparent_below_index + 1):
        if i < len(colors_list):
            colors_list[i] = [1, 1, 1, 0]  # Transparent white

    cmap_custom = colors.ListedColormap(colors_list)
    norm = colors.BoundaryNorm(levels, cmap_custom.N)
    return cmap_custom, norm


def setup_figure_with_colorbar_layout(
    n_cols: int,
    figsize_per_panel: Tuple[float, float] = (4, 6),
    include_legend: bool = True,
    include_colorbar: bool = True,
) -> Tuple[plt.Figure, plt.GridSpec]:
    """Setup figure with proper layout for multi-panel plots with colorbar.

    Args:
        n_cols: Number of columns (panels).
        figsize_per_panel: Size per panel (width, height).
        include_legend: Whether to include space for legend.
        include_colorbar: Whether to include space for colorbar.

    Returns:
        Tuple of (figure, gridspec) for creating subplots.
    """
    fig_width = figsize_per_panel[0] * n_cols
    fig_height = figsize_per_panel[1]

    # Calculate height ratios
    height_ratios = [1]  # Main plots
    if include_legend:
        height_ratios.append(0.06)  # Legend row
        fig_height += 0.4
    if include_colorbar:
        height_ratios.append(0.04)  # Colorbar row
        fig_height += 0.3

    fig = plt.figure(figsize=(fig_width, fig_height))

    gs = fig.add_gridspec(
        len(height_ratios),
        n_cols,
        height_ratios=height_ratios,
        hspace=0.02,
        top=0.92,
        bottom=0.12,
    )

    return fig, gs


def add_horizontal_legend(
    fig: plt.Figure,
    gs: plt.GridSpec,
    legend_elements: List[plt.Line2D],
    row_index: int,
    fontsize: int = 10,
) -> None:
    """Add a horizontal legend spanning all columns.

    Args:
        fig: Figure to add legend to.
        gs: GridSpec for layout.
        legend_elements: List of legend elements.
        row_index: Row index in the gridspec for the legend.
        fontsize: Font size for legend text.
    """
    if legend_elements:
        legend_ax = fig.add_subplot(gs[row_index, :])
        legend_ax.axis("off")
        legend_ax.legend(
            handles=legend_elements,
            loc="center",
            ncol=len(legend_elements),
            frameon=False,
            fontsize=fontsize,
        )

def setup_colormap_and_levels(bounds: np.ndarray):
    """Setup colormap and normalization for AR plotting.

    Args:
        bounds: Array of bounds for the colormap.

    Returns:
        Tuple of (colormap, normalization) based on bounds.
    """
    # Create custom colormap from original code
    cmap_colors = [
        "#ffffff",
        "#bde6fa",
        "#7bbae7",
        "#4892bd",
        "#49ae62",
        "#a7d051",
        "#f9d251",
        "#f7792f",
        "#e43d28",
        "#c11b24",
        "#921318",
    ]
    cmap = mcolors.LinearSegmentedColormap.from_list("custom_cubehelix", cmap_colors)
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    return cmap, norm


def add_horizontal_colorbar(
    fig: plt.Figure,
    gs: plt.GridSpec,
    mappable,
    row_index: int,
    label: str,
    extend: str = "max",
    fontsize: int = 12,
):
    """Add a horizontal colorbar spanning all columns.

    Args:
        fig: Figure to add colorbar to.
        gs: GridSpec for layout.
        mappable: Mappable object (e.g., contour plot result).
        row_index: Row index in the gridspec for the colorbar.
        label: Label for the colorbar.
        extend: Colorbar extension ('max', 'min', 'both', 'neither').
        fontsize: Font size for colorbar label.

    Returns:
        The created colorbar object.
    """
    cbar_ax = fig.add_subplot(gs[row_index, :])
    cbar = fig.colorbar(mappable, cax=cbar_ax, orientation="horizontal", extend=extend)
    cbar.set_label(label, fontsize=fontsize)
    return cbar


def set_axis_extent_from_bbox(
    ax, bbox: Dict[str, float], crs: ccrs.Projection = ccrs.PlateCarree()
) -> None:
    """Set axis extent from bounding box, handling longitude conversion.

    Args:
        ax: Cartopy axis to set extent for.
        bbox: Bounding box with latitude_min/max, longitude_min/max keys.
        crs: Coordinate reference system for the extent.
    """
    lon_min, lon_max = convert_bbox_longitude(bbox)
    ax.set_extent(
        [lon_min, lon_max, bbox["latitude_min"], bbox["latitude_max"]],
        crs=crs,
    )

def build_mercator_bounds(min_lon, max_lon, min_lat, max_lat, zoom_val, aspect_ratio, out_crs=ccrs.Mercator()):
    """Calculate the bounding box edges of a mercator projection for a given zoom level and aspect ratio.
    
    Args:
        min_lon, max_lon, min_lat, max_lat: Bounding box edges
        zoom_val: Zoom level
        aspect_ratio: Aspect ratio x/y
        out_crs: Out crs for extent values.

    Returns:
        tuple: (lon_min, lon_max, lat_min, lat_max)
    """
    mercator_crs = ccrs.Mercator()
    center_lon = (min_lon + max_lon)/2
    center_lat = (min_lat + max_lat)/2
    # Define zoom scaling
    zoom_coefficient = 2
    # Calculate minimum longitude (min_lon) and maximum longitude (max_lon)
    min_lon, max_lon = (
        center_lon - (zoom_coefficient * zoom_val),
        center_lon + (zoom_coefficient * zoom_val),
    )
            
    # Transform map center to specified crs (default to Mercator)
    c_mercator = mercator_crs.transform_point(center_lon, center_lat, src_crs=ccrs.Mercator())

    # Transform minimum longitude and maximum longitude to specified crs (default to
    # Mercator)
    min_lon = mercator_crs.transform_point(
        min_lon, center_lat, src_crs=ccrs.Mercator()
    )[0]
    max_lon = mercator_crs.transform_point(
        max_lon, center_lat, src_crs=ccrs.Mercator()
    )[0]

    # Our goal is to calculate minimum latitude (min_lat) and maximum latitude (max_lat)
    # using center point and distance between min_lon and max_lon
    # To achieve this we will use formula [(lon_distance/lat_distance) =
    # (aspect_ratio[0]/aspect_ratio[1])]
    # Calculate distance between min_lon and max_lon
    lon_distance = (max_lon) - (min_lon)

    # To calculate lat_distance, we will proceed accordingly
    lat_distance = lon_distance * aspect_ratio[1] / aspect_ratio[0]

    # Now calculate max_lat and min_lon by adding/subtracting half of the distance from
    # center latitude
    max_lat = c_mercator[1] + lat_distance / 2
    min_lat = c_mercator[1] - lat_distance / 2

    # We can return our result in any format (eg. in Mercator coordinates or in degrees)
    if out_crs != ccrs.Mercator():
        min_lon, min_lat = out_crs.transform_point(min_lon, min_lat, src_crs=out_crs)
        max_lon, max_lat = out_crs.transform_point(max_lon, max_lat, src_crs=out_crs)

    return min_lon, max_lon, min_lat, max_lat

def generate_plot_extent_bounds(min_lon, max_lon, min_lat, max_lat, zoom, aspect_ratio, out_crs=ccrs.Mercator()):
    """
    Generate extent from bounding box edges and optional zoom level.

    Args:
        min_lon, max_lon, min_lat, max_lat: Bounding box edges
        zoom (float or 'auto'):  Zoom out level [0 to 10] or 'auto' to dynamically 
            determine the zoom level
        aspect_ratio (tuple): Aspect ratio x/y
        out_crs (cartopy.crs, optional): Out crs for extent values.
        
    Returns:
        tuple: (lon_min, lon_max, lat_min, lat_max)
    """

    init_lat_range = (max_lat - min_lat).copy()
    init_lon_range = (max_lon - min_lon).copy()

    if zoom == "auto":
        zoom_val = 1
    else:
        zoom_val = zoom

    # 
    min_lon, max_lon, min_lat, max_lat = build_mercator_bounds(min_lon, max_lon, min_lat, max_lat, zoom_val, aspect_ratio, out_crs)
    if zoom == 'auto':
        while init_lat_range > (max_lat - min_lat) or init_lon_range > (max_lon - min_lon):
            zoom_val += 1
            min_lon, max_lon, min_lat, max_lat = build_mercator_bounds(min_lon, max_lon, min_lat, max_lat, zoom_val, aspect_ratio, out_crs)
    return min_lon, max_lon, min_lat, max_lat

def generate_extent(
    center_point: Tuple[float, float],
    zoom: float,
    aspect_ratio: Tuple[float, float],
    out_crs: ccrs.Projection = ccrs.Mercator(),
) -> Tuple[float, float, float, float]:
    """Generate extent from central location and zoom level.

    Args:
        center_point: Center of the map as (longitude, latitude).
        zoom: Zoom level [0 to 10].
        aspect_ratio: Aspect ratio (width, height).
        out_crs: Output coordinate reference system for extent values.

    Returns:
        Tuple of (lon_min, lon_max, lat_min, lat_max).
    """
    mercator_crs = ccrs.Mercator()

    # Define zoom scaling
    zoom_coefficient = 2

    # Calculate minimum and maximum longitude
    lon_min, lon_max = (
        center_point[0] - (zoom_coefficient * zoom),
        center_point[0] + (zoom_coefficient * zoom),
    )

    # Transform map center to specified crs (default to Mercator)
    c_mercator = mercator_crs.transform_point(*center_point, src_crs=ccrs.PlateCarree())

    # Transform longitude bounds to specified crs
    lon_min_mercator = mercator_crs.transform_point(
        lon_min, center_point[1], src_crs=ccrs.PlateCarree()
    )[0]
    lon_max_mercator = mercator_crs.transform_point(
        lon_max, center_point[1], src_crs=ccrs.PlateCarree()
    )[0]

    # Calculate latitude bounds using aspect ratio
    lon_distance = lon_max_mercator - lon_min_mercator
    lat_distance = lon_distance * aspect_ratio[1] / aspect_ratio[0]

    lat_max = c_mercator[1] + lat_distance / 2
    lat_min = c_mercator[1] - lat_distance / 2

    # Convert to output coordinate system if needed
    if out_crs != ccrs.Mercator():
        lon_min_out, lat_min_out = out_crs.transform_point(
            lon_min_mercator, lat_min, src_crs=mercator_crs
        )
        lon_max_out, lat_max_out = out_crs.transform_point(
            lon_max_mercator, lat_max, src_crs=mercator_crs
        )
        return lon_min_out, lon_max_out, lat_min_out, lat_max_out

    return lon_min_mercator, lon_max_mercator, lat_min, lat_max


def create_storm_report_legend_elements(
    tornado_reports: Optional[pd.DataFrame] = None,
    hail_reports: Optional[pd.DataFrame] = None,
    wind_reports: Optional[pd.DataFrame] = None,
) -> List[plt.Line2D]:
    """Create legend elements for storm reports.

    Args:
        tornado_reports: DataFrame with tornado reports.
        hail_reports: DataFrame with hail reports.
        wind_reports: DataFrame with wind reports.

    Returns:
        List of legend elements for the reports.
    """
    legend_elements = []

    if tornado_reports is not None and len(tornado_reports) > 0:
        legend_elements.append(
            plt.Line2D(
                [0],
                [0],
                marker="^",
                color="w",
                markerfacecolor="k",
                markersize=8,
                markeredgecolor="k",
                label="Tornado Reports",
            )
        )

    if hail_reports is not None and len(hail_reports) > 0:
        legend_elements.append(
            plt.Line2D(
                [0],
                [0],
                marker="s",
                color="w",
                markerfacecolor="green",
                markersize=8,
                markeredgecolor="darkgreen",
                label="Hail Reports",
            )
        )

    if wind_reports is not None and len(wind_reports) > 0:
        legend_elements.append(
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor="blue",
                markersize=8,
                markeredgecolor="darkblue",
                label="Wind Reports",
            )
        )

    return legend_elements


def plot_storm_reports_on_axis(
    ax,
    tornado_reports: Optional[pd.DataFrame] = None,
    hail_reports: Optional[pd.DataFrame] = None,
    wind_reports: Optional[pd.DataFrame] = None,
    marker_size: int = 20,
    alpha: float = 0.8,
    zorder: int = 10,
) -> None:
    """Plot storm reports on a cartopy axis.

    Args:
        ax: Cartopy axis to plot on.
        tornado_reports: DataFrame with tornado report locations.
        hail_reports: DataFrame with hail report locations.
        wind_reports: DataFrame with wind report locations.
        marker_size: Size of report markers.
        alpha: Transparency of markers.
        zorder: Drawing order (higher values drawn on top).
    """

    if tornado_reports is not None and len(tornado_reports) > 0:
        ax.scatter(
            tornado_reports["longitude"],
            tornado_reports["latitude"],
            c=lsr_colors["tornado"],
            s=marker_size,
            alpha=alpha,
            transform=ccrs.PlateCarree(),
            marker="^",
            linewidths=1,
            zorder=zorder,
        )

    if hail_reports is not None and len(hail_reports) > 0:
        ax.scatter(
            hail_reports["longitude"],
            hail_reports["latitude"],
            c=lsr_colors["hail"],
            s=marker_size,
            alpha=alpha,
            transform=ccrs.PlateCarree(),
            marker="s",
            linewidths=1,
            zorder=zorder,
        )

    if wind_reports is not None and len(wind_reports) > 0:
        ax.scatter(
            wind_reports["longitude"],
            wind_reports["latitude"],
            c=lsr_colors["wind"],
            s=marker_size,
            alpha=alpha,
            transform=ccrs.PlateCarree(),
            marker="o",
            linewidths=1,
            zorder=zorder,
        )


def get_polygon_from_bounding_box(bounding_box):
    """Convert a bounding box tuple to a shapely Polygon."""
    if bounding_box is None:
        return None
    left_lon, right_lon, bot_lat, top_lat = bounding_box
    return Polygon(
        [
            (left_lon, bot_lat),
            (right_lon, bot_lat),
            (right_lon, top_lat),
            (left_lon, top_lat),
            (left_lon, bot_lat),
        ]
    )


def plot_polygon(
    polygon, ax, color="yellow", alpha=0.5, my_zorder=1, linewidth=2, fill=True
):
    """Plot a shapely Polygon on a Cartopy axis."""
    if polygon is None:
        return
    patch = patches.Polygon(
        polygon.exterior.coords,
        closed=True,
        facecolor=color if fill else "none",
        edgecolor=color,
        alpha=alpha,
        linewidth=linewidth,
        zorder=my_zorder,
        transform=ccrs.PlateCarree(),
    )
    ax.add_patch(patch)


def plot_all_cases(
    ewb_cases,
    event_type=None,
    filename=None,
    bounding_box=None,
    fill_boxes=False,
    ax=None,
):
    """A function to plot all cases
    Args:
        ewb_cases (list): A list of cases to plot.
        event_type (str): The type of event to plot. If None, all
            events will be plotted).
        filename (str): The name of the file to save the plot. If
            None, the plot will not be saved.
        bounding_box (tuple): A tuple of the form (min_lon, min_lat,
        max_lon, max_lat) to set the bounding box for the plot. If
            None, the full world map will be plotted.
        fill_boxes (bool): Whether to fill the boxes with color.
        ax (matplotlib.axes.Axes): The axis to plot the cases on. If None, a new axis
            will be created using plt.axes(projection=ccrs.PlateCarree()).
    """
    # plot all cases on one giant world map
    if ax is None:
        _ = plt.figure(figsize=(15, 10))
        ax = plt.axes(projection=ccrs.PlateCarree())

    # setup to plot cartopy axes
    # ax.set_projection(ccrs.PlateCarree())

    # plot the full map or a subset if bounding_box is specified
    if bounding_box is None:
        ax.set_global()
    else:
        ax.set_extent(bounding_box, crs=ccrs.PlateCarree())

    # save the bounding box polygon to subset the counts later
    if bounding_box is not None:
        bounding_box_polygon = get_polygon_from_bounding_box(bounding_box)

    # Add coastlines and gridlines
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    ax.add_feature(cfeature.LAND, edgecolor="black")
    ax.add_feature(cfeature.LAKES, edgecolor="black", facecolor="white")
    ax.add_feature(cfeature.RIVERS, edgecolor="black")
    ax.add_feature(cfeature.OCEAN, edgecolor="black", facecolor="white", zorder=10)

    # Add gridlines
    gl = ax.gridlines(
        draw_labels=True, linewidth=0.5, color="gray", alpha=0.5, linestyle="--"
    )
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()
    gl.xlabel_style = {"size": 14}
    gl.ylabel_style = {"size": 14}

    # Define colors for each event type
    # use seaborn color palette for colorblind friendly colors
    sns_palette = sns.color_palette("tab10")
    sns.set_style("whitegrid")

    event_colors = {
        "freeze": sns_palette[0],
        "heat_wave": sns_palette[3],
        "tropical_cyclone": sns_palette[1],
        "severe_convection": sns_palette[5],
        "atmospheric_river": sns_palette[7],
    }

    # Initialize counts for each event type
    counts_by_type = dict(
        {
            "freeze": 0,
            "heat_wave": 0,
            "severe_convection": 0,
            "atmospheric_river": 0,
            "tropical_cyclone": 0,
        }
    )

    zorders = {
        "freeze": 9,
        "heat_wave": 8,
        "atmospheric_river": 2,
        "tropical_cyclone": 10,
        "severe_convection": 0,
    }
    alphas = {
        "freeze": 0.2,
        "heat_wave": 0.2,
        "atmospheric_river": 0.3,
        "tropical_cyclone": 0.07,
        "severe_convection": 0.02,
    }

    # Handle both IndividualCaseCollection and IndividualCase
    if isinstance(ewb_cases, cases.IndividualCaseCollection):
        cases_to_plot = ewb_cases.cases
    elif isinstance(ewb_cases, cases.IndividualCase):
        cases_to_plot = [ewb_cases]
    else:
        raise TypeError(
            f"ewb_cases must be IndividualCase or "
            f"IndividualCaseCollection, got {type(ewb_cases)}"
        )

    # Plot boxes for each case
    for indiv_case in cases_to_plot:
        # Get color based on event type
        indiv_event_type = indiv_case.event_type
        color = event_colors.get(
            indiv_event_type, "gray"
        )  # Default to gray if event type not found

        # check if the case is inside the bounding box
        if bounding_box is not None:
            if not shapely.intersects(
                indiv_case.location.as_geopandas().geometry[0], bounding_box_polygon
            ):
                # print(f"Skipping case {indiv_case.case_id_number} "
                # f"as it is outside the bounding box.")
                continue

        # count the events by type
        counts_by_type[indiv_event_type] += 1

        # Plot the case geopandas info
        if event_type is None or indiv_event_type == event_type:
            # to handle wrapping around the prime meridian, we
            # can't use geopandas plot (and besides it is slow)
            # instead we have multi-polygon patches if it wraps
            # around and we need to plot each polygon separately
            if isinstance(
                indiv_case.location.as_geopandas().geometry.iloc[0],
                shapely.geometry.MultiPolygon,
            ):
                for poly in indiv_case.location.as_geopandas().geometry.iloc[0].geoms:
                    plot_polygon(
                        poly,
                        ax,
                        color=color,
                        alpha=alphas[indiv_event_type],
                        my_zorder=zorders[indiv_event_type],
                        fill=fill_boxes,
                    )
            else:
                plot_polygon(
                    indiv_case.location.as_geopandas().geometry.iloc[0],
                    ax,
                    color=color,
                    alpha=alphas[indiv_event_type],
                    my_zorder=zorders[indiv_event_type],
                    fill=fill_boxes,
                )

    # Create a custom legend for event types
    if event_type is not None:
        # if we are only plotting one event type, only show that in the legend
        legend_elements = [
            Patch(
                facecolor=event_colors[event_type],
                alpha=0.9,
                label=f"{event_type.replace('_', ' ').title()} (n = %d)"
                % counts_by_type[event_type],
            ),
        ]
    else:
        # otherwise show all event types in the legend
        legend_elements = [
            Patch(
                facecolor=event_colors["heat_wave"],
                alpha=0.9,
                label="Heat Wave (n = %d)" % counts_by_type["heat_wave"],
            ),
            Patch(
                facecolor=event_colors["freeze"],
                alpha=0.9,
                label="Freeze (n = %d)" % counts_by_type["freeze"],
            ),
            Patch(
                facecolor=event_colors["severe_convection"],
                alpha=0.9,
                label="Convection (n = %d)" % counts_by_type["severe_convection"],
            ),
            Patch(
                facecolor=event_colors["atmospheric_river"],
                alpha=0.9,
                label="Atmospheric River (n = %d)"
                % counts_by_type["atmospheric_river"],
            ),
            Patch(
                facecolor=event_colors["tropical_cyclone"],
                alpha=0.9,
                label="Tropical Cyclone (n = %d)" % counts_by_type["tropical_cyclone"],
            ),
        ]

    # Create a larger legend by specifying a larger font size in the prop dictionary
    legend = ax.legend(
        handles=legend_elements,
        loc="lower left",
        framealpha=1,
        frameon=True,
        borderpad=0.5,
        handletextpad=0.8,
        handlelength=2.5,
    )
    legend.set_zorder(10)

    if event_type is None:
        title = "ExtremeWeatherBench Cases (n = %d)" % sum(counts_by_type.values())
    else:
        title = (
            f"{event_type.replace('_', ' ').title()} (n = {counts_by_type[event_type]})"
        )

    ax.set_title(title, loc="center", fontsize=20)

    # save if there is a filename specified (otherwise the user
    # just wants to see the plot)
    if filename is not None:
        plt.savefig(filename, transparent=False, bbox_inches="tight", dpi=300)


# main plotting function for plotting all cases
def plot_all_cases_and_obs(
    ewb_cases,
    event_type=None,
    filename=None,
    bounding_box=None,
    targets=None,
    show_orig_pph=False,
    case_id=None,
    ax=None,
    show_legend=True,
):
    """Plot all cases (outlined) and observations (filled) on map.
    Args:
        ewb_cases (list): A list of cases to plot.
        event_type (str): The type of event to plot. If None, all
        events will be plotted).
        filename (str): The name of the file to save the plot. If
            None, the plot will not be saved.
        bounding_box (tuple): A tuple of the form (min_lon, min_lat,
        max_lon, max_lat) to set the bounding box for the plot. If
            None, the full world map will be plotted.
        targets (dict): A dictionary containing observation metadata for each case,
            such as PPH and LSR reports.
        show_orig_pph (bool): Whether to show the original PPH reports.
        case_id (str): The ID of the case to plot. If None, all cases will be plotted.
        ax (matplotlib.axes.Axes): The axis to plot the cases on. If None, a new axis
            will be created using plt.axes(projection=ccrs.PlateCarree()).
    """
    # plot all cases on one giant world map
    if ax is None:
        _ = plt.figure(figsize=(15, 10))
        ax = plt.axes(projection=ccrs.PlateCarree())

    # plot the full map or a subset if bounding_box is specified
    if bounding_box is None:
        ax.set_global()
    else:
        ax.set_extent(bounding_box, crs=ccrs.PlateCarree())

    # save the bounding box polygon to subset the counts later
    if bounding_box is not None:
        bounding_box_polygon = get_polygon_from_bounding_box(bounding_box)

    # Add coastlines and gridlines
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    ax.add_feature(cfeature.LAND, edgecolor="black")
    ax.add_feature(cfeature.LAKES, edgecolor="black", facecolor="white")
    ax.add_feature(cfeature.RIVERS, edgecolor="black")
    ax.add_feature(cfeature.OCEAN, edgecolor="black", facecolor="white", zorder=10)

    # Add gridlines
    gl = ax.gridlines(
        draw_labels=True, linewidth=0.5, color="gray", alpha=0.5, linestyle="--"
    )
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()

    # Define colors for each event type
    # use seaborn color palette for colorblind friendly colors
    sns_palette = sns.color_palette("tab10")
    sns.set_style("whitegrid")

    event_colors = {
        "freeze": sns_palette[0],
        "heat_wave": sns_palette[3],
        "tropical_cyclone": sns_palette[1],
        "severe_convection": sns_palette[5],
        "atmospheric_river": sns_palette[7],
    }

    # Initialize counts for each event type
    counts_by_type = dict(
        {
            "freeze": 0,
            "heat_wave": 0,
            "severe_convection": 0,
            "atmospheric_river": 0,
            "tropical_cyclone": 0,
        }
    )

    severe_report_counts = dict(
        {
            "hail": 0,
            "tor": 0,
            "wind": 0,
        }
    )

    observation_counts_by_type = dict(
        {
            "freeze": 0,
            "heat_wave": 0,
            "tropical_cyclone": 0,
            "severe_convection": 0,
            "atmospheric_river": 0,
        }
    )

    zorders = {
        "freeze": 9,
        "heat_wave": 8,
        "atmospheric_river": 2,
        "tropical_cyclone": 10,
        "severe_convection": 0,
    }
    alphas = {
        "freeze": 1,
        "heat_wave": 1,
        "atmospheric_river": 1,
        "tropical_cyclone": 1,
        "severe_convection": 1,
    }

    # Handle both IndividualCaseCollection and IndividualCase
    if isinstance(ewb_cases, cases.IndividualCaseCollection):
        cases_to_plot = ewb_cases.cases
    elif isinstance(ewb_cases, cases.IndividualCase):
        cases_to_plot = [ewb_cases]
    else:
        raise TypeError(
            f"ewb_cases must be IndividualCase or "
            f"IndividualCaseCollection, got {type(ewb_cases)}"
        )

    # Plot boxes for each case
    for indiv_case in cases_to_plot:
        # Get color based on event type
        indiv_event_type = indiv_case.event_type
        color = event_colors.get(
            indiv_event_type, "gray"
        )  # Default to gray if event type not found

        if bounding_box is not None:
            if not shapely.intersects(
                indiv_case.location.as_geopandas().geometry[0], bounding_box_polygon
            ):
                # print(f"Skipping case {indiv_case.case_id_number} "
                # f"as it is outside the bounding box.")
                continue

        # if a specific case id is specified, only plot that case
        if case_id is not None and indiv_case.case_id_number != case_id:
            continue

        # count the events by type
        counts_by_type[indiv_event_type] += 1

        # Plot the case geopandas info
        if indiv_event_type == event_type or event_type is None:
            # print(indiv_case)

            # to handle wrapping around the prime meridian, we
            # can't use geopandas plot (and besides it is slow)
            # instead we have multi-polygon patches if it wraps
            # around and we need to plot each polygon separately
            if isinstance(
                indiv_case.location.as_geopandas().geometry.iloc[0],
                shapely.geometry.MultiPolygon,
            ):
                for poly in indiv_case.location.as_geopandas().geometry.iloc[0].geoms:
                    plot_polygon(
                        poly,
                        ax,
                        color=color,
                        alpha=alphas[indiv_event_type],
                        my_zorder=zorders[indiv_event_type],
                        linewidth=1.2,
                        fill=False,
                    )
            else:
                plot_polygon(
                    indiv_case.location.as_geopandas().geometry.iloc[0],
                    ax,
                    color=color,
                    alpha=alphas[indiv_event_type],
                    my_zorder=zorders[indiv_event_type],
                    linewidth=1.2,
                    fill=False,
                )

            # grab the target data for this case; targets is a list of tuples of
            # (case_id, target dataset)
            # print(targets)
            if indiv_event_type == "severe_convection":
                my_target_info = [
                    n[1]
                    for n in targets
                    if n[0] == indiv_case.case_id_number
                    and n[1].attrs["source"] == "local_storm_reports"
                ]
            elif indiv_event_type in ["heat_wave", "freeze", "tropical_cyclone"]:
                my_target_info = [
                    n[1]
                    for n in targets
                    if n[0] == indiv_case.case_id_number
                    and n[1].attrs["source"] != "ERA5"
                ]
            # print(my_target_info)

            # make a scatter plot of the target points (for hot/cold/tc events)
            if (
                indiv_event_type in ["heat_wave", "freeze", "tropical_cyclone"]
                and len(my_target_info) > 0
            ):
                # Get the data from my_target_info
                data = my_target_info[0]

                # sparse array for GHCN data
                if indiv_event_type in ["heat_wave", "freeze"]:
                    try:
                        data = utils.stack_dataarray_from_dims(
                            data["surface_air_temperature"], ["latitude", "longitude"]
                        )
                    except Exception as e:
                        print(
                            f"Error stacking sparse data for "
                            f"{indiv_case.case_id_number} from "
                            f"dimensions latitude, longitude: {e}. "
                            f"This is likely because the data is not "
                            f"available for this case."
                        )
                        continue
                try:
                    lat_values = data["latitude"].values
                    lon_values = data["longitude"].values
                except Exception as e:
                    print(
                        f"Error stacking sparse data from dimensions "
                        f"latitude, longitude: {e}"
                    )
                    continue

                # Convert longitude values from 0-360 to -180 to 180 for proper
                # antimeridian handling with Cartopy
                lon_values_180 = utils.convert_longitude_to_180(lon_values)

                ax.scatter(
                    lon_values_180,
                    lat_values,
                    color=color,
                    s=1,
                    alpha=alphas[indiv_event_type],
                    transform=ccrs.Geodetic(),
                    zorder=zorders[indiv_event_type],
                )

                # add the count of observations
                observation_counts_by_type[indiv_event_type] += len(lat_values)

            # if it is convective, show the PPH and LSRs
            if indiv_event_type == "severe_convection":
                # Get the data from my_target_info
                data = my_target_info[0]
                # print(data)
                try:
                    data = utils.stack_dataarray_from_dims(
                        data["report_type"], ["latitude", "longitude"]
                    )
                except Exception as e:
                    print(
                        f"Error stacking sparse data for "
                        f"{indiv_case.case_id_number} from "
                        f"dimensions latitude, longitude: {e}. "
                        f"This is likely because the data is not "
                        f"available for this case."
                    )
                    continue

                for my_data in data:
                    # print(my_data)
                    hail_reports = my_data[my_data == "hail"]
                    if len(hail_reports) == 0:
                        hail_reports = my_data[my_data == 2]

                    severe_report_counts["hail"] += len(hail_reports)
                    # print(hail_reports)
                    lat_values = hail_reports.latitude.values
                    lon_values = hail_reports.longitude.values
                    ax.scatter(
                        lon_values,
                        lat_values,
                        color=lsr_colors["hail"],
                        alpha=0.9,
                        marker="s",
                        transform=ccrs.Geodetic(),
                        zorder=8,
                        s=6,
                    )

                    tor_reports = my_data[my_data == "tor"]
                    if len(tor_reports) == 0:
                        tor_reports = my_data[my_data == 3]

                    severe_report_counts["tor"] += len(tor_reports)
                    # print(tor_reports)
                    lat_values = tor_reports.latitude.values
                    lon_values = tor_reports.longitude.values
                    ax.scatter(
                        lon_values,
                        lat_values,
                        color=lsr_colors["tornado"],
                        marker="^",
                        transform=ccrs.Geodetic(),
                        zorder=9,
                        s=6,
                    )

    if show_legend:
        # Create a custom legend for event types
        if event_type is not None:
            # if we are only plotting one event type, only show that in the legend
            if event_type == "severe_convection":
                legend_elements = [
                    plt.Line2D(
                        [0],
                        [0],
                        color=lsr_colors["hail"],
                        marker="s",
                        linestyle="none",
                        label="Hail Reports (n = %d)" % severe_report_counts["hail"],
                    ),
                    plt.Line2D(
                        [0],
                        [0],
                        color=lsr_colors["tornado"],
                        marker="^",
                        linestyle="none",
                        label="Tornado Reports (n = %d)" % severe_report_counts["tor"],
                    ),
                ]
            else:
                if event_type in ["heat_wave", "freeze"]:
                    my_label = (
                        "GHCNh stations (n = %d)"
                        % observation_counts_by_type[event_type]
                    )
                elif event_type == "tropical_cyclone":
                    my_label = (
                        "IBTrACS (n = %d)" % observation_counts_by_type[event_type]
                    )
                else:
                    my_label = (
                        "Atmospheric Rivers (n = %d)"
                        % observation_counts_by_type[event_type]
                    )

                legend_elements = [
                    plt.Line2D(
                        [0],
                        [0],
                        color=event_colors[event_type],
                        linestyle="none",
                        marker=".",
                        markersize=10,
                        label=my_label,
                    ),
                ]
        else:
            # otherwise show all event types in the legend
            legend_elements = [
                Patch(
                    facecolor=event_colors["heat_wave"],
                    alpha=0.9,
                    label="Heat Wave (n = %d)" % counts_by_type["heat_wave"],
                ),
                Patch(
                    facecolor=event_colors["freeze"],
                    alpha=0.9,
                    label="Freeze (n = %d)" % counts_by_type["freeze"],
                ),
                Patch(
                    facecolor=event_colors["severe_convection"],
                    alpha=0.9,
                    label="Convection (n = %d)" % counts_by_type["severe_convection"],
                ),
                Patch(
                    facecolor=event_colors["atmospheric_river"],
                    alpha=0.9,
                    label="Atmospheric River (n = %d)"
                    % counts_by_type["atmospheric_river"],
                ),
                Patch(
                    facecolor=event_colors["tropical_cyclone"],
                    alpha=0.9,
                    label="Tropical Cyclone (n = %d)"
                    % counts_by_type["tropical_cyclone"],
                ),
            ]
        # Create a larger legend by specifying a larger font size in the prop dictionary
        legend = ax.legend(
            handles=legend_elements,
            loc="lower left",
            framealpha=1,
            frameon=True,
            borderpad=0.5,
            handletextpad=0.8,
            handlelength=2.5,
        )
        legend.set_zorder(10)

    if event_type is None:
        title = "ExtremeWeatherBench Cases (n = %d)" % sum(counts_by_type.values())
    else:
        title = (
            f"{event_type.replace('_', ' ').title()} (n = %d)"
            % counts_by_type[event_type]
        )

    ax.set_title(title, fontsize=20)

    # save if there is a filename specified (otherwise the user
    # just wants to see the plot)
    if filename is not None:
        plt.savefig(filename, transparent=False, bbox_inches="tight", dpi=300)


def plot_boxes(box_list, box_names, title, filename=None):
    # plot all cases on one giant world map
    _ = plt.figure(figsize=(15, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()

    # Add coastlines and gridlines
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    ax.add_feature(cfeature.LAND, edgecolor="black")
    ax.add_feature(cfeature.LAKES, edgecolor="black", facecolor="white")
    ax.add_feature(cfeature.RIVERS, edgecolor="black")

    # Add gridlines
    gl = ax.gridlines(
        draw_labels=True, linewidth=0.5, color="black", alpha=1, linestyle="--"
    )
    gl.top_labels = False
    gl.right_labels = False
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()

    # Define colors for each event type
    # use seaborn color palette for colorblind friendly colors
    _ = sns.color_palette("tab10")
    sns.set_style("whitegrid")

    # Plot boxes for each case
    for box in box_list:
        plot_polygon(box, ax, color="blue", alpha=1, fill=False)

    plt.legend(loc="lower left", fontsize=12)
    ax.set_title(title, loc="left", fontsize=20)

    # save if there is a filename specified (otherwise the user
    # just wants to see the plot)
    if filename is not None:
        plt.savefig(filename, transparent=False, bbox_inches="tight", dpi=300)


def plot_results_by_metric(
    data,
    settings,
    title,
    filename=None,
    show_all_in_legend=False,
    ax=None,
    y_label="Celsius",
):
    """
    Plots the results of the ExtremeWeatherBench for the data
    specified.
    parameters:
        data: list of dictionaries containing the data to plot
        settings: list of dictionaries containing the plot settings
        title: string, the title of the plot
        filename: string, filename to save the plot to (None if you
        don't want to save it)
        show_all_in_legend: boolean, if True, then all labels will
        be shown in the legend, if False they will be grouped
    """
    if ax is None:
        fig = plt.figure(figsize=(6, 4))
        ax = fig.add_axes([0, 0, 1, 1])

    sns.set_theme(style="whitegrid")
    _ = sns.color_palette("tab10")

    legend_elements = []
    legend_labels = list()

    # and add the HRES line
    for my_settings in settings:
        if show_all_in_legend:
            # my_label = f"{my_settings['label_str']} (n={my_n})"
            raise ValueError("need to fix what my_n should be")
        else:
            my_label = my_settings["label_str"]

        if "HRES" in my_label:
            legend_elements.append(
                plt.Line2D(
                    [0], [0], color=my_settings["color"], linewidth=4, label=my_label
                )
            )
            break

    # Add a blank line to your legend_elements list
    legend_elements.append(plt.Line2D([0], [0], color="white", alpha=0, label=" "))

    for i, model in enumerate(data):
        my_mean = model["value"].mean("case_id_number")
        my_n = len(np.unique(model["case_id_number"].values))
        my_settings = settings[i]
        if show_all_in_legend:
            my_label = f"{my_settings['label_str']} (n={my_n})"
        else:
            my_label = my_settings["label_str"]

        ax.plot(
            my_mean,
            color=my_settings["color"],
            linewidth=4,
            label=my_label,
            linestyle=my_settings["linestyle"],
            marker=my_settings["marker"],
            markersize=10,
        )

        # add any unique labels to the legend except for HRES
        # (it gets its own line in the legend)
        if show_all_in_legend or (
            my_label not in legend_labels and "HRES" not in my_label
        ):
            legend_labels.append(my_label)
            legend_elements.append(
                plt.Line2D(
                    [0], [0], color=my_settings["color"], linewidth=4, label=my_label
                )
            )

    # set the xticks in days
    xtick_str = [
        f"{int(my_time / np.timedelta64(1, 'D')):d}"
        for my_time in model["lead_time"].values
    ]
    ax.set_xticks(labels=xtick_str, ticks=np.arange(0, len(model["lead_time"]), 1))

    ax.set_ylabel(y_label, fontsize=20)
    ax.set_xlabel("Lead Time (days)", fontsize=20)
    ax.set_title(title, fontsize=20)

    # Add a blank line to your legend_elements list
    legend_elements.append(plt.Line2D([0], [0], color="white", alpha=0, label=" "))

    # now add the unique groups with markers
    my_groups = list()
    for my_settings in settings:
        if my_settings["group"] not in my_groups and my_settings["group"] != "HRES":
            my_groups.append(my_settings["group"])
            legend_elements.append(
                plt.Line2D(
                    [0],
                    [0],
                    color="darkgrey",
                    marker=my_settings["marker"],
                    markersize=10,
                    label=my_settings["group"],
                    linestyle=my_settings["linestyle"],
                    linewidth=4,
                )
            )

    ax.legend(handles=legend_elements, loc="center left", bbox_to_anchor=(1.0, 0.5))

    if filename is not None:
        plt.savefig(filename, bbox_inches="tight", dpi=300)


def plot_two_results_by_metric(
    data1,
    data2,
    settings1,
    settings2,
    y_label1,
    y_label2,
    title,
    filename=None,
    show_all_in_legend=False,
    ax=None,
):
    """
    Plots the results of the ExtremeWeatherBench for the data
    specified for two different metrics.
    parameters:
        data1: list of dictionaries containing the data to plot for the first metric
        data2: list of dictionaries containing the data to plot for the second metric
        settings1: list of dictionaries containing the plot settings for the first
            metric
        settings2: list of dictionaries containing the plot settings for the second
            metric
        title: string, the title of the plot
        filename: string, filename to save the plot to (None if you
        don't want to save it)
        show_all_in_legend: boolean, if True, then all labels will
        be shown in the legend, if False they will be grouped
    """
    if ax is None:
        fig = plt.figure(figsize=(6, 4))
        ax = fig.add_axes([0, 0, 1, 1])

    ax2 = ax.twinx()

    sns.set_theme(style="whitegrid")
    _ = sns.color_palette("tab10")

    legend_elements = []
    legend_labels = list()

    # and add the HRES line
    for my_settings in settings1:
        if show_all_in_legend:
            # my_label = f"{my_settings['label_str']} (n={my_n})"
            raise ValueError("need to fix what my_n should be")
        else:
            my_label = my_settings["label_str"]

        if "HRES" in my_label:
            legend_elements.append(
                plt.Line2D(
                    [0], [0], color=my_settings["color"], linewidth=4, label=my_label
                )
            )
            break

    # Add a blank line to your legend_elements list
    legend_elements.append(plt.Line2D([0], [0], color="white", alpha=0, label=" "))

    for i, model in enumerate(data1):
        my_mean = model["value"].mean("case_id_number")
        my_n = len(np.unique(model["case_id_number"].values))
        my_settings = settings1[i]
        if show_all_in_legend:
            my_label = f"{my_settings['label_str']} (n={my_n})"
        else:
            my_label = my_settings["label_str"]

        ax.plot(
            my_mean,
            color=my_settings["color"],
            linewidth=4,
            label=my_label,
            linestyle=my_settings["linestyle"],
            marker=my_settings["marker"],
            markersize=10,
        )

        # plot the second metric
        model2 = data2[i]
        my_mean2 = model2["value"].mean("case_id_number")
        my_settings2 = settings2[i]
        ax2.plot(
            my_mean2,
            color=my_settings2["color"],
            linewidth=4,
            label=my_label,
            linestyle=my_settings2["linestyle"],
            marker=my_settings2["marker"],
            markersize=10,
        )

        # add any unique labels to the legend except for HRES
        # (it gets its own line in the legend)
        if show_all_in_legend or (
            my_label not in legend_labels and "HRES" not in my_label
        ):
            legend_labels.append(my_label)
            legend_elements.append(
                plt.Line2D(
                    [0], [0], color=my_settings["color"], linewidth=4, label=my_label
                )
            )

    # set the xticks in days
    xtick_str = [
        f"{int(my_time / np.timedelta64(1, 'D')):d}"
        for my_time in model["lead_time"].values
    ]
    ax.set_xticks(labels=xtick_str, ticks=np.arange(0, len(model["lead_time"]), 1))

    ax.set_ylabel(y_label1, fontsize=20)
    ax2.set_ylabel(y_label2, fontsize=20)
    ax.set_xlabel("Lead Time (days)", fontsize=20)
    ax.set_title(title, fontsize=20)

    # Add a blank line to your legend_elements list
    legend_elements.append(plt.Line2D([0], [0], color="white", alpha=0, label=" "))

    # now add the unique groups with markers
    my_groups = list()
    for my_settings in settings1 + settings2:
        if my_settings["group"] not in my_groups and my_settings["group"] != "HRES":
            my_groups.append(my_settings["group"])
            legend_elements.append(
                plt.Line2D(
                    [0],
                    [0],
                    color="darkgrey",
                    marker=my_settings["marker"],
                    markersize=10,
                    label=my_settings["group"],
                    linestyle=my_settings["linestyle"],
                    linewidth=4,
                )
            )

    ax2.legend(handles=legend_elements, loc="center left", bbox_to_anchor=(1.1, 0.5))

    if filename is not None:
        plt.savefig(filename, bbox_inches="tight", dpi=300)


def plot_heatmap(
    relative_error_array,
    error_array,
    settings,
    title=None,
    filename=None,
    ax=None,
    show_colorbar=False,
):
    """
    Plots a heatmap of the relative error of the models versus the IFS HRES
    parameters:
        relative_error_array: xarray dataset containing the relative error
        error_array: xarray dataset containing the error
        settings: list of dictionaries containing the plot settings
        title: string, the title of the plot
        filename: string, filename to save the plot to (None if you don't want to
            save it)
        ax: matplotlib axis object, the axis to plot the heatmap on
            (None if you are creating a new figure). If provided, the function
            will create 3 subplots within this axis, effectively subdividing it.
            The parent axis will be hidden and used as a container.
        show_colorbar: boolean, if True, the colorbar will be shown
    """
    n_rows = 1
    n_cols = len(settings["metric_str"])
    col_space = 0.5
    row_space = 0.5
    figsize = (
        5 * n_cols + col_space * (n_cols - 1),
        5 * n_rows + row_space * (n_rows - 1),
    )

    reds = sns.color_palette("Reds", 6)
    blues = sns.color_palette("Blues_r", 6)
    cmap = mcolors.ListedColormap(
        blues + [(0.95, 0.95, 0.95)] + reds, name="wb_scorecard"
    )
    cb_levels = [-50, -20, -10, -5, -2, -1, 1, 2, 5, 10, 20, 50]
    vmin = cb_levels[0]
    vmax = cb_levels[-1]
    norm = mcolors.BoundaryNorm(cb_levels, cmap.N, extend="both")
    cbar_kws = dict(
        orientation="horizontal",
        extend="both",
        fraction=0.05,
        pad=0.05,
    )

    # Create figure and subplots
    if ax is None:
        # Create new figure and subplots
        fig, axs = plt.subplots(n_rows, n_cols, figsize=figsize)
        is_subplot = False
    else:
        # Use existing axis - create subplots within it
        fig = ax.get_figure()
        parent_pos = ax.get_position()

        # Calculate positions for 3 subplots within the parent axis
        # Account for spacing between subplots
        total_width = parent_pos.width
        total_height = parent_pos.height
        # Use smaller spacing for nested subplots
        spacing = 0.01  # spacing between subplots (as fraction of parent)

        subplot_width = (total_width - spacing * (n_cols - 1)) / n_cols

        # Leave some space at top and bottom for labels when used as subplot
        label_padding = 0.05  # fraction of height to reserve for labels
        plot_height = total_height * (1 - label_padding)
        plot_bottom = parent_pos.y0 + total_height * label_padding * 0.3

        axs = []
        for i in range(n_cols):
            left = parent_pos.x0 + i * (subplot_width + spacing)
            # Create new axis within the parent axis's bounding box
            sub_ax = fig.add_axes([left, plot_bottom, subplot_width, plot_height])
            axs.append(sub_ax)

        # Hide the parent axis since we're using it as a container
        ax.set_visible(False)
        is_subplot = True

    # Adjust font sizes based on whether we're a subplot
    if is_subplot:
        title_fontsize = "xx-large"
        label_fontsize = "xx-large"
        tick_fontsize = "xx-large"
        title_y = 1.05
        annot_fontsize = 12
    else:
        title_fontsize = "xx-large"
        label_fontsize = "large"
        tick_fontsize = "large"
        title_y = 1.1
        annot_fontsize = "large"

    subplot_titles = settings["subplot_titles"]
    for i, my_title in enumerate(subplot_titles):
        ax = axs[i]
        # print(my_title)
        metric = settings["metric_str"][i]
       
        # Determine format based on the range of values being displayed
        error_values = error_array[metric]
        error_max = np.nanmax(np.abs(error_values))
        error_min = np.nanmin(np.abs(error_values[error_values != 0])) if np.any(error_values != 0) else error_max
        
        # Choose format based on magnitude
        if error_max >= 1000:
            fmt = ".0f"  # No decimals for very large numbers
        elif error_max >= 100:
            fmt = ".1f"  # One decimal for large numbers
        elif error_max >= 1:
            fmt = ".2f"  # Two decimals for medium numbers
        elif error_min < 0.01:
            fmt = ".2g"  # General format (scientific notation) for very small numbers
        else:
            fmt = ".3f"  # Three decimals for small numbers

        ax = sns.heatmap(
            relative_error_array[metric],
            annot=error_array[metric],
            fmt=fmt,
            cmap=cmap,
            norm=norm,
            vmin=vmin,
            vmax=vmax,
            square=True,
            linecolor="w",
            linewidths=0.5 if is_subplot else 1.0,
            cbar=False,
            ax=ax,
            annot_kws={"size": annot_fontsize} if annot_fontsize else {},
        )

        if is_subplot and i == 0 or not is_subplot:
            ax.set_yticklabels(
                settings["model_order"], fontsize=tick_fontsize, rotation=0
            )
        else:
            ax.set_yticklabels([])

        ax.set_xticklabels(settings["lead_time_days"], fontsize=tick_fontsize)
        ax.set_xlabel("Lead time [days]", fontsize=label_fontsize)
        ax.set_title(my_title, fontsize=title_fontsize, y=title_y)

        # Add padding for labels when used as subplot
        if is_subplot:
            ax.tick_params(axis="both", which="major", pad=2)

    # if there are less than n_cols subplots, add a blank subplot
    if len(subplot_titles) < n_cols:
        for i in range(len(subplot_titles), n_cols):
            print(f"adding blank subplot {i}")
            axs[i].set_visible(False)

    # Position colorbar appropriately based on whether we're a subplot
    if is_subplot and show_colorbar:
        # When used as subplot, position colorbar relative to the subplot axes
        # Get bounding box of all subplot axes
        bboxes = [ax.get_position() for ax in axs]
        left = min(bbox.x0 for bbox in bboxes)
        right = max(bbox.x1 for bbox in bboxes)
        bottom = min(bbox.y0 for bbox in bboxes)
        width = right - left
        
        # Position colorbar below the subplots with proper spacing
        cbar_height = 0.03
        cbar_spacing = 0.02  # Space between subplots and colorbar
        cbar_width = width * 0.9
        cbar_left = left + width * 0.05  # Center 90% width colorbar
        cbar_bottom = bottom - cbar_height - cbar_spacing
        cax = fig.add_axes((cbar_left, cbar_bottom, cbar_width, cbar_height))
        
        # Create the colorbar using the first subplot's collections
        cb = fig.colorbar(mappable=axs[0].collections[0], cax=cax, **cbar_kws)
        cb.ax.set_xticks(cb_levels)
        
        # Scale up the label font size for the colorbar (1.2x larger)
        base_size = plt.rcParams['font.size']
        font_scalings = fm.font_scalings
        label_size_numeric = base_size * font_scalings.get(label_fontsize, 1.0)
        cbar_fontsize = label_size_numeric * 1.2
        cb.ax.set_xlabel(
            "Better ← % difference vs IFS HRES → Worse", fontsize=cbar_fontsize
        )
    elif show_colorbar:
        # make the colorbar take up 90% of the width of the figure and center it on the bottom of the figure
        cax = fig.add_axes((0, -0.05, 1, 0.05))
        cax.set_position([0.1, -0.05, 0.8, 0.05])
        cb = fig.colorbar(mappable=ax.collections[0], cax=cax, **cbar_kws)
        cb.ax.set_xticks(cb_levels)
        
        # Scale up the label font size for the colorbar (1.2x larger)
        base_size = plt.rcParams['font.size']
        font_scalings = fm.font_scalings
        label_size_numeric = base_size * font_scalings.get(label_fontsize, 1.0)
        cbar_fontsize = label_size_numeric * 1.2
        cb.ax.set_xlabel(
            "Better ← % difference vs IFS HRES → Worse", fontsize=cbar_fontsize
        )

    # Only call tight_layout if we created the figure ourselves
    if not is_subplot:
        fig.tight_layout()

    if filename is not None:
        plt.savefig(filename, bbox_inches="tight", dpi=300)
