import logging
from typing import Optional, Tuple

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.animation as animation
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from extremeweatherbench import utils

import src.plots.plotting_utils as plotting

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _drop_non_spatial_except_valid_time(da: xr.DataArray) -> xr.DataArray:
    """Keep latitude/longitude/valid_time; collapse any other dims.

    Forecast pickles have a ``lead_time`` dim. When resolving an ERA5-style
    peak from a forecast (ERA5 pickle missing), use the shortest lead so the
    snapshot is as close to analysis as possible.
    """
    if "lead_time" in da.dims:
        da = da.sel(lead_time=pd.Timedelta(0), method="nearest")
    extra = [d for d in da.dims if d not in ("latitude", "longitude", "valid_time")]
    if extra:
        da = da.isel({d: 0 for d in extra})
    return da


def _lonlat_only(da: xr.DataArray) -> xr.DataArray:
    """Reduce to latitude/longitude so pcolormesh gets a 2-D field."""
    extra = [d for d in da.dims if d not in ("latitude", "longitude")]
    if extra:
        da = da.isel({d: 0 for d in extra})
    return da


def snap_ar_anchor_to_synoptic(anchor, hours=(0, 12)) -> np.datetime64:
    """Snap an hourly ERA5 peak to 00/12Z so 00/12-init models have data.

    HRES (and similarly cadenced forecasts) only populate even synoptic
    hours. A 15Z ERA5 peak nearest-neighbors onto 18Z, which is all-NaN
    at 24/72/120/168/240 h leads. 00/12Z is the coarsest grid shared by
    every model in these figures.
    """
    ts = pd.Timestamp(anchor)
    midnight = ts.normalize()
    candidates = [
        midnight + pd.Timedelta(days=day_off, hours=h)
        for day_off in (-1, 0, 1)
        for h in hours
    ]
    return np.datetime64(min(candidates, key=lambda c: abs(c - ts)))


def _nearest_finite_valid_time(da: xr.DataArray, valid_time) -> np.datetime64:
    """Return the valid_time closest to ``valid_time`` that has any finite data.

    Used after snapping so a leftover all-NaN slice (wrong init cadence)
    still falls back to a neighboring populated time rather than a blank
    panel.
    """
    if "valid_time" not in da.dims:
        return np.datetime64(pd.Timestamp(valid_time))
    vts = pd.to_datetime(np.atleast_1d(da.valid_time.values))
    target = pd.Timestamp(valid_time)
    ranked = sorted(
        range(len(vts)),
        key=lambda i: abs(vts[i] - target),
    )
    for i in ranked:
        sl = da.isel(valid_time=i)
        if np.isfinite(np.asarray(sl.values)).any():
            return np.datetime64(vts[i])
    return np.datetime64(vts[ranked[0]]) if len(ranked) else np.datetime64(target)


def resolve_ar_anchor_valid_time(ds) -> np.datetime64:
    """Return the valid_time of maximum AR-mask area inside the case window.

    Mirrors the heat/freeze ``peak_day`` anchor: one shared snapshot time so
    every model panel is valid at the same moment. The previous plotter
    always used ``valid_time[0]`` (the yaml ``start_date``), which is often
    hours to weeks before the AR arrives — e.g. case 103 (Feb 2024
    California) peaks ~36 h after start, and case 108 (April 2023 Middle
    East) is a 30-day window with no AR on April 1.

    Prefers AR-mask area (the field drawn as the black contour). If the mask
    is identically zero, fall back to the timestep of maximum IVT.
    """
    mask = _drop_non_spatial_except_valid_time(ds["atmospheric_river_mask"])
    if "valid_time" not in mask.dims:
        vt = ds["integrated_vapor_transport"].valid_time.values
        return np.datetime64(np.asarray(vt).reshape(-1)[0])

    area = mask.sum(dim=("latitude", "longitude"))
    if float(np.nanmax(area.values)) > 0:
        idx = int(area.argmax("valid_time").values)
        return np.datetime64(area.valid_time.values[idx])

    ivt = _drop_non_spatial_except_valid_time(ds["integrated_vapor_transport"])
    ivt_max = ivt.max(dim=("latitude", "longitude"))
    idx = int(ivt_max.argmax("valid_time").values)
    return np.datetime64(ivt_max.valid_time.values[idx])


def select_ivt_and_maks(graphics_obect, lead_time_hours, valid_time=None):
    """Select IVT and AR mask for one forecast lead, valid at ``valid_time``.

    ``valid_time`` should be the shared case anchor (ERA5 peak AR-mask
    time). If omitted, the peak is resolved from ``graphics_obect`` itself.
    Forecast grids are coarser than ERA5, so selection uses nearest.
    """
    try:
        if valid_time is None:
            valid_time = resolve_ar_anchor_valid_time(graphics_obect)
        lead_time_td = pd.Timedelta(hours=lead_time_hours)
        ivt = graphics_obect["integrated_vapor_transport"].sel(
            lead_time=lead_time_td, method="nearest"
        )
        ar_mask = graphics_obect["atmospheric_river_mask"].sel(
            lead_time=lead_time_td, method="nearest"
        )
        vt = _nearest_finite_valid_time(ivt, valid_time)
        ivt2 = _lonlat_only(ivt.sel(valid_time=vt, method="nearest"))
        ar_mask2 = _lonlat_only(ar_mask.sel(valid_time=vt, method="nearest"))
        return ivt2, ar_mask2
    except (KeyError, AttributeError) as e:
        case_id = getattr(graphics_obect, 'case_id_number', 'unknown')
        print(f"Skipping {lead_time_hours} hours for case {case_id}: missing data. Error: {e}")
        return None, None
    except Exception as e:
        case_id = getattr(graphics_obect, 'case_id_number', 'unknown')
        print(f"Skipping {lead_time_hours} hours for case {case_id}: missing data. Error: {e}")
        return None, None

def select_ivt_and_maks_era5(graphics_obect, valid_time=None):
    """Select ERA5 IVT and AR mask at the shared case anchor valid_time."""
    if valid_time is None:
        valid_time = resolve_ar_anchor_valid_time(graphics_obect)
    ivt = graphics_obect["integrated_vapor_transport"]
    ar_mask = graphics_obect["atmospheric_river_mask"]
    ivt = _lonlat_only(ivt.sel(valid_time=valid_time, method="nearest"))
    ar_mask = _lonlat_only(ar_mask.sel(valid_time=valid_time, method="nearest"))
    return ivt, ar_mask
    
def setup_atmospheric_river_colormap_and_levels() -> Tuple[
    mcolors.ListedColormap, mcolors.BoundaryNorm, np.ndarray
]:
    """Setup colormap and normalization for AR plotting.

    Returns:
        Tuple of (colormap, normalization, levels) for CBSS plotting.
        Levels based on thresholds: < 10,000 (Low/transparent),
        10,000-22,500 (Marginal), > 22,500 (Significant).
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
    bounds = np.arange(0, 1200, 100)
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    return cmap, norm


def plot_ar_mask_single_timestep(
    ivt_data: xr.DataArray,
    ar_mask: xr.DataArray,
    title: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    colorbar: bool = True,
    show_axes: bool = False,
    left_label=None,
) -> plt.Axes:
    """Plot the AR mask for a single timestep.

    This function plots the AR mask for a single timestep. The incoming data must be
    dataarrays with only 2 dimensions: longitude and latitude.

    Args:
        ivt_data: Integrated vapor transport data with time dimension.
        ar_mask: AR mask data with time dimension.
        title: Title of the plot.
        ax: Axes to plot on.
    Returns:
        Axes object.
    """

    cmap, norm = setup_atmospheric_river_colormap_and_levels()

    # Strong checks for dimensions
    if len(ivt_data.dims) != 2 or len(ar_mask.dims) != 2:
        raise ValueError("IVT and AR mask data must have only 2 dimensions.")

    if "longitude" not in ivt_data.dims or "latitude" not in ivt_data.dims:
        raise ValueError("IVT data must have longitude and latitude dimensions.")

    if "longitude" not in ar_mask.dims or "latitude" not in ar_mask.dims:
        raise ValueError("AR mask data must have longitude and latitude dimensions.")
    if ax is None:
        fig = plt.figure(figsize=(16, 9))
        # Adjust subplot parameters to center plot and minimize whitespace
        # Leave space for colorbar on right, but center the main plot area
        fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.05)
        ax = plt.axes(projection=ccrs.PlateCarree())
        is_subplot = False
    else:
        fig = ax.figure
        is_subplot = True

    # Use general plotting functions for geographic features
    plotting.add_geographic_features(ax, include_land_ocean=True, land_ocean_alpha=0.1)
    # Override borders with custom linestyle
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    if show_axes:
        plotting.setup_gridlines(ax, show_top_labels=False, show_right_labels=False, show_left_labels=True, show_bottom_labels=True)
    else:
        plotting.setup_gridlines(ax, show_top_labels=False, show_right_labels=False, show_left_labels=False, show_bottom_labels=False)

    # center_latitude = (ar_mask.latitude.min() + ar_mask.latitude.max()) / 2
    # center_longitude = (ar_mask.longitude.min() + ar_mask.longitude.max()) / 2
    # center_point = (
    #     utils.convert_longitude_to_180(center_longitude.values),
    #     center_latitude.values,
    # )
    # lon_min, lon_max, lat_min, lat_max = generate_extent(
    #     center_point, zoom=8, aspect_ratio=(16, 9), out_crs=ccrs.PlateCarree()
    # )
    lon_min, lon_max, lat_min, lat_max = plotting.generate_plot_extent_bounds(ar_mask.longitude.min(), 
        ar_mask.longitude.max(), ar_mask.latitude.min(), ar_mask.latitude.max(), 
        zoom="auto", aspect_ratio=(9, 9), out_crs=ccrs.PlateCarree())

    # Create initial IVT plot
    im = ax.pcolormesh(
        ivt_data.longitude,
        ivt_data.latitude,
        ivt_data.values,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=norm,
    )

    # Add AR mask as contour
    _ = ax.contour(
        ar_mask.longitude,
        ar_mask.latitude,
        ar_mask.values,
        levels=[0.5],
        colors="black",
        linewidths=2,
        transform=ccrs.PlateCarree(),
    )
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())
    # Add colorbar if requested
    if colorbar:
        cbar = fig.colorbar(im, ax=ax, label="Integrated Vapor Transport (kgm^-1s^-1)")
        cbar.set_label("Integrated Vapor Transport (kgm^-1s^-1)", size=14)
        cbar.ax.tick_params(labelsize=12)
    if title:
        if is_subplot:
            title_size = "xx-large"
        else:
            title_size = "large"
        _ = ax.set_title(title, loc="center", size=title_size)

    if left_label is not None:
        # Convert axes coordinates to figure coordinates for robust positioning
        # Position text to the left of the axis in figure coordinates
        ax_pos = ax.get_position(fig)
        # Position text at the left edge of the figure, vertically centered on the axis
        fig.text(ax_pos.x0 - 0.01, ax_pos.y0 + ax_pos.height * 0.5, left_label, 
            fontsize="xx-large", ha='right', va='center')

    return ax


def plot_ar_mask_global(
    ivt_data: xr.DataArray,
    ar_mask: xr.DataArray,
    title: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    colorbar: bool = True,
) -> plt.Axes:
    """Plot IVT and AR mask on a global map for a single timestep.

    Unlike plot_ar_mask_single_timestep, this function sets a global
    extent rather than zooming into the data domain.

    Args:
        ivt_data: 2D IVT DataArray with latitude and longitude dims.
        ar_mask: 2D AR mask DataArray with latitude and longitude dims.
        title: Optional title for the plot.
        ax: Existing Axes to plot on; creates a new figure if None.
        colorbar: Whether to add a colorbar.

    Returns:
        Axes object.
    """
    cmap, norm = setup_atmospheric_river_colormap_and_levels()

    if len(ivt_data.dims) != 2 or len(ar_mask.dims) != 2:
        raise ValueError("IVT and AR mask data must have only 2 dimensions.")
    if "longitude" not in ivt_data.dims or "latitude" not in ivt_data.dims:
        raise ValueError("IVT data must have longitude and latitude dimensions.")
    if "longitude" not in ar_mask.dims or "latitude" not in ar_mask.dims:
        raise ValueError("AR mask must have longitude and latitude dimensions.")

    if ax is None:
        fig = plt.figure(figsize=(18, 9))
        fig.subplots_adjust(left=0.05, right=0.88, top=0.90, bottom=0.05)
        ax = plt.axes(projection=ccrs.PlateCarree())
        is_subplot = False
    else:
        fig = ax.figure
        is_subplot = True

    plotting.add_geographic_features(ax, include_land_ocean=True, land_ocean_alpha=0.1)
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    ax.set_global()

    im = ax.pcolormesh(
        ivt_data.longitude,
        ivt_data.latitude,
        ivt_data.values,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=norm,
    )
    ax.contour(
        ar_mask.longitude,
        ar_mask.latitude,
        ar_mask.values,
        levels=[0.5],
        colors="black",
        linewidths=1.5,
        transform=ccrs.PlateCarree(),
    )

    title_size = "large" if is_subplot else 18
    if title:
        ax.set_title(title, loc="left", size=title_size)

    if colorbar:
        # For PlateCarree the axes rect fills the map exactly, so we can
        # read the axes position directly in figure coordinates.
        fig.canvas.draw()
        spine = ax.spines["geo"]
        path_fig = spine.get_path().transformed(
            spine.get_transform() + fig.transFigure.inverted()
        )
        verts = path_fig.vertices
        y_bot = float(verts[:, 1].min())
        y_top = float(verts[:, 1].max())
        x_right = float(verts[:, 0].max())

        cbar_gap = 0.012
        cbar_width = 0.020
        cbar_ax = fig.add_axes(
            [x_right + cbar_gap, y_bot, cbar_width, y_top - y_bot]
        )
        cbar = fig.colorbar(im, cax=cbar_ax, orientation="vertical")
        cbar.set_label(
            r"Integrated Vapor Transport (kg m$^{-1}$ s$^{-1}$)",
            size=16,
            rotation=270,
            labelpad=22,
        )
        cbar.ax.tick_params(labelsize=14)

    return ax


def plot_ar_mask_animation(
    case_id: int,
    title: str,
    ivt_data: xr.DataArray,
    ar_mask: xr.DataArray,
) -> None:
    """Create an animated plot of AR mask evolution over time.

    Uses the same domain as ax2 in create_case_summary_plot with original
    styling including custom colormap and contour representation.

    Args:
        case_id: Case ID number.
        title: Event title.
        ivt_data: Integrated vapor transport data with time dimension.
        ar_mask: AR mask data with time dimension.
    """
    # Get the time dimension name
    time_dim = "valid_time" if "valid_time" in ivt_data.dims else "time"

    cmap, norm = setup_atmospheric_river_colormap_and_levels()

    # Create figure and axes matching original styling with tighter layout
    fig = plt.figure(figsize=(16, 9))
    # Adjust subplot parameters to center plot and minimize whitespace
    # Leave space for colorbar on right, but center the main plot area
    fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.05)
    ax = plt.axes(projection=ccrs.PlateCarree())
    # Use general plotting functions for geographic features
    plotting.add_geographic_features(ax, include_land_ocean=True, land_ocean_alpha=0.1)
    # Override borders with custom linestyle
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    plotting.setup_gridlines(ax, show_top_labels=False, show_right_labels=False)

    # Set extent to match ax2 domain (same as AR mask extent + 5 degrees)
    first_ar_slice = ar_mask.isel({time_dim: 0})
    center_latitude = (
        first_ar_slice.latitude.min() + first_ar_slice.latitude.max()
    ) / 2
    center_longitude = (
        first_ar_slice.longitude.min() + first_ar_slice.longitude.max()
    ) / 2
    center_point = (
        utils.convert_longitude_to_180(center_longitude.values),
        center_latitude.values,
    )
    lon_min, lon_max, lat_min, lat_max = generate_extent(
        center_point, zoom=8, aspect_ratio=(16, 9), out_crs=ccrs.PlateCarree()
    )

    # Initialize first frame
    first_time_idx = 0
    ar_slice = ar_mask.isel({time_dim: first_time_idx})
    ivt_slice = ivt_data.isel({time_dim: first_time_idx})
    first_time = ar_mask[time_dim].isel({time_dim: first_time_idx}).values

    # Create initial IVT plot
    im = ax.pcolormesh(
        ivt_slice.longitude,
        ivt_slice.latitude,
        ivt_slice.values,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=norm,
    )

    # Add AR mask as contour
    _ = ax.contour(
        ar_slice.longitude,
        ar_slice.latitude,
        ar_slice.values,
        levels=[0.5],
        colors="black",
        linewidths=2,
        transform=ccrs.PlateCarree(),
    )
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())
    # Add colorbar
    cbar = fig.colorbar(im, ax=ax, label="Integrated Vapor Transport (kgm^-1s^-1)")
    cbar.set_label("Integrated Vapor Transport (kgm^-1s^-1)", size=14)
    cbar.ax.tick_params(labelsize=12)

    # Set initial title matching original format
    _ = ax.set_title(
        f"Case {case_id}: Integrated Vapor Transport and Atmospheric River Mask\n"
        f"{title}\n"
        f"Valid {pd.to_datetime(first_time).strftime('%Y-%m-%d %H:%M')}",
        loc="left",
    )

    def update(frame_idx):
        """Update function for animation."""
        # Clear all previous plots
        ax.clear()

        # Re-add features using general plotting functions
        plotting.add_geographic_features(
            ax, include_land_ocean=True, land_ocean_alpha=0.1
        )
        # Override borders with custom linestyle
        ax.add_feature(cfeature.BORDERS, linestyle=":")
        plotting.setup_gridlines(ax, show_top_labels=False, show_right_labels=False)

        # Reset extent
        ax.set_extent(
            [
                float(first_ar_slice.longitude.min()) - 5,
                float(first_ar_slice.longitude.max()) + 5,
                float(first_ar_slice.latitude.min()) - 5,
                float(first_ar_slice.latitude.max()) + 5,
            ],
            crs=ccrs.PlateCarree(),
        )

        # Get data for this frame
        ar_slice = ar_mask.isel({time_dim: frame_idx})
        ivt_slice = ivt_data.isel({time_dim: frame_idx})
        current_time = ar_mask[time_dim].isel({time_dim: frame_idx}).values

        # Plot IVT background
        im = ax.pcolormesh(
            ivt_slice.longitude,
            ivt_slice.latitude,
            ivt_slice.values,
            transform=ccrs.PlateCarree(),
            cmap=cmap,
            norm=norm,
        )

        # Add AR mask as contour
        ax.contour(
            ar_slice.longitude,
            ar_slice.latitude,
            ar_slice.values,
            levels=[0.5],
            colors="black",
            linewidths=2,
            transform=ccrs.PlateCarree(),
        )

        # Update title
        ax.set_title(
            f"Case {case_id}: Integrated Vapor Transport and Atmospheric River Mask\n"
            f"{title}\n"
            f"Valid {pd.to_datetime(current_time).strftime('%Y-%m-%d %H:%M')}",
            loc="left",
        )

        return [im]

    # Create animation
    num_frames = len(ar_mask[time_dim])
    anim = animation.FuncAnimation(
        fig,
        update,
        frames=range(num_frames),
        interval=200,  # 200ms between frames like original
        blit=False,
        repeat=True,
    )

    # Save animation
    animation_filename = f"case_{case_id:03d}_ar_mask_animation.gif"
    anim.save(animation_filename, writer="pillow", fps=5)
    plt.close()

    logger.info("    Saved AR mask animation: %s", animation_filename)


def generate_extent(center_point, zoom, aspect_ratio, out_crs=ccrs.Mercator()):
    """
    Generate extent from central location and zoom level
    Args:
        center_point (tuple(float, float)): center of the map as (longitude, latitude)
        zoom (float):  Zoom level [0 to 10]
        aspect_ratio (tuple): Aspect ratio x/y
        out_crs (cartopy.crs, optional): Out crs for extent values.
    Returns:
        tuple: (lon_min, lon_max, lat_min, lat_max)
    """
    mercator_crs = ccrs.Mercator()

    # Define zoom scaling
    zoom_coefficient = 2

    # Calculate minimum longitude (min_lon) and maximum longitude (max_lon)
    lon_min, lon_max = (
        center_point[0] - (zoom_coefficient * zoom),
        center_point[0] + (zoom_coefficient * zoom),
    )

    # Transform map center to specified crs (default to Mercator)
    c_mercator = mercator_crs.transform_point(*center_point, src_crs=ccrs.Mercator())

    # Transform minimum longitude and maximum longitude to specified crs (default to
    # Mercator)
    lon_min = mercator_crs.transform_point(
        lon_min, center_point[1], src_crs=ccrs.Mercator()
    )[0]
    lon_max = mercator_crs.transform_point(
        lon_max, center_point[1], src_crs=ccrs.Mercator()
    )[0]

    # Our goal is to calculate minimum latitude (min_lat) and maximum latitude (max_lat)
    # using center point and distance between min_lon and max_lon
    # To achieve this we will use formula [(lon_distance/lat_distance) =
    # (aspect_ratio[0]/aspect_ratio[1])]
    # Calculate distance between min_lon and max_lon
    lon_distance = (lon_max) - (lon_min)

    # To calculate lat_distance, we will proceed accordingly
    lat_distance = lon_distance * aspect_ratio[1] / aspect_ratio[0]

    # Now calculate max_lat and min_lan by adding/subtracting half of the distance from
    # center latitude
    lat_max = c_mercator[1] + lat_distance / 2
    lat_min = c_mercator[1] - lat_distance / 2

    # We can return our result in any format (eg. in Mercator coordinates or in degrees)
    if out_crs != ccrs.Mercator():
        lon_min, lat_min = out_crs.transform_point(lon_min, lat_min, src_crs=out_crs)
        lon_max, lat_max = out_crs.transform_point(lon_max, lat_max, src_crs=out_crs)

    return lon_min, lon_max, lat_min, lat_max
