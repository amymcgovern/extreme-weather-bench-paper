"""Build a 2 x N temperature-event figure for arbitrary cases.

Top row: max consecutive event days per grid cell. Reds for
``heat_wave`` cases, Blues for ``freeze`` cases. When every input
case shares the same event type a single shared colorbar sits to
the right of the row; in mixed-type runs each top-row panel gets
its own inline colorbar instead.

Bottom row: peak-day 2 m temperature where above (heat) or below
(freeze) the matching climatology percentile (85th for heat, 15th
for freeze). The bottom-row Celsius colorbar is shared across all
panels because both event types render with the same Celsius cmap.

Every map shares the same width:height ratio via ``generate_extent``
on a Mercator projection.
"""

import argparse
import sys
from pathlib import Path

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import regionmask
import xarray as xr
from extremeweatherbench import cases, defaults, utils
from matplotlib.gridspec import GridSpec

from src.plots.heat_freeze_utils import (
    celsius_colormap_and_normalize,
    generate_freeze_dataset,
    generate_heatwave_dataset,
    plot_freeze_case,
    plot_heatwave_case,
)
from src.plots.plotting_utils import generate_extent

# data_prep is not packaged; expose temperature_events.py via sys.path.
_EWB_DATA_PREP = Path.home() / "code" / "ExtremeWeatherBench" / "data_prep"
if str(_EWB_DATA_PREP) not in sys.path:
    sys.path.insert(0, str(_EWB_DATA_PREP))
from temperature_events import (  # noqa: E402
    MIN_CONSECUTIVE_DAYS,
    _add_map_features,
    compute_consecutive_field,
    plot_consecutive_map,
)

DEFAULT_CASE_IDS = [14, 23, 70]
DEFAULT_OUTPUT_DIR = (
    Path.home()
    / "code"
    / "extreme-weather-bench-paper"
    / "graphics"
    / "paper"
    / "subplots"
)
CLIMATOLOGY_URI = (
    "gs://extremeweatherbench/datasets/"
    "surface_air_temperature_1990_2019_climatology.zarr/"
)
# Use Mercator for both axes projection and generate_extent so the
# requested aspect ratio is preserved on display.
EXTENT_CRS = ccrs.Mercator()
DEFAULT_PADDING_DEG = 1.0
# Per-case padding override (degrees) added on each side of the
# consec-field bounds before computing the extent. Must be >= 0 to
# guarantee no data is cropped.
PADDING_BY_CASE: dict[int, float] = {}
# Visual spacing between the rightmost case panel and the shared
# colorbar (figure-fraction units) and the colorbar's width.
COLORBAR_GAP_FRAC = 0.008
COLORBAR_WIDTH_FRAC = 0.012

HEAT_TYPES = {"heat_wave"}
FREEZE_TYPES = {"freeze", "cold_snap"}
SUPPORTED_TYPES = HEAT_TYPES | FREEZE_TYPES


def _load_cases(case_ids: list[int]) -> list:
    """Return IndividualCase objects in the order of ``case_ids``."""
    all_cases = cases.load_ewb_events_yaml_into_case_list()
    by_id = {c.case_id_number: c for c in all_cases}
    missing = [cid for cid in case_ids if cid not in by_id]
    if missing:
        raise ValueError(f"Cases not found in events.yaml: {missing}")
    selected = [by_id[cid] for cid in case_ids]
    bad = [
        f"case {c.case_id_number} ({c.event_type})"
        for c in selected
        if c.event_type not in SUPPORTED_TYPES
    ]
    if bad:
        raise ValueError(
            "Only heat_wave / freeze cases are supported; got: "
            + ", ".join(bad)
        )
    return selected


def _to_plot_lon(lons: np.ndarray) -> np.ndarray:
    """Wrap 0..360 longitudes into -180..180 for PlateCarree plotting."""
    arr = np.asarray(lons, dtype=float)
    return np.where(arr > 180, arr - 360.0, arr)


def _cell_aspect(fig, gs_cell) -> tuple[float, float]:
    """Return the (width, height) of a GridSpec cell in inches."""
    pos = gs_cell.get_position(fig)
    fig_w, fig_h = fig.get_size_inches()
    return (pos.width * fig_w, pos.height * fig_h)


def _build_extent(
    case_id: int,
    lats: np.ndarray,
    lons: np.ndarray,
    cell_aspect: tuple[float, float],
) -> tuple[float, float, float, float]:
    """Build a Mercator extent containing all data plus padding.

    Picks the smallest ``generate_extent`` zoom whose resulting
    PlateCarree bounds contain ``data_span + 2 * pad`` in BOTH
    dimensions, then returns the extent in Mercator coordinates so it
    can be applied to a Mercator-projected axes. Iterating in
    PlateCarree space avoids the Mercator-distortion bug an analytical
    formula has at higher latitudes.
    """
    plot_lons = _to_plot_lon(lons)
    pad = max(PADDING_BY_CASE.get(case_id, DEFAULT_PADDING_DEG), 0.0)
    lat_arr = np.asarray(lats)
    center = (
        float((plot_lons.min() + plot_lons.max()) / 2),
        float((lat_arr.min() + lat_arr.max()) / 2),
    )
    required_lon = float(plot_lons.max() - plot_lons.min()) + 2 * pad
    required_lat = float(lat_arr.max() - lat_arr.min()) + 2 * pad

    # Seed zoom from the lon constraint (lon span is exactly 4*zoom in
    # PlateCarree); then grow until the lat span also fits.
    zoom = max(required_lon / 4, 0.05)
    for _ in range(80):
        lon_min, lon_max, lat_min, lat_max = generate_extent(
            center, zoom, cell_aspect, out_crs=ccrs.PlateCarree()
        )
        if (
            (lon_max - lon_min) >= required_lon
            and (lat_max - lat_min) >= required_lat
        ):
            break
        zoom *= 1.05
    return generate_extent(center, zoom, cell_aspect)


def _is_heat(case_obj) -> bool:
    return case_obj.event_type in HEAT_TYPES


def _consec_event_label(case_obj) -> str:
    """Map yaml event_type to plot_consecutive_map's expected literal."""
    return "heat_wave" if _is_heat(case_obj) else "cold_snap"


def _generate_event_dataset(era5, clim_q85, clim_q15, case_obj):
    """Build the merged peak-day dataset for one case."""
    if _is_heat(case_obj):
        return generate_heatwave_dataset(era5, clim_q85, case_obj)
    return generate_freeze_dataset(era5, clim_q15, case_obj)


def _plot_event_panel(case_obj, ds, ax):
    """Dispatch heat / freeze peak-day plotting on a shared axes.

    Returns the mappable so callers can build a shared colorbar.
    Styling kwargs are uniform across both branches so the bottom row
    stays visually consistent.
    """
    common = dict(
        ax=ax,
        add_colorbar=False,
        add_map_features=False,
        add_gridlines=False,
        show_case_label=False,
    )
    if _is_heat(case_obj):
        return plot_heatwave_case(ds, case_obj, **common)
    return plot_freeze_case(ds, case_obj, **common)


def _snug_colorbar(fig, panel_ax, cax) -> None:
    """Move ``cax`` to sit just right of ``panel_ax`` at the same height.

    Avoids the wide gap that ``GridSpec(wspace=...)`` creates when the
    colorbar lives in its own column alongside the case panels.
    """
    fig.canvas.draw_idle()
    panel_pos = panel_ax.get_position()
    cax.set_position(
        [
            panel_pos.x1 + COLORBAR_GAP_FRAC,
            panel_pos.y0,
            COLORBAR_WIDTH_FRAC,
            panel_pos.height,
        ]
    )


def _output_filename(case_ids: list[int], event_types: set[str]) -> str:
    """Build a default output filename keyed on event type + case ids."""
    if event_types <= HEAT_TYPES:
        kind = "heat"
    elif event_types <= FREEZE_TYPES:
        kind = "freeze"
    else:
        kind = "mixed"
    n = len(case_ids)
    ids_str = "_".join(str(i) for i in case_ids)
    return f"{kind}_{2 * n}panel_cases_{ids_str}.png"


def build_figure(case_ids: list[int], output_path: Path) -> Path:
    """Compute fields and render the 2 x N figure to ``output_path``."""
    case_list = _load_cases(case_ids)
    event_types = {c.event_type for c in case_list}
    uniform_event_type = len(event_types) == 1
    has_heat = any(_is_heat(c) for c in case_list)
    has_freeze = any(not _is_heat(c) for c in case_list)
    n = len(case_list)

    print("Computing consecutive event-day fields...")
    consec_results = [compute_consecutive_field(c) for c in case_list]
    shared_vmax = max(
        int(np.asarray(consec).max()) for consec, _, _ in consec_results
    )
    shared_vmax = max(shared_vmax, MIN_CONSECUTIVE_DAYS)

    print("Loading ERA5 + climatology for peak-day maps...")
    era5 = (
        defaults.era5_heatwave_target.open_and_maybe_preprocess_data_from_source()
    )
    if "time" in era5.dims and "valid_time" not in era5.dims:
        era5 = era5.rename({"time": "valid_time"})
    era5 = utils.convert_longitude_to_180(era5)

    clim_root = xr.open_zarr(
        CLIMATOLOGY_URI, storage_options={"anon": True}
    )
    clim_q85 = (
        utils.convert_longitude_to_180(clim_root.sel(quantile=0.85))
        if has_heat
        else None
    )
    clim_q15 = (
        utils.convert_longitude_to_180(clim_root.sel(quantile=0.15))
        if has_freeze
        else None
    )

    event_datasets = [
        _generate_event_dataset(era5, clim_q85, clim_q15, c) for c in case_list
    ]
    # Mask out ocean grid points so the bottom-row panels match the
    # land-only top-row consec maps.
    land_regions = regionmask.defined_regions.natural_earth_v5_0_0.land_110
    event_datasets = [
        ds.where(land_regions.mask(ds.longitude, ds.latitude) == 0)
        for ds in event_datasets
    ]

    # Scale the figure width with the number of cases so each panel
    # keeps roughly the same on-screen size as the original 3-case
    # layout (~6.5 inches per panel + a thin colorbar column).
    fig_width = 6.5 * n + 1.0
    fig = plt.figure(figsize=(fig_width, 11))
    gs = GridSpec(
        2,
        n + 1,
        figure=fig,
        width_ratios=[1] * n + [0.04],
        wspace=0.18,
        hspace=0.25,
        left=0.04,
        right=0.96,
        top=0.93,
        bottom=0.06,
    )

    cell_aspect = _cell_aspect(fig, gs[0, 0])
    extents = [
        _build_extent(c.case_id_number, lats, lons, cell_aspect)
        for c, (_, lats, lons) in zip(case_list, consec_results)
    ]

    print("Drawing top row (consecutive event days)...")
    top_mappables = []
    top_axes = []
    for col, (case_obj, (consec, lats, lons), ext) in enumerate(
        zip(case_list, consec_results, extents)
    ):
        ax = fig.add_subplot(gs[0, col], projection=EXTENT_CRS)
        top_axes.append(ax)
        title = (
            f"Case {case_obj.case_id_number}: {case_obj.title}\n"
            f"{case_obj.start_date.date()} to {case_obj.end_date.date()}"
        )
        # In uniform-type mode we draw a single shared row colorbar to
        # the right; in mixed mode each panel gets its own inline
        # colorbar so the Reds and Blues scales aren't conflated.
        im = plot_consecutive_map(
            consec,
            lats,
            lons,
            _consec_event_label(case_obj),
            title=title,
            ax=ax,
            vmax=shared_vmax,
            add_colorbar=not uniform_event_type,
        )
        ax.set_extent(ext, crs=EXTENT_CRS)
        top_mappables.append(im)

    cax_top = None
    if uniform_event_type:
        cax_top = fig.add_subplot(gs[0, n])
        cbar_top = fig.colorbar(top_mappables[-1], cax=cax_top)
        cbar_top.set_ticks(range(MIN_CONSECUTIVE_DAYS, shared_vmax + 1))
        cbar_top.ax.set_ylabel(
            "Consecutive Days",
            rotation=270,
            labelpad=15,
            va="center",
            fontsize=12,
        )

    print("Drawing bottom row (peak-day temperature maps)...")
    bottom_im = None
    bottom_axes = []
    for col, (case_obj, ds, ext) in enumerate(
        zip(case_list, event_datasets, extents)
    ):
        ax = fig.add_subplot(gs[1, col], projection=EXTENT_CRS)
        bottom_axes.append(ax)
        ax.add_feature(cfeature.OCEAN, facecolor="lightblue", zorder=0)
        bottom_im = _plot_event_panel(case_obj, ds, ax)
        _add_map_features(ax)
        ax.set_extent(ext, crs=EXTENT_CRS)

    # Bottom-row colorbar: every panel uses the same fixed Celsius
    # normalization, so any panel's mappable suffices.
    _, norm_bottom = celsius_colormap_and_normalize()
    cax_bot = fig.add_subplot(gs[1, n])
    cbar_bot = fig.colorbar(bottom_im, cax=cax_bot)
    cbar_bot.mappable.set_norm(norm_bottom)
    cbar_bot.ax.set_ylabel(
        "Temperature (C)",
        rotation=270,
        labelpad=15,
        va="center",
        fontsize=12,
    )

    # Pull each shared colorbar in close to the rightmost case panel
    # (the GridSpec ``wspace`` would otherwise leave a wide gap).
    if cax_top is not None:
        _snug_colorbar(fig, top_axes[-1], cax_top)
    _snug_colorbar(fig, bottom_axes[-1], cax_bot)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure to {output_path}")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a 2 x N temperature-event figure (top: consecutive"
            " event days; bottom: peak-day temperature vs the matching"
            " climatology percentile). Heat / freeze color schemes are"
            " selected automatically per case."
        )
    )
    parser.add_argument(
        "--cases",
        type=int,
        nargs="+",
        default=DEFAULT_CASE_IDS,
        metavar="CASE_ID",
        help=(
            "case_id_number(s) from events.yaml (heat_wave or freeze)."
            f" Default: {' '.join(str(i) for i in DEFAULT_CASE_IDS)}"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output PNG path. Default: "
            f"{DEFAULT_OUTPUT_DIR}/<heat|freeze|mixed>_<2n>panel_cases_<ids>.png"
        ),
    )
    args = parser.parse_args()
    if args.output:
        out = Path(args.output)
    else:
        # Pre-load cases just to pick the right default filename prefix.
        case_list = _load_cases(args.cases)
        event_types = {c.event_type for c in case_list}
        out = DEFAULT_OUTPUT_DIR / _output_filename(args.cases, event_types)
    build_figure(args.cases, out)


if __name__ == "__main__":
    main()
