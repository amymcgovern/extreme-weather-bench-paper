"""Build a 3 x 4 tropical-cyclone track figure across models.

Rows are storms (Lee 2023, Kay 2022, Lan 2023 by default).
Columns are forecast models (BB AIFS, BB Pangu, BB GraphCast,
ECMWF HRES). Each panel plots forecast tracks colored by
init_time, the IBTrACS analysis track in black, and landfall
markers: stars at the predicted landfall point matched per
init_time (next-landfall pairing) and X markers at every
IBTrACS landfall.

Track data is loaded from the per-case pickles produced by
``src/plots/compute_tc_tracks.py`` -- one pickle per (model, case)
under ``saved_data/<model>_tc_tracks/case_<id>.pkl``, each
storing ``{"target": ibtracs_ds, "forecast": forecast_ds}`` with
dask graphs already materialized (see ``_maybe_load`` in the
compute script). The HRES column transparently inherits the
HRES WB2 -> BB HRES fallback that ``compute_tc_tracks.py``
applies at compute time; if HRES WB2 returned no detections for
a case, the pickle under ``hres_tc_tracks/`` already holds the
BB HRES result.

If a pickle is missing this script raises rather than silently
rendering blank panels; the error message names the missing file
and the ``compute_tc_tracks.py`` invocation that would produce it.
"""

import argparse
import pickle
import sys
from pathlib import Path
from typing import Any, Optional

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import extremeweatherbench as ewb
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from extremeweatherbench import cases
from matplotlib.cm import ScalarMappable
from matplotlib.colors import ListedColormap
from matplotlib.gridspec import GridSpec
from matplotlib.lines import Line2D

REPO_ROOT = Path(__file__).resolve().parents[2]

# Keep sys.path aligned with compute_tc_tracks.py so notebooks that import
# this module keep resolving src.data / src.plots correctly. The bare
# ``from check_icechunk import ...`` inside tc_forecast_setup.py doesn't
# matter here anymore (we no longer touch forecast objects), but leaving
# these on the path is harmless.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
_SRC_DATA = REPO_ROOT / "src" / "data"
if str(_SRC_DATA) not in sys.path:
    sys.path.insert(0, str(_SRC_DATA))

from src.plots.plotting_utils import generate_extent  # noqa: E402

DEFAULT_ROWS: list[tuple[int, str]] = [
    (173, "TC Lee"),
    (200, "TC Kay"),
    (185, "TC Lan"),
]

# Cap forecast lead time used for plotting and landfall matching.
# Forecasts beyond this horizon are noisier and clutter the panel,
# so we drop any track points (and the landfalls derived from them)
# whose lead_time exceeds this cutoff.
MAX_FORECAST_LEAD = np.timedelta64(10, "D")

# Minimum number of valid (non-NaN) track points required for an
# init_time's forecast track to be drawn. Inits with fewer points
# are NaN'd out so they don't clutter the panel or contribute
# spurious short-track landfall stars.
MIN_TRACK_POINTS = 10

# (cache_key, display_name, per-case pickle dir, compute_tc_tracks.py flag)
# The pickle dir names must match the ``out_dir`` values in
# compute_tc_tracks.py. The HRES entry inherits that script's HRES WB2
# -> BB HRES fallback (compute writes the fallback result into
# hres_tc_tracks/ when the primary produces no detections), so no
# fallback wiring is needed at load time.
MODEL_COLS: list[tuple[str, str, str, str]] = [
    ("BB_AIFS", "AIFS", "aifs_bb_tc_tracks", "--run_bb_aifs"),
    ("BB_Pangu", "Pangu", "pang_bb_tc_tracks", "--run_bb_pangu"),
    ("BB_Graphcast", "GraphCast", "gc_bb_tc_tracks", "--run_bb_graphcast"),
    ("HRES", "IFS HRES", "hres_tc_tracks", "--run_hres"),
]

TC_TRACKS_ROOT = REPO_ROOT / "saved_data"
DEFAULT_OUTPUT = (
    REPO_ROOT / "graphics" / "paper" / "subplots" / "tc_3x4_eta_otis_lan.png"
)


def _apply_lead_time_cap(
    forecast_ds: Optional[xr.Dataset],
    max_lead: np.timedelta64 = MAX_FORECAST_LEAD,
) -> Optional[xr.Dataset]:
    """Drop forecast lead_times above ``max_lead``.

    Lead-time coords may be ``timedelta64`` or numeric hours; we
    handle both. Returns ``None`` unchanged. Returns the dataset
    unchanged when ``lead_time`` is missing or already empty.
    """
    if forecast_ds is None or "lead_time" not in forecast_ds.dims:
        return forecast_ds
    lt = forecast_ds.lead_time.values
    if lt.size == 0:
        return forecast_ds
    if np.issubdtype(lt.dtype, np.timedelta64):
        keep = lt <= max_lead
    else:
        max_hours = float(max_lead / np.timedelta64(1, "h"))
        keep = lt <= max_hours
    if keep.all():
        return forecast_ds
    return forecast_ds.isel(lead_time=np.where(keep)[0])


def _drop_short_tracks(
    forecast_ds: Optional[xr.Dataset],
    min_points: int = MIN_TRACK_POINTS,
) -> Optional[xr.Dataset]:
    """NaN out lat/lon/data for inits with < ``min_points`` valid points.

    A "track" here is the set of detection points sharing one
    ``init_time = valid_time - lead_time``. Inits whose track has
    fewer than ``min_points`` non-NaN positions are masked across
    every (lead_time, valid_time) cell, removing them from both
    the plotted lines and ``find_landfalls``.
    """
    if forecast_ds is None:
        return forecast_ds
    if "lead_time" not in forecast_ds.dims or "valid_time" not in forecast_ds.dims:
        return forecast_ds
    if "latitude" not in forecast_ds.coords:
        return forecast_ds

    lt_vals = forecast_ds.lead_time.values
    vt_vals = forecast_ds.valid_time.values
    # Empty forecasts (no TC detections) come back with size-0 coords whose
    # numpy dtype defaults to float64, which then breaks the timedelta
    # subtraction below. There's nothing to mask on an empty grid so we can
    # just return the dataset unchanged and let ``_forecast_has_detections``
    # downstream flip it to ``None``.
    if lt_vals.size == 0 or vt_vals.size == 0:
        return forecast_ds

    lat_da = forecast_ds.coords["latitude"].transpose("lead_time", "valid_time")
    lon_da = forecast_ds.coords["longitude"].transpose("lead_time", "valid_time")
    valid = ~np.isnan(lat_da.values)

    if not np.issubdtype(lt_vals.dtype, np.timedelta64):
        lt_vals = lt_vals.astype("timedelta64[h]")
    init_grid = vt_vals[None, :] - lt_vals[:, None]

    unique_inits, inverse = np.unique(init_grid.ravel(), return_inverse=True)
    counts = np.bincount(
        inverse, weights=valid.ravel().astype(np.int64),
    ).astype(np.int64)
    short_inits = unique_inits[counts < min_points]
    if short_inits.size == 0:
        return forecast_ds

    keep_2d = ~np.isin(init_grid, short_inits)
    keep_da = xr.DataArray(keep_2d, dims=("lead_time", "valid_time"))

    out = forecast_ds.assign_coords(
        latitude=lat_da.where(keep_da),
        longitude=lon_da.where(keep_da),
    )
    for var in list(out.data_vars):
        if {"lead_time", "valid_time"} <= set(out[var].dims):
            out[var] = out[var].transpose(
                "lead_time", "valid_time", ...,
            ).where(keep_da)
    return out


def _forecast_has_detections(ds: Optional[xr.Dataset]) -> bool:
    """Return True only if the forecast Dataset has at least one
    non-NaN track point along its (lead_time, valid_time) grid.

    ``len(ds)`` reports the number of variables and so always
    looks non-zero for the TC track Dataset, which is why we
    inspect the latitude coord directly.
    """
    if ds is None:
        return False
    if "latitude" not in ds.coords:
        return False
    if ds.sizes.get("lead_time", 0) == 0 or ds.sizes.get("valid_time", 0) == 0:
        return False
    return bool(np.any(~np.isnan(ds.coords["latitude"].values)))

EXTENT_CRS = ccrs.Mercator()
PADDING_DEG = 5.0
CELL_ASPECT = (4, 4.5)

TITLE_FONTSIZE = 22
STORM_LABEL_FONTSIZE = 22
CBAR_LABEL_FONTSIZE = 12
CBAR_TICK_FONTSIZE = 10


def _setup_colormap() -> mcolors.LinearSegmentedColormap:
    """Same cubehelix-style ramp used by figure5_tc_tracks.py."""
    cmap_colors = [
        "#ffffff", "#bde6fa", "#7bbae7", "#4892bd",
        "#49ae62", "#a7d051", "#f9d251", "#f7792f",
        "#e43d28", "#c11b24", "#921318",
    ]
    return mcolors.LinearSegmentedColormap.from_list(
        "custom_cubehelix", cmap_colors,
    )


def _wrap_lon(arr: np.ndarray) -> np.ndarray:
    """Normalize longitudes to [-180, 180] for Cartopy ax.plot."""
    return ((np.asarray(arr, dtype=float) + 180.0) % 360.0) - 180.0


def _add_basemap(ax) -> None:
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.25)
    ax.add_feature(
        cfeature.LAND, color="#e8e8e8", alpha=0.8, zorder=0,
    )
    ax.add_feature(cfeature.STATES, linewidth=0.15, alpha=0.4)
    ax.gridlines(
        crs=ccrs.PlateCarree(), draw_labels=False,
        linewidth=0.2, color="gray", alpha=0.25, linestyle="--",
    )


def _load_cases(case_ids: list[int]):
    """Return IndividualCase objects in the order of ``case_ids``."""
    all_cases = cases.load_ewb_events_yaml_into_case_list()
    by_id = {c.case_id_number: c for c in all_cases}
    missing = [cid for cid in case_ids if cid not in by_id]
    if missing:
        raise ValueError(f"Cases not found in events.yaml: {missing}")
    return [by_id[cid] for cid in case_ids]


def _load_case_pickle(dir_name: str, case_id: int) -> dict[str, Any]:
    """Read one ``{"target", "forecast"}`` pickle written by compute_tc_tracks.py.

    Raises ``FileNotFoundError`` with an actionable message rather than
    silently returning None -- a missing pickle would produce a blank panel
    with no indication of why.
    """
    pkl = TC_TRACKS_ROOT / dir_name / f"case_{case_id}.pkl"
    if not pkl.exists():
        raise FileNotFoundError(
            f"Missing per-case TC pickle {pkl}. Regenerate it with:\n"
            f"    python -m src.plots.compute_tc_tracks "
            f"--case_ids {case_id} <flag for this column>\n"
            f"where <flag> is one of --run_hres / --run_bb_aifs / "
            f"--run_bb_graphcast / --run_bb_pangu."
        )
    with open(pkl, "rb") as f:
        return pickle.load(f)


def load_tracks(case_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Assemble the ``{case_id: {"target", "forecasts": {...}}}`` structure.

    Reads one per-case pickle per (case, model column) from the dirs
    produced by ``compute_tc_tracks.py``. The IBTrACS target is identical
    across all model pickles for a given case so we take it from the first
    successful read. Forecasts that came back without detections at compute
    time are stored as ``None`` here, matching what ``plot_tc_panel`` expects.
    """
    _load_cases(case_ids)  # surfaces bad case_ids before touching the filesystem
    result: dict[int, dict[str, Any]] = {}

    for cid in case_ids:
        target: Optional[xr.Dataset] = None
        forecasts: dict[str, Optional[xr.Dataset]] = {}

        for cache_key, _display, dir_name, _flag in MODEL_COLS:
            payload = _load_case_pickle(dir_name, cid)
            if target is None:
                target = payload["target"]
            fc = payload.get("forecast")
            if not _forecast_has_detections(fc):
                fc = None
            forecasts[cache_key] = fc

        result[cid] = {"target": target, "forecasts": forecasts}

    return result


def _track_dataarray_from_forecast(forecast_ds: xr.Dataset) -> xr.DataArray:
    """Pick a representative DataArray for landfall detection.

    ``find_landfalls`` reads ``latitude``/``longitude``/``valid_time``
    coords and uses the data values for interpolation; any of the
    forecast dataset variables works.
    """
    for var in (
        "air_pressure_at_mean_sea_level",
        "surface_wind_speed",
    ):
        if var in forecast_ds.data_vars:
            return forecast_ds[var]
    return forecast_ds[list(forecast_ds.data_vars)[0]]


def _track_dataarray_from_target(target_ds: xr.Dataset) -> xr.DataArray:
    """Pick a representative DataArray for the IBTrACS target."""
    for var in (
        "air_pressure_at_mean_sea_level",
        "surface_wind_speed",
    ):
        if var in target_ds.data_vars:
            return target_ds[var]
    return target_ds[list(target_ds.data_vars)[0]]


def _iter_forecast_groups(forecast_ds: xr.Dataset):
    """Yield (init_time, lats, lons) per non-empty init_time group.

    The forecast detection dataset has dims ``(lead_time,
    valid_time)`` and lat/lon coords on both. Convert valid_time
    to init_time so we can group by initialization, drop NaN
    detections, and surface only init_times with at least one
    valid point.
    """
    if "init_time" not in forecast_ds.dims:
        forecast_ds = ewb.utils.convert_valid_time_to_init_time(forecast_ds)

    lat_coord = forecast_ds.coords["latitude"]
    lon_coord = forecast_ds.coords["longitude"]

    for it in forecast_ds.init_time.values:
        lats = lat_coord.sel(init_time=it).values.ravel()
        lons = _wrap_lon(lon_coord.sel(init_time=it).values.ravel())
        mask = ~(np.isnan(lats) | np.isnan(lons))
        if mask.any():
            yield it, lats[mask], lons[mask]


def _compute_landfalls(
    forecast_ds: Optional[xr.Dataset],
    target_ds: xr.Dataset,
):
    """Return (first_forecast_landfall_per_init, target_landfalls).

    The forecast dict is keyed by init_time -> (lon, lat) for the
    *first* predicted landfall from that init's track. We do not
    gate by IBTrACS-landfall matching, so every init that crosses
    land at least once contributes a star. ``target_landfalls`` is
    the list of ``(lon, lat)`` for every IBTrACS landfall on this
    case. Returns empty containers when nothing is detected.
    """
    target_track = _track_dataarray_from_target(target_ds)
    target_landfalls = ewb.calc.find_landfalls(target_track)
    target_points = []
    if len(target_landfalls) > 0:
        t_lats = target_landfalls.coords["latitude"].values
        t_lons = _wrap_lon(target_landfalls.coords["longitude"].values)
        target_points = list(zip(t_lons, t_lats))

    matched: dict[np.datetime64, tuple[float, float]] = {}
    if not _forecast_has_detections(forecast_ds):
        return matched, target_points

    forecast_track = _track_dataarray_from_forecast(forecast_ds)
    forecast_landfalls = ewb.calc.find_landfalls(forecast_track)
    if len(forecast_landfalls) == 0:
        return matched, target_points

    reduced = ewb.calc.select_first_forecast_landfall_per_init(
        forecast_landfalls
    )

    inits = np.atleast_1d(reduced.init_time.values)
    lats = np.atleast_1d(reduced.coords["latitude"].values)
    lons = _wrap_lon(np.atleast_1d(reduced.coords["longitude"].values))
    for it, lat, lon in zip(inits, lats, lons):
        if not np.isnan(lat) and not np.isnan(lon):
            matched[np.datetime64(it)] = (float(lon), float(lat))
    return matched, target_points


def _get_shared_extent(
    case_id: int,
    case_data: dict[str, Any],
):
    """Build a Mercator extent that contains all available tracks.

    Includes the IBTrACS analysis track and every model forecast
    we have detections for, plus a small lat/lon padding.
    """
    target_ds = case_data["target"]
    all_lons = list(_wrap_lon(target_ds.coords["longitude"].values))
    all_lats = list(target_ds.coords["latitude"].values)

    for forecast_ds in case_data["forecasts"].values():
        if not _forecast_has_detections(forecast_ds):
            continue
        lat_coord = forecast_ds.coords["latitude"].values.ravel()
        lon_coord = _wrap_lon(forecast_ds.coords["longitude"].values.ravel())
        mask = ~(np.isnan(lat_coord) | np.isnan(lon_coord))
        all_lons.extend(lon_coord[mask].tolist())
        all_lats.extend(lat_coord[mask].tolist())

    if not all_lons:
        return None

    center_lon = (min(all_lons) + max(all_lons)) / 2
    center_lat = (min(all_lats) + max(all_lats)) / 2
    lon_span = max(all_lons) - min(all_lons) + 2 * PADDING_DEG
    lat_span = max(all_lats) - min(all_lats) + 2 * PADDING_DEG

    zoom_lon = lon_span / 4
    zoom_lat = lat_span * CELL_ASPECT[0] / CELL_ASPECT[1] / 4
    zoom = max(zoom_lon, zoom_lat)
    return generate_extent(
        (center_lon, center_lat), zoom, CELL_ASPECT,
    )


def plot_tc_panel(
    forecast_ds: Optional[xr.Dataset],
    target_ds: xr.Dataset,
    ax,
    fig,
    extent,
    *,
    show_col_title: bool = False,
    col_title: str = "",
    storm_label: Optional[str] = None,
    add_colorbar: bool = True,
):
    """Render one (storm, model) panel with landfall markers."""
    _add_basemap(ax)
    if extent is not None:
        ax.set_extent(extent, crs=EXTENT_CRS)

    valid_groups: list[tuple[Any, np.ndarray, np.ndarray]] = []
    if _forecast_has_detections(forecast_ds):
        try:
            valid_groups = list(_iter_forecast_groups(forecast_ds))
        except Exception as exc:  # noqa: BLE001
            print(f"  -> could not group forecast tracks: {exc!r}")
            valid_groups = []

    if not valid_groups:
        ax.text(
            0.5, 0.5, "No forecast data",
            transform=ax.transAxes,
            ha="center", va="center",
            fontsize=10, color="gray", style="italic",
        )

    n_times = len(valid_groups)
    if n_times > 0:
        cmap = _setup_colormap()
        colors = cmap(np.linspace(0.2, 1 - 1 / (n_times + 1), n_times))
    else:
        colors = np.empty((0, 4))

    for i, (_it, lats_v, lons_v) in enumerate(valid_groups):
        ax.plot(
            lons_v, lats_v, "-",
            color=colors[i], alpha=0.85,
            linewidth=3, transform=ccrs.PlateCarree(),
        )

    target_lats = target_ds.coords["latitude"].values
    target_lons = _wrap_lon(target_ds.coords["longitude"].values)
    ax.plot(
        target_lons, target_lats,
        "-ok", transform=ccrs.PlateCarree(),
        linewidth=3, markersize=4,
        markeredgecolor="white", markeredgewidth=0.3,
    )

    matched_landfalls, target_landfall_points = _compute_landfalls(
        forecast_ds, target_ds,
    )

    init_to_color = {
        np.datetime64(it): colors[i]
        for i, (it, _, _) in enumerate(valid_groups)
    }
    for it, (lon, lat) in matched_landfalls.items():
        color = init_to_color.get(it, "magenta")
        ax.scatter(
            lon, lat, marker="*", s=180,
            facecolor=color, edgecolor="black", linewidth=0.8,
            transform=ccrs.PlateCarree(), zorder=6,
        )

    for lon, lat in target_landfall_points:
        ax.scatter(
            lon, lat, marker="X", s=130,
            facecolor="black", edgecolor="white", linewidth=0.8,
            transform=ccrs.PlateCarree(), zorder=7,
        )

    if show_col_title:
        ax.set_title(
            col_title, fontsize=TITLE_FONTSIZE, pad=8, loc="left",
        )

    if storm_label:
        ax.text(
            -0.02, 0.5, storm_label,
            transform=ax.transAxes,
            fontsize=STORM_LABEL_FONTSIZE,
            va="center", ha="right", rotation=90,
        )

    if add_colorbar and n_times > 0:
        init_datetimes = [pd.to_datetime(str(it)) for it, _, _ in valid_groups]
        cmap_ = ListedColormap(colors)
        norm_ = plt.Normalize(vmin=-0.5, vmax=n_times - 0.5)
        sm_ = ScalarMappable(cmap=cmap_, norm=norm_)
        sm_.set_array([])
        cbar = fig.colorbar(
            sm_, ax=ax, orientation="horizontal",
            pad=0.04, shrink=0.85, aspect=22,
        )
        cbar.set_label(
            "Init Date", fontsize=CBAR_LABEL_FONTSIZE, labelpad=2,
        )
        all_labels = [dt.strftime("%m/%d") for dt in init_datetimes]
        max_ticks = 5
        if n_times <= max_ticks:
            indices = list(range(n_times))
        else:
            indices = sorted(
                set(
                    np.round(
                        np.linspace(0, n_times - 1, max_ticks)
                    ).astype(int).tolist()
                )
            )
        cbar.set_ticks(np.array(indices, dtype=float))
        cbar.set_ticklabels(
            [all_labels[i] for i in indices],
            fontsize=CBAR_TICK_FONTSIZE,
        )
        cbar.ax.tick_params(
            labelsize=CBAR_TICK_FONTSIZE, pad=2, length=2,
        )
        cbar.outline.set_linewidth(0.4)
        cbar.outline.set_edgecolor("gray")
        cbar.ax.set_xlim(-0.5, n_times - 0.5)


def _add_shared_init_colorbar(fig, axes_grid):
    """Add one vertical init-time colorbar on the right side.

    The colorbar's top/bottom are pinned to the actual top of
    the topmost subplot row and the bottom of the bottommost
    subplot row (computed from realized axes bboxes, so it
    accounts for any Cartopy aspect shrinking). Styling matches
    Figure 5 in the paper: 0.015-wide vertical bar, no ticks,
    single rotated label combining "Later Init Times",
    "Initialization Date", and "Earlier Init Times" with arrows.
    """
    fig.canvas.draw()  # finalize axes positions for get_position()

    top_y = max(
        ax.get_position().y1 for ax in axes_grid[0] if ax is not None
    )
    bottom_y = min(
        ax.get_position().y0 for ax in axes_grid[-1] if ax is not None
    )
    right_x = max(
        ax.get_position().x1
        for row in axes_grid for ax in row if ax is not None
    )

    cbar_left = min(right_x + 0.005, 0.965)
    cbar_width = 0.015

    cax = fig.add_axes(
        [cbar_left, bottom_y, cbar_width, top_y - bottom_y]
    )

    cmap = _setup_colormap()
    sample_colors = cmap(np.linspace(0.2, 0.95, 256))
    cmap_full = ListedColormap(sample_colors)
    norm = plt.Normalize(vmin=0.0, vmax=1.0)
    sm = ScalarMappable(cmap=cmap_full, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cax, orientation="vertical")
    cbar.set_ticks([])
    cbar.outline.set_linewidth(0.4)
    cbar.outline.set_edgecolor("gray")
    cbar.ax.set_ylabel(
        "\u2190 Later Init Times              Initialization Date"
        "              Earlier Init Times \u2192",
        rotation=270,
        labelpad=14,
        fontsize=14,
        va="center",
    )


def build_figure(
    rows: list[tuple[int, str]],
    output_path: Path,
) -> Path:
    """Load per-case tracks from disk and render the 3 x 4 figure."""
    case_ids = [cid for cid, _ in rows]
    cache = load_tracks(case_ids)

    for cid in case_ids:
        forecasts = cache[cid]["forecasts"]
        for key in list(forecasts.keys()):
            ds = _apply_lead_time_cap(forecasts[key])
            forecasts[key] = _drop_short_tracks(ds)

    n_rows = len(rows)
    n_cols = len(MODEL_COLS)

    fig = plt.figure(figsize=(4 * n_cols, 4.5 * n_rows))
    gs = GridSpec(
        n_rows, n_cols, figure=fig,
        left=0.04, right=0.92, top=0.94, bottom=0.06,
        wspace=0.05, hspace=0.10,
    )

    row_extents = {
        cid: _get_shared_extent(cid, cache[cid])
        for cid, _ in rows
    }

    axes_grid: list[list[Optional[Any]]] = [
        [None] * n_cols for _ in range(n_rows)
    ]
    for row_idx, (case_id, storm_label) in enumerate(rows):
        extent = row_extents[case_id]
        target_ds = cache[case_id]["target"]
        for col_idx, (cache_key, display_name, _, _) in enumerate(MODEL_COLS):
            forecast_ds = cache[case_id]["forecasts"].get(cache_key)
            ax = fig.add_subplot(
                gs[row_idx, col_idx], projection=EXTENT_CRS,
            )
            axes_grid[row_idx][col_idx] = ax
            plot_tc_panel(
                forecast_ds=forecast_ds,
                target_ds=target_ds,
                ax=ax,
                fig=fig,
                extent=extent,
                show_col_title=(row_idx == 0),
                col_title=display_name,
                storm_label=storm_label if col_idx == 0 else None,
                add_colorbar=False,
            )

    _add_shared_init_colorbar(fig, axes_grid)

    legend_handles = [
        Line2D(
            [0], [0], marker="*", color="w",
            markerfacecolor="lightgray", markeredgecolor="black",
            markersize=14, label="Forecast landfall (per init)",
        ),
        Line2D(
            [0], [0], marker="X", color="w",
            markerfacecolor="black", markeredgecolor="white",
            markersize=12, label="IBTrACS landfall",
        ),
        Line2D(
            [0], [0], marker="o", color="black",
            markerfacecolor="black", markeredgecolor="white",
            markersize=6, linewidth=2.5, label="IBTrACS track",
        ),
    ]
    fig.legend(
        handles=legend_handles, loc="lower center",
        ncol=3, frameon=False, fontsize=11,
        bbox_to_anchor=(0.5, 0.0),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure to {output_path}")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a 3 x 4 tropical-cyclone track figure (rows are"
            " storms, columns are models) with forecast landfall"
            " markers and IBTrACS landfall markers."
        )
    )
    parser.add_argument(
        "--cases", type=int, nargs="+",
        default=[cid for cid, _ in DEFAULT_ROWS],
        metavar="CASE_ID",
        help=(
            "TC case_id_number(s) from events.yaml. Default: "
            f"{[cid for cid, _ in DEFAULT_ROWS]}"
        ),
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output PNG path. Default: {DEFAULT_OUTPUT}",
    )
    args = parser.parse_args()

    case_list = _load_cases(args.cases)
    label_overrides = {cid: lbl for cid, lbl in DEFAULT_ROWS}
    rows = [
        (c.case_id_number, label_overrides.get(c.case_id_number, c.title))
        for c in case_list
    ]
    build_figure(rows, Path(args.output))


if __name__ == "__main__":
    main()
