"""Plot every heat / freeze case as a per-case 4 x 5 forecast-evolution figure.

Iterates every ``event_type in {"heat_wave", "freeze"}`` case in
``events.yaml`` and, for each case, produces one PNG showing 4 model
rows (AIFS / GraphCast / Pangu / HRES) x 5 lead-time columns (10, 7, 5,
3, 1 days) of 2 m temperature valid at the case's anchor timestep, plus
two stacked truth panels on the right column: ERA5 gridded truth (row 0)
and GHCN station scatter (row 1). All panels share the same Celsius
colormap so forecast vs. truth is a direct visual comparison.

Anchors are set at compute time (see ``compute_heat_freeze_plot_data``)
and re-selected at plot time via ``--anchor {peak_day, max_low}``:

- ``peak_day`` (default; heat_wave + freeze): timestep of the ERA5
  spatial-mean max (heat) or min (freeze) inside the case window.
- ``max_low`` (heat_wave only): timestep of the warmest daily minimum,
  matching what ``ewb.metrics.MaximumLowestMeanAbsoluteError`` uses.
  Freeze cases are skipped with a log line.

Output PNGs land at
``graphics/heat_wave/heat_case_<id>[_maxlow].png`` and
``graphics/freeze/freeze_case_<id>.png``.

Usage:
    python -m src.plots.plot_all_heat_freeze --n_jobs 8
    python -m src.plots.plot_all_heat_freeze --anchor max_low --n_jobs 8
    python -m src.plots.plot_all_heat_freeze --case_ids 7 --n_jobs 1
"""

import argparse
import pickle
from pathlib import Path
from typing import Optional

# Force a non-interactive backend before any pyplot import so worker
# processes started by joblib don't try to attach to a display.
import matplotlib
matplotlib.use("Agg")

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from extremeweatherbench import cases
from joblib import Parallel, delayed
from matplotlib.cm import ScalarMappable
from matplotlib.gridspec import GridSpec

from src.plots.heat_freeze_utils import (
    celsius_colormap_and_normalize,
    celsius_diff_colormap_and_normalize,
)

LEAD_HOURS = [240, 168, 120, 72, 24]
LEAD_LABELS = ["10 days", "7 days", "5 days", "3 days", "1 day"]

# (label, subdirectory basename) -- suffix "_maxlow" appended when anchor=max_low.
# Row order matches the shared paper convention used by plot_all_ar.py
# and plot_all_cbss_pph.py: HRES first, then GraphCast, Pangu, AIFS.
# (plot_all_tc.py intentionally uses its own order.)
MODEL_ROWS: list[tuple[str, str]] = [
    ("HRES", "hres_heat_freeze_graphics"),
    ("Graphcast", "gc_bb_heat_freeze_graphics"),
    ("Pangu", "pang_bb_heat_freeze_graphics"),
    ("AIFS", "aifs_bb_heat_freeze_graphics"),
]

TRUTH_ERA5_DIR = "era5_heat_freeze_graphics"
TRUTH_GHCN_DIR = "ghcn_heat_freeze_graphics"

TITLE_FONTSIZE = 22
ROW_LABEL_FONTSIZE = 18
COL_TITLE_FONTSIZE = 16
TRUTH_TITLE_FONTSIZE = 14
CBAR_LABEL_FONTSIZE = 20
CBAR_TICK_FONTSIZE = 14

PADDING_DEG = 1.0


def _anchor_suffix(anchor: str) -> str:
    return "" if anchor == "peak_day" else "_maxlow"


def _output_suffix(anchor: str, marginal: bool) -> str:
    """Combine ``--marginal`` + ``--anchor`` into a compute-side dir suffix.

    Must stay in sync with ``compute_heat_freeze_plot_data._output_suffix``
    so ``plot_all_heat_freeze`` reads exactly what the compute stage wrote.
    """
    return ("_marginal" if marginal else "") + _anchor_suffix(anchor)


def _anchor_label(anchor: str) -> str:
    return "peak day" if anchor == "peak_day" else "warmest daily min"


def _kind_for_event(event_type: str) -> str:
    return "heat" if event_type == "heat_wave" else "freeze"


def _case_type_label(event_type: str, marginal: bool) -> str:
    """Human-readable type for figure titles: Heat / Freeze / Marginal."""
    if marginal:
        return "Marginal"
    return "Heat" if event_type == "heat_wave" else "Freeze"


def _round_to(x: float, step: float, mode: str) -> float:
    """Round ``x`` to the nearest multiple of ``step`` in the requested direction."""
    if mode == "down":
        return float(np.floor(x / step) * step)
    if mode == "up":
        return float(np.ceil(x / step) * step)
    raise ValueError(f"mode must be 'up' or 'down', got {mode!r}")


def _infer_abs_norm(
    era5_ds: xr.Dataset,
    ghcn_ds: Optional[xr.Dataset],
    model_datasets: list[Optional[xr.Dataset]],
    kind: str,
) -> tuple[mcolors.Colormap, mcolors.Normalize]:
    """Return an absolute-temperature colormap sized to the case's data.

    The fixed 0..45 C heat ramp (and -30..15 freeze ramp) from
    ``celsius_colormap_and_normalize`` bakes in the assumption that a
    heat_wave case lives in a hot climate. Marginal-temperature cases
    break that assumption -- many marginal ``heat_wave`` events run in
    boreal winter with temps well below 0 C, and the truth panels on
    the diff plot collapse to near-black because the inferno colormap
    saturates at its dark end.

    Strategy:
      * Gather every 2 m temperature value from the ERA5 grid, GHCN
        stations, and all model / lead panels for this case.
      * Take the 1st and 99th percentiles as robust min/max (guards
        against a single bad pixel spraying the range wide).
      * Pad by 2 C and snap to the nearest 5 C so the colorbar labels
        stay tidy.
      * Pick ``coolwarm`` centered on 0 C when the range crosses zero
        (visually intuitive: blue = cold, red = warm), otherwise fall
        back to the existing ``inferno`` / ``coolwarm`` ramps from
        ``celsius_colormap_and_normalize(kind)``.
    """
    values: list[np.ndarray] = []
    def _push(da_or_ds):
        if da_or_ds is None:
            return
        if isinstance(da_or_ds, xr.Dataset):
            if "surface_air_temperature" not in da_or_ds:
                return
            arr = da_or_ds["surface_air_temperature"].values
        else:
            arr = np.asarray(da_or_ds.values)
        finite = arr[np.isfinite(arr)]
        if finite.size:
            values.append(finite)

    _push(era5_ds)
    _push(ghcn_ds)
    for m in model_datasets:
        _push(m)

    if not values:
        # No data at all -- fall back to the fixed ramp so we don't crash.
        return celsius_colormap_and_normalize(kind=kind)

    all_vals_k = np.concatenate(values)
    all_vals_c = all_vals_k - 273.15
    lo = float(np.percentile(all_vals_c, 1))
    hi = float(np.percentile(all_vals_c, 99))
    if not np.isfinite(lo) or not np.isfinite(hi) or lo >= hi:
        return celsius_colormap_and_normalize(kind=kind)

    # Pad + snap for tidy colorbar ticks.
    vmin = _round_to(lo - 2, 5.0, "down")
    vmax = _round_to(hi + 2, 5.0, "up")

    # Diverging when the case straddles freezing, sequential otherwise.
    if vmin < 0 < vmax:
        # coolwarm center at 0 keeps sub-freezing blue and warm red so
        # the truth panels stay meteorologically intuitive.
        span = max(abs(vmin), abs(vmax))
        return plt.get_cmap("coolwarm"), mcolors.TwoSlopeNorm(
            vmin=vmin, vcenter=0.0, vmax=vmax
        ) if abs(vmin) != abs(vmax) else mcolors.Normalize(
            vmin=-span, vmax=span
        )
    # Entirely hot or entirely cold -- reuse the paper's default ramps
    # but with the inferred range so we don't waste the colormap.
    if vmax <= 0:
        cmap = plt.get_cmap("coolwarm")
    else:
        cmap = plt.get_cmap("inferno")
    return cmap, mcolors.Normalize(vmin=vmin, vmax=vmax)


def _load_case(directory: Path, case_id: int) -> Optional[xr.Dataset]:
    """Load one per-case pickle, tolerant of missing files."""
    pkl = directory / f"case_{case_id}.pkl"
    if not pkl.exists():
        return None
    with open(pkl, "rb") as f:
        return pickle.load(f)


def _add_basemap(ax) -> None:
    ax.coastlines(linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle=":")
    ax.add_feature(cfeature.STATES, linewidth=0.2, alpha=0.4)


def _plot_field_panel(
    ax,
    da: xr.DataArray,
    extent,
    cmap,
    norm,
    mask_ocean: bool = False,
    kelvin_to_celsius: bool = True,
) -> None:
    """Render one gridded 2 m T panel (forecast or ERA5) in Celsius.

    The BB model archives (AIFS / GraphCast / Pangu / HRES) come pre-masked
    with NaN over ocean, so ocean pixels render as the axes background.
    ERA5 has no such mask, so pass ``mask_ocean=True`` to overlay
    ``cfeature.OCEAN`` on top of the pcolormesh so the ERA5 panel visually
    matches the model panels.

    ``kelvin_to_celsius`` (default True) subtracts 273.15 before plotting;
    set False when ``da`` is already in the target units (e.g. a
    forecast-minus-truth difference in K == diff in C).
    """
    values = (da - 273.15).values if kelvin_to_celsius else da.values
    ax.pcolormesh(
        da["longitude"], da["latitude"], values,
        cmap=cmap, norm=norm,
        transform=ccrs.PlateCarree(), shading="auto",
    )
    if mask_ocean:
        ax.add_feature(
            cfeature.OCEAN, facecolor="white", edgecolor="none", zorder=3,
        )
    _add_basemap(ax)
    if extent is not None:
        ax.set_extent(extent, crs=ccrs.PlateCarree())


def _plot_ghcn_panel(
    ax,
    ghcn_ds: Optional[xr.Dataset],
    extent,
    cmap,
    norm,
) -> int:
    """Render the GHCN station scatter panel and return the station count."""
    _add_basemap(ax)
    if extent is not None:
        ax.set_extent(extent, crs=ccrs.PlateCarree())
    if ghcn_ds is None or ghcn_ds["surface_air_temperature"].size == 0:
        ax.text(
            0.5, 0.5, "No GHCN stations",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=11, color="gray", style="italic",
        )
        return 0
    lat = np.asarray(ghcn_ds["latitude"].values).ravel()
    lon = np.asarray(ghcn_ds["longitude"].values).ravel()
    t_c = np.asarray(ghcn_ds["surface_air_temperature"].values).ravel() - 273.15
    good = ~(np.isnan(lat) | np.isnan(lon) | np.isnan(t_c))
    if not good.any():
        ax.text(
            0.5, 0.5, "No GHCN stations",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=11, color="gray", style="italic",
        )
        return 0
    ax.scatter(
        lon[good], lat[good], c=t_c[good],
        cmap=cmap, norm=norm, s=25,
        edgecolor="black", linewidth=0.3,
        transform=ccrs.Geodetic(), zorder=5,
    )
    return int(good.sum())


def _empty_placeholder(ax, extent, text: str) -> None:
    _add_basemap(ax)
    if extent is not None:
        ax.set_extent(extent, crs=ccrs.PlateCarree())
    ax.text(
        0.5, 0.5, text,
        transform=ax.transAxes, ha="center", va="center",
        fontsize=11, color="gray", style="italic",
    )


def _build_extent(
    case,
    era5_ds: xr.Dataset,
) -> tuple[float, float, float, float]:
    """Case-bbox extent with a small pad; anchored on ERA5 grid coverage.

    We use ERA5's realized bbox (post-slice) so cases whose yaml bounds
    happen to sit off-grid still get a snug extent. The pad matches what
    heat_freeze_6panel uses (1 degree).
    """
    lat = era5_ds["latitude"].values
    lon = era5_ds["longitude"].values
    lat_min = float(np.min(lat)) - PADDING_DEG
    lat_max = float(np.max(lat)) + PADDING_DEG
    lon_min = float(np.min(lon)) - PADDING_DEG
    lon_max = float(np.max(lon)) + PADDING_DEG
    return (lon_min, lon_max, lat_min, lat_max)


def _fmt_anchor_time(anchor_ts: np.datetime64) -> str:
    return pd.to_datetime(anchor_ts).strftime("%Y-%m-%d %HZ")


def _plot_case(
    my_case,
    anchor: str,
    model_dirs: list[tuple[str, Path]],
    era5_dir: Path,
    ghcn_dir: Path,
    basepath: str,
    mode: str = "abs",
    diff_vmax: float = 10.0,
    marginal: bool = False,
) -> str:
    """Worker: render one per-case figure. Returns a status string.

    ``mode``:
        - ``"abs"`` (default): each model/lead panel shows absolute 2 m T
          on the case-appropriate heat/freeze Celsius colormap.
        - ``"diff"``: each model/lead panel shows (forecast - ERA5) at
          the anchor timestep, on a diverging RdBu_r ramp saturated at
          +/- ``diff_vmax`` C. Truth panels (ERA5 gridded + GHCN) stay
          on the absolute colormap so they remain interpretable as
          reference.
    """
    cid = my_case.case_id_number
    event_type = my_case.event_type

    print(
        f"plotting case {cid} ({event_type}, mode={mode}, anchor={anchor}):"
        f" {my_case.title}",
        flush=True,
    )

    era5_ds = _load_case(era5_dir, cid)
    if era5_ds is None:
        msg = (
            f"skip case {cid} ({event_type}): missing "
            f"{era5_dir}/case_{cid}.pkl (no ERA5 truth)"
        )
        print(msg, flush=True)
        return msg
    anchor_ts = np.datetime64(era5_ds["anchor_valid_time"].values)
    ghcn_ds = _load_case(ghcn_dir, cid)

    kind = _kind_for_event(event_type)
    # Preload every model's per-case pickle once; the row loop below reads
    # from this list instead of re-opening each pickle. Marginal cases also
    # feed the datasets into ``_infer_abs_norm`` to size the
    # absolute-temperature colormap dynamically.
    preloaded_models: list[Optional[xr.Dataset]] = [
        _load_case(Path(model_dir), cid) for _, model_dir in model_dirs
    ]
    if marginal:
        # Marginal ``heat_wave`` cases include boreal-winter events whose
        # temps run well below the fixed 0..45 C ramp -- infer per-case so
        # the truth panels stay legible. Regular curated heat/freeze cases
        # deliberately keep the fixed ramps so cross-case comparison in
        # the paper stays consistent.
        cmap_abs, norm_abs = _infer_abs_norm(
            era5_ds, ghcn_ds, preloaded_models, kind
        )
    else:
        cmap_abs, norm_abs = celsius_colormap_and_normalize(kind=kind)
    if mode == "diff":
        cmap_diff, norm_diff = celsius_diff_colormap_and_normalize(vmax=diff_vmax)
        era5_ref = era5_ds["surface_air_temperature"]
    else:
        cmap_diff, norm_diff = None, None
        era5_ref = None

    fig = plt.figure(figsize=(19, 11))
    n_rows, n_cols = 4, 6  # 5 lead cols + 1 truth col
    gs = GridSpec(
        n_rows, n_cols, figure=fig,
        left=0.045, right=0.99, top=0.90, bottom=0.09,
        wspace=0.05, hspace=0.22,
        width_ratios=[1.0] * 5 + [1.05],
    )
    extent = _build_extent(my_case, era5_ds)

    axes_lead: list[list] = [
        [None] * len(LEAD_HOURS) for _ in range(len(MODEL_ROWS))
    ]

    skip_msgs: list[str] = []
    for row_idx, (row_label, model_dir) in enumerate(model_dirs):
        model_ds = preloaded_models[row_idx]
        if model_ds is None:
            skip_msgs.append(f"missing model dir {model_dir}")
        for col_idx, lead_h in enumerate(LEAD_HOURS):
            ax = fig.add_subplot(
                gs[row_idx, col_idx], projection=ccrs.PlateCarree(),
            )
            axes_lead[row_idx][col_idx] = ax
            if row_idx == 0:
                ax.set_title(
                    LEAD_LABELS[col_idx],
                    fontsize=COL_TITLE_FONTSIZE, pad=6,
                )
            if col_idx == 0:
                ax.text(
                    -0.06, 0.5, row_label,
                    transform=ax.transAxes,
                    fontsize=ROW_LABEL_FONTSIZE,
                    ha="right", va="center", rotation=90,
                )
            if model_ds is None:
                _empty_placeholder(ax, extent, "No data")
                continue
            lead_td = np.timedelta64(lead_h * 3600, "s")
            if lead_td not in model_ds["lead_time"].values:
                _empty_placeholder(ax, extent, "No data")
                continue
            snap = model_ds["surface_air_temperature"].sel(lead_time=lead_td)
            if snap.size == 0:
                _empty_placeholder(ax, extent, "No data")
                continue
            if mode == "diff":
                # Align to ERA5 grid by coord labels (some models are
                # lat-ascending, ERA5 is lat-descending); reindex_like
                # guarantees ordering matches so pcolormesh renders on
                # the same footprint.
                snap_aligned = snap.reindex_like(era5_ref)
                diff = snap_aligned - era5_ref
                _plot_field_panel(
                    ax, diff, extent, cmap_diff, norm_diff,
                    kelvin_to_celsius=False,
                )
            else:
                _plot_field_panel(ax, snap, extent, cmap_abs, norm_abs)

    era5_ax = fig.add_subplot(gs[0, 5], projection=ccrs.PlateCarree())
    _plot_field_panel(
        era5_ax, era5_ds["surface_air_temperature"], extent, cmap_abs, norm_abs,
        mask_ocean=True,
    )
    era5_ax.set_title(
        f"ERA5 ({_anchor_label(anchor)}\n{_fmt_anchor_time(anchor_ts)})",
        fontsize=TRUTH_TITLE_FONTSIZE, pad=6,
    )

    ghcn_ax = fig.add_subplot(gs[1, 5], projection=ccrs.PlateCarree())
    n_stations = _plot_ghcn_panel(ghcn_ax, ghcn_ds, extent, cmap_abs, norm_abs)
    ghcn_ax.set_title(
        f"GHCN (n={n_stations})",
        fontsize=TRUTH_TITLE_FONTSIZE, pad=6,
    )

    # Hide the two unused truth-column cells so their axes lines don't
    # print underneath the shared colorbar below.
    for r in (2, 3):
        placeholder = fig.add_subplot(gs[r, 5])
        placeholder.set_visible(False)

    bottom_row = [ax for ax in axes_lead[-1] if ax is not None]
    if bottom_row:
        pos0 = bottom_row[0].get_position(fig)
        pos_last_lead = bottom_row[-1].get_position(fig)
        pos_truth = era5_ax.get_position(fig)
        cbar_y = pos0.y0 - pos0.height * 0.28
        cbar_height = pos0.height * 0.12

        if mode == "diff":
            # Two colorbars: wide diff cbar under lead columns, small abs
            # cbar under truth column, so both scales stay legible.
            sm_diff = ScalarMappable(cmap=cmap_diff, norm=norm_diff)
            sm_diff.set_array([])
            diff_cbar_ax = fig.add_axes(
                [pos0.x0, cbar_y, pos_last_lead.x1 - pos0.x0, cbar_height],
            )
            diff_cbar = fig.colorbar(
                sm_diff, cax=diff_cbar_ax, orientation="horizontal",
                extend="both",
            )
            diff_cbar.set_label(
                "Forecast \u2212 ERA5 (\u00b0C)",
                fontsize=CBAR_LABEL_FONTSIZE, labelpad=2,
            )
            diff_cbar.ax.tick_params(labelsize=CBAR_TICK_FONTSIZE)

            sm_abs = ScalarMappable(cmap=cmap_abs, norm=norm_abs)
            sm_abs.set_array([])
            abs_cbar_ax = fig.add_axes(
                [pos_truth.x0, cbar_y, pos_truth.x1 - pos_truth.x0, cbar_height],
            )
            abs_cbar = fig.colorbar(
                sm_abs, cax=abs_cbar_ax, orientation="horizontal",
            )
            abs_cbar.set_label(
                "ERA5 / GHCN 2 m T (\u00b0C)",
                fontsize=CBAR_LABEL_FONTSIZE - 4, labelpad=2,
            )
            abs_cbar.ax.tick_params(labelsize=CBAR_TICK_FONTSIZE - 2)
        else:
            sm = ScalarMappable(cmap=cmap_abs, norm=norm_abs)
            sm.set_array([])
            cbar_ax = fig.add_axes(
                [pos0.x0, cbar_y, pos_truth.x1 - pos0.x0, cbar_height],
            )
            cbar = fig.colorbar(sm, cax=cbar_ax, orientation="horizontal")
            cbar.set_label(
                "2 m Temperature (\u00b0C)",
                fontsize=CBAR_LABEL_FONTSIZE, labelpad=2,
            )
            cbar.ax.tick_params(labelsize=CBAR_TICK_FONTSIZE)

    fig.suptitle(
        f"{_case_type_label(event_type, marginal)} case {cid}: "
        f"{my_case.title} on {my_case.start_date} "
        f"({_anchor_label(anchor)} {_fmt_anchor_time(anchor_ts)})",
        fontsize=TITLE_FONTSIZE, y=0.965,
    )

    if marginal:
        # Marginal cases are always heat_wave event_type (see marginal
        # _temperature_events.yaml). Route them to a distinct directory /
        # filename prefix so they never collide with regular heat cases.
        out_dir = Path(basepath) / "graphics/marginal_temperature"
        prefix = "marginal"
    else:
        out_dir = Path(basepath) / f"graphics/{event_type}"
        prefix = "heat" if event_type == "heat_wave" else "freeze"
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = _anchor_suffix(anchor)
    mode_suffix = "_diff" if mode == "diff" else ""
    out_path = out_dir / f"{prefix}_case_{cid}{suffix}{mode_suffix}.png"
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)

    status = f"ok case {cid} ({event_type}) -> {out_path}"
    if skip_msgs:
        status += " | " + " ; ".join(skip_msgs)
    print(status, flush=True)
    return status


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Plot every heat/freeze case as a 4x5 model-vs-lead figure with"
            " ERA5 and GHCN truth panels."
        )
    )
    parser.add_argument("--n_jobs", type=int, default=1)
    parser.add_argument(
        "--case_ids",
        type=int,
        nargs="*",
        default=None,
        help="Optional case_id_number filter. Default: every heat/freeze case.",
    )
    parser.add_argument(
        "--anchor",
        choices=["peak_day", "max_low"],
        default="peak_day",
        help=(
            "Anchor timestep used at compute time. max_low is heat-only."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["abs", "diff"],
        default="abs",
        help=(
            "abs (default): plot absolute 2 m T on the heat/freeze"
            " colormap. diff: plot forecast - ERA5 on a diverging RdBu_r"
            " colormap; truth panels stay on the absolute colormap."
        ),
    )
    parser.add_argument(
        "--diff_vmax",
        type=float,
        default=10.0,
        help="Saturation (deg C) for the diff colormap. Default: 10.",
    )
    parser.add_argument(
        "--marginal",
        action="store_true",
        default=False,
        help=(
            "Plot the marginal-temperature cases produced by"
            " ``compute_heat_freeze_plot_data.py --marginal`` from"
            " ``marginal_temperature_events.yaml``. Reads pickles from"
            " ``*_heat_freeze_graphics_marginal[_maxlow]/`` and saves PNGs"
            " under ``graphics/marginal_temperature/marginal_case_<id>[...].png``"
            " so regular heat/freeze outputs are never overwritten."
        ),
    )
    args = parser.parse_args()

    basepath = str(Path.home() / "extreme-weather-bench-paper") + "/"
    saved_data_root = Path(basepath) / "saved_data"
    suffix = _output_suffix(args.anchor, args.marginal)
    model_dirs = [
        (label, saved_data_root / f"{name}{suffix}")
        for label, name in MODEL_ROWS
    ]
    era5_dir = saved_data_root / f"{TRUTH_ERA5_DIR}{suffix}"
    ghcn_dir = saved_data_root / f"{TRUTH_GHCN_DIR}{suffix}"

    if args.marginal:
        # Mirror compute_heat_freeze_plot_data.py: marginal cases live in
        # a distinct EWB YAML with only ``heat_wave`` event_type entries.
        import importlib.resources
        from extremeweatherbench import data as _ewb_data
        yaml_path = importlib.resources.files(_ewb_data).joinpath(
            "marginal_temperature_events.yaml"
        )
        ewb_cases = cases.load_individual_cases_from_yaml(yaml_path)
        ewb_cases = [
            c for c in ewb_cases
            if c.event_type in {"heat_wave", "freeze"}
        ]
        print(
            f"[marginal] loaded {len(ewb_cases)} cases from"
            f" {yaml_path.name}",
            flush=True,
        )
    else:
        ewb_cases = cases.load_ewb_events_yaml_into_case_list()
        ewb_cases = [
            c for c in ewb_cases
            if c.event_type in {"heat_wave", "freeze"}
        ]
    if args.case_ids:
        wanted = set(args.case_ids)
        ewb_cases = [c for c in ewb_cases if c.case_id_number in wanted]

    if args.anchor == "max_low":
        pre = len(ewb_cases)
        ewb_cases = [c for c in ewb_cases if c.event_type == "heat_wave"]
        dropped = pre - len(ewb_cases)
        if dropped:
            print(
                f"[anchor=max_low] skipping {dropped} freeze cases; anchor"
                " is heat-only",
                flush=True,
            )

    print(
        f"Plotting {len(ewb_cases)} cases with anchor={args.anchor}"
        f" marginal={args.marginal} mode={args.mode} n_jobs={args.n_jobs}",
        flush=True,
    )

    # Workers print their own progress live via ``print(..., flush=True)``.
    Parallel(n_jobs=args.n_jobs, backend="loky")(
        delayed(_plot_case)(
            c, args.anchor, model_dirs, era5_dir, ghcn_dir, basepath,
            mode=args.mode, diff_vmax=args.diff_vmax,
            marginal=args.marginal,
        )
        for c in ewb_cases
    )
    print("Done", flush=True)
