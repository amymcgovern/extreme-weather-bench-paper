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
MODEL_ROWS: list[tuple[str, str]] = [
    ("AIFS", "aifs_bb_heat_freeze_graphics"),
    ("Graphcast", "gc_bb_heat_freeze_graphics"),
    ("Pangu", "pang_bb_heat_freeze_graphics"),
    ("HRES", "hres_heat_freeze_graphics"),
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


def _anchor_label(anchor: str) -> str:
    return "peak day" if anchor == "peak_day" else "warmest daily min"


def _kind_for_event(event_type: str) -> str:
    return "heat" if event_type == "heat_wave" else "freeze"


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
        model_ds = _load_case(Path(model_dir), cid)
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
        f"Case {cid}: {my_case.title} on {my_case.start_date} "
        f"({_anchor_label(anchor)} {_fmt_anchor_time(anchor_ts)})",
        fontsize=TITLE_FONTSIZE, y=0.965,
    )

    out_dir = Path(basepath) / f"graphics/{event_type}"
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = "heat" if event_type == "heat_wave" else "freeze"
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
    args = parser.parse_args()

    basepath = str(Path.home() / "extreme-weather-bench-paper") + "/"
    saved_data_root = Path(basepath) / "saved_data"
    suffix = _anchor_suffix(args.anchor)
    model_dirs = [
        (label, saved_data_root / f"{name}{suffix}")
        for label, name in MODEL_ROWS
    ]
    era5_dir = saved_data_root / f"{TRUTH_ERA5_DIR}{suffix}"
    ghcn_dir = saved_data_root / f"{TRUTH_GHCN_DIR}{suffix}"

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
        f" mode={args.mode} n_jobs={args.n_jobs}",
        flush=True,
    )

    # Workers print their own progress live via ``print(..., flush=True)``.
    Parallel(n_jobs=args.n_jobs, backend="loky")(
        delayed(_plot_case)(
            c, args.anchor, model_dirs, era5_dir, ghcn_dir, basepath,
            mode=args.mode, diff_vmax=args.diff_vmax,
        )
        for c in ewb_cases
    )
    print("Done", flush=True)
