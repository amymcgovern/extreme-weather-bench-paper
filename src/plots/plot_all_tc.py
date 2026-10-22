"""Plot every tropical-cyclone case as a 1 x 4 model-column figure.

Iterates all ``event_type == "tropical_cyclone"`` cases from
``events.yaml`` and, for each case, produces one PNG at
``graphics/tropical_cyclone/tc_case_<id>.png`` with 4 model
columns (AIFS / Pangu / GraphCast / HRES IFS) sharing the same
Cartopy extent. Forecast tracks are colored by init_time, the
IBTrACS analysis track is drawn in black, and landfall markers
are added: stars at the first predicted landfall per init and
X markers at every IBTrACS landfall.

Per-case pickles are loaded from the directories written by
``src/plots/compute_tc_tracks.py`` (one pickle per (model, case)
under ``saved_data/<model>_tc_tracks/case_<id>.pkl``, each with
``{"target", "forecast"}`` and dask graphs already materialized).
Missing per-model pickles render "No forecast data" in that
column rather than aborting the case; a case with no pickles
across any model is skipped with a log line.

Panel-drawing internals (``plot_tc_panel``, ``_get_shared_extent``,
lead-time capping, short-track masking, ``MODEL_COLS``, etc.) are
reused from ``src/plots/tc_3x4_panel.py``.

Usage:
    python -m src.plots.plot_all_tc --n_jobs 8
    python -m src.plots.plot_all_tc --cases 173 200 185 --n_jobs 3
"""

import argparse
import pickle
from pathlib import Path
from typing import Any, Optional

# Force a non-interactive backend before any pyplot import so worker
# processes started by joblib don't try to attach to a display.
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import xarray as xr
from extremeweatherbench import cases
from joblib import Parallel, delayed
from matplotlib.gridspec import GridSpec

from src.plots.tc_3x4_panel import (
    EXTENT_CRS,
    MODEL_COLS,
    TC_TRACKS_ROOT,
    TrackMapExtent,
    _apply_lead_time_cap,
    _drop_short_tracks,
    _forecast_has_detections,
    _get_shared_extent,
    landfall_legend_handles,
    plot_tc_panel,
)


# Back-compat alias so external callers importing the underscore-prefixed
# name still work. New code should import ``landfall_legend_handles`` from
# ``tc_3x4_panel`` directly to avoid pulling in this module's ``Agg``
# backend side effect.
_landfall_legend_handles = landfall_legend_handles


def _load_one(dir_name: str, case_id: int) -> Optional[dict[str, Any]]:
    """Load one ``{"target", "forecast"}`` pickle, tolerant of missing files.

    Mirrors ``_load_case`` in ``src/plots/plot_all_ar.py``: returns
    ``None`` when the per-case pickle doesn't exist so the driver
    can note the skip and continue rendering the remaining columns.
    """
    pkl = TC_TRACKS_ROOT / dir_name / f"case_{case_id}.pkl"
    if not pkl.exists():
        return None
    with open(pkl, "rb") as f:
        return pickle.load(f)


def _plot_case(
    my_case,
    basepath: str,
    model_cols: Optional[list[tuple[str, str, str, str]]] = None,
    out_path: Optional[Path] = None,
    max_cols: Optional[int] = None,
    storm_row_labels: bool = True,
    ibtracs_below: bool = False,
    init_times: Optional[list] = None,
    ibtracs_thin: bool = False,
) -> str:
    """Render one case's 1 x N model-column PNG. Runs in a worker process.

    ``storm_row_labels=False`` drops the storm name drawn beside the first
    panel of each row (the suptitle already names the storm), which declutters
    multi-row figures.

    ``model_cols`` defaults to ``MODEL_COLS``; each entry is
    ``(cache_key, display_name, dir_name, flag)``. ``dir_name`` may be an
    absolute path to a per-case pickle dir outside ``TC_TRACKS_ROOT``.
    ``out_path`` overrides ``graphics/tropical_cyclone/tc_case_<id>.png``.
    ``max_cols`` wraps the model panels onto multiple rows (default: one row).
    ``init_times`` fixes the init-date colors across panels (and across figures
    given the same list); see ``plot_tc_panel``.
    """
    if model_cols is None:
        model_cols = MODEL_COLS
    cid = my_case.case_id_number
    forecasts: dict[str, Optional[xr.Dataset]] = {}
    target: Optional[xr.Dataset] = None
    skips: list[str] = []

    print(f"Plotting case {cid}")

    for cache_key, _display, dir_name, flag in model_cols:
        payload = _load_one(dir_name, cid)
        if payload is None:
            forecasts[cache_key] = None
            skips.append(
                f"missing {dir_name}/case_{cid}.pkl "
                f"(rerun compute_tc_tracks with {flag})"
            )
            continue
        if target is None:
            target = payload.get("target")
        fc = payload.get("forecast")
        fc = _apply_lead_time_cap(fc)
        fc = _drop_short_tracks(fc)
        forecasts[cache_key] = fc if _forecast_has_detections(fc) else None

    if target is None:
        return f"skip case {cid}: no per-case pickles found across any model"

    case_data = {"target": target, "forecasts": forecasts}
    extent = _get_shared_extent(cid, case_data)
    proj = extent.crs if isinstance(extent, TrackMapExtent) else EXTENT_CRS

    n_models = len(model_cols)
    n_cols = min(n_models, max_cols) if max_cols else n_models
    n_rows = -(-n_models // n_cols)
    # Slightly taller figure + smaller top region so the fontsize-22 suptitle
    # has clear headroom above the fontsize-22 per-column titles (previously
    # top=0.86 put the two title bands on top of each other). Margins are
    # fixed in inches (as on the 5.4in one-row figure) so wrapped rows keep
    # the same headroom.
    fig_height = 5.4 + 4.8 * (n_rows - 1)
    fig = plt.figure(figsize=(4 * n_cols, fig_height))
    gs = GridSpec(
        n_rows, n_cols, figure=fig,
        left=0.04, right=0.98,
        top=round(1 - 1.08 / fig_height, 6), bottom=round(0.81 / fig_height, 6),
        wspace=0.05, hspace=0.45,
    )

    for col_idx, (cache_key, display_name, _, _) in enumerate(model_cols):
        ax = fig.add_subplot(gs[col_idx // n_cols, col_idx % n_cols], projection=proj)
        plot_tc_panel(
            forecast_ds=forecasts[cache_key],
            target_ds=target,
            ax=ax,
            fig=fig,
            extent=extent,
            show_col_title=True,
            col_title=display_name,
            storm_label=my_case.title if storm_row_labels and col_idx % n_cols == 0 else None,
            ibtracs_below=ibtracs_below,
            ibtracs_thin=ibtracs_thin,
            add_colorbar=True,
            init_times=init_times,
        )

    fig.suptitle(
        f"Case {cid}: {my_case.title} on {my_case.start_date}",
        fontsize=22,
        y=round(1 - 0.162 / fig_height, 6),
    )
    fig.legend(
        handles=_landfall_legend_handles(),
        loc="lower center",
        ncol=3,
        frameon=False,
        fontsize=11,
        bbox_to_anchor=(0.5, 0.0),
    )

    if out_path is None:
        out_path = Path(basepath) / f"graphics/tropical_cyclone/tc_case_{cid}.png"
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    prefix = f"ok case {cid} -> {out_path}"
    if skips:
        return prefix + " | " + " ; ".join(skips)
    return prefix


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Plot every tropical-cyclone case as a 1 x 4 model-column "
            "figure using per-case pickles from compute_tc_tracks.py."
        )
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=1,
        help=(
            "Number of parallel worker processes for per-case plotting "
            "(loky backend, since matplotlib is not thread-safe). Default: 1."
        ),
    )
    parser.add_argument(
        "--cases",
        type=int,
        nargs="*",
        default=None,
        metavar="CASE_ID",
        help=(
            "Optional case_id_number filter. Default: every "
            "tropical_cyclone case in events.yaml."
        ),
    )
    args = parser.parse_args()

    basepath = str(Path.home() / "extreme-weather-bench-paper") + "/"

    ewb_cases = cases.load_ewb_cases()
    ewb_cases = [c for c in ewb_cases if c.event_type == "tropical_cyclone"]
    if args.cases:
        wanted = set(args.cases)
        ewb_cases = [c for c in ewb_cases if c.case_id_number in wanted]

    print(
        f"Plotting {len(ewb_cases)} TC cases with n_jobs={args.n_jobs}",
        flush=True,
    )

    results = Parallel(n_jobs=args.n_jobs, backend="loky")(
        delayed(_plot_case)(mc, basepath) for mc in ewb_cases
    )
    for r in results:
        print(r, flush=True)

    print("Done", flush=True)
