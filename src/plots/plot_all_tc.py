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
from matplotlib.lines import Line2D

from src.plots.tc_3x4_panel import (
    EXTENT_CRS,
    MODEL_COLS,
    TC_TRACKS_ROOT,
    _apply_lead_time_cap,
    _drop_short_tracks,
    _forecast_has_detections,
    _get_shared_extent,
    plot_tc_panel,
)


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


def _landfall_legend_handles() -> list[Line2D]:
    """Same three-element legend used by tc_3x4_panel.build_figure."""
    return [
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


def _plot_case(my_case, basepath: str) -> str:
    """Render one case's 1 x 4 model-column PNG. Runs in a worker process."""
    cid = my_case.case_id_number
    forecasts: dict[str, Optional[xr.Dataset]] = {}
    target: Optional[xr.Dataset] = None
    skips: list[str] = []

    print(f"Plotting case {cid}")

    for cache_key, _display, dir_name, flag in MODEL_COLS:
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

    n_cols = len(MODEL_COLS)
    fig = plt.figure(figsize=(4 * n_cols, 5.0))
    gs = GridSpec(
        1, n_cols, figure=fig,
        left=0.04, right=0.98, top=0.86, bottom=0.16,
        wspace=0.05,
    )

    for col_idx, (cache_key, display_name, _, _) in enumerate(MODEL_COLS):
        ax = fig.add_subplot(gs[0, col_idx], projection=EXTENT_CRS)
        plot_tc_panel(
            forecast_ds=forecasts[cache_key],
            target_ds=target,
            ax=ax,
            fig=fig,
            extent=extent,
            show_col_title=True,
            col_title=display_name,
            storm_label=my_case.title if col_idx == 0 else None,
            add_colorbar=True,
        )

    fig.suptitle(
        f"Case {cid}: {my_case.title} on {my_case.start_date}",
        fontsize=22,
    )
    fig.legend(
        handles=_landfall_legend_handles(),
        loc="lower center",
        ncol=3,
        frameon=False,
        fontsize=11,
        bbox_to_anchor=(0.5, 0.0),
    )

    out_path = Path(basepath) / f"graphics/tropical_cyclone/tc_case_{cid}.png"
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

    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
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
