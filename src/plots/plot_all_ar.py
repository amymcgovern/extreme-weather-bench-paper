# setup all the imports
import argparse
import pickle
from pathlib import Path

# Force a non-interactive backend before any pyplot import so worker
# processes started by joblib don't try to attach to a display.
import matplotlib
matplotlib.use("Agg")

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import pandas as pd
from extremeweatherbench import (
    cases,
    defaults,
)
from joblib import Parallel, delayed
from matplotlib.cm import ScalarMappable

import src.plots.atmospheric_river_utils as ar_plot_utils


def _load_case(model_dir: Path, case_id: int):
    """Load a single case's IVT pickle from a per-case model directory.

    Returns None if the file doesn't exist.

    Older pickles produced by `compute_ar_plot_data.py` (before the
    materialize-before-pickle fix) store `integrated_vapor_transport` as a
    dask array with ~1000 tiny per-(lead_time, valid_time) tiles. Every
    subplot then walks that graph, which is ~200x slower per row than the
    equivalent numpy read. Force `.load()` here so a single case pays the
    materialization cost once, regardless of pickle vintage. On new pickles
    this is a no-op.
    """
    p = model_dir / f"case_{case_id}.pkl"
    if not p.exists():
        return None
    with open(p, "rb") as f:
        raw = pickle.load(f)
    if hasattr(raw, "load"):
        raw = raw.load()
    return raw


def _plot_era_only(
    my_case,
    era5_dir: Path,
    basepath: str,
) -> str:
    """Plot a standalone ERA5 figure for one case. Runs in a worker process."""
    my_id = my_case.case_id_number
    era5_raw = _load_case(era5_dir, my_id)
    if era5_raw is None:
        return (f"[era-only] skip case {my_id}: no per-case pickle at "
                f"{era5_dir}/case_{my_id}.pkl")

    print(f"Plotting ERA5 for case {my_id}: {my_case.title} on {my_case.start_date}", flush=True)

    era5_ivt, era5_ar_mask = ar_plot_utils.select_ivt_and_maks_era5(era5_raw)
    fig, ax = plt.subplots(
        figsize=(5, 5),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    ar_plot_utils.plot_ar_mask_single_timestep(
        ivt_data=era5_ivt,
        ar_mask=era5_ar_mask,
        title="ERA5",
        ax=ax,
        colorbar=True,
        show_axes=True,
    )
    ax.set_title(
        f"ERA5 for case {my_id}: {my_case.title} on {my_case.start_date}",
        fontsize=32,
    )
    out_path = Path(basepath) / f"graphics/atmospheric_river/era5_case_{my_id}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return f"[era-only] ok case {my_id} -> {out_path}"


def _plot_case(
    my_case,
    plot_era_separately: bool,
    hres_dir: Path,
    gc_dir: Path,
    pang_dir: Path,
    aifs_dir: Path,
    era5_dir: Path,
    lead_times_to_plot: list,
    basepath: str,
) -> str:
    """Plot all-models AR figure for one case. Runs in a worker process."""
    my_id = my_case.case_id_number

    hres_raw = _load_case(hres_dir, my_id)
    gc_raw = _load_case(gc_dir, my_id)
    pang_raw = _load_case(pang_dir, my_id)
    aifs_raw = _load_case(aifs_dir, my_id)
    era5_raw = _load_case(era5_dir, my_id)

    row_length = 5 if plot_era_separately else 4
    fig, axs = plt.subplots(
        row_length, len(lead_times_to_plot) + 1,
        figsize=(18, 2 * len(lead_times_to_plot)),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )

    skip_msgs = []

    if not plot_era_separately:
        if era5_raw is not None:
            era5_ivt, era5_ar_mask = ar_plot_utils.select_ivt_and_maks_era5(era5_raw)
            ar_plot_utils.plot_ar_mask_single_timestep(
                ivt_data=era5_ivt, ar_mask=era5_ar_mask,
                title="ERA5",
                ax=axs[0, len(lead_times_to_plot)],
                colorbar=False, show_axes=False,
            )
            for i in range(1, row_length):
                axs[i, len(lead_times_to_plot)].set_visible(False)
        else:
            skip_msgs.append(
                f"Skipping ERA5 for case {my_id}: no per-case pickle at "
                f"{era5_dir}/case_{my_id}.pkl"
            )

    def _plot_model_row(raw, row, model_label, model_dir):
        if raw is None:
            skip_msgs.append(
                f"Skipping {model_label} for case {my_id}: no per-case pickle at "
                f"{model_dir}/case_{my_id}.pkl"
            )
            return
        for i, lead_time_hours in enumerate(lead_times_to_plot):
            ivt, ar_mask = ar_plot_utils.select_ivt_and_maks(raw, lead_time_hours)
            if ivt is None or ar_mask is None:
                skip_msgs.append(
                    f"Skipping {model_label} for case {my_id}: missing ivt or "
                    "ar mask data in graphics object"
                )
                continue
            title = f"{lead_time_hours} hours" if row == 0 else None
            left_label = model_label if i == 0 else None
            ar_plot_utils.plot_ar_mask_single_timestep(
                ivt_data=ivt, ar_mask=ar_mask,
                title=title, ax=axs[row, i],
                colorbar=False, left_label=left_label,
            )

    _plot_model_row(hres_raw, 0, "HRES", hres_dir)
    _plot_model_row(gc_raw, 1, "Graphcast", gc_dir)
    _plot_model_row(pang_raw, 2, "Pangu", pang_dir)
    _plot_model_row(aifs_raw, 3, "AIFS", aifs_dir)

    # Colorbar below the bottom row spanning all lead-time columns.
    cmap, norm = ar_plot_utils.setup_atmospheric_river_colormap_and_levels()
    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])

    pos0 = axs[row_length - 1, 0].get_position(fig)
    pos3 = axs[row_length - 1, len(lead_times_to_plot)].get_position(fig)
    cbar_y = pos0.y0 - pos0.height * 0.3
    cbar_height = pos0.height * 0.15
    cbar_ax = fig.add_axes([pos0.x0, cbar_y, pos3.x1 - pos0.x0, cbar_height])
    cbar = fig.colorbar(sm, cax=cbar_ax, orientation="horizontal")
    cbar.set_label(r"Integrated Vapor Transport (kg m$^{-1}$ s$^{-1}$)", size=32)
    cbar.ax.tick_params(labelsize=24)

    fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32)
    out_path = Path(basepath) / f"graphics/atmospheric_river/ar_case_{my_id}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    prefix = f"ok case {my_id} -> {out_path}"
    if skip_msgs:
        return prefix + " | " + " ; ".join(skip_msgs)
    return prefix


if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    parser = argparse.ArgumentParser(
        description="Plot all atmospheric river cases."
    )
    parser.add_argument(
        "--plot_era_separately",
        action="store_true",
        default=False,
        help="Plot ERA5 separately (default: False)",
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
    args = parser.parse_args()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "atmospheric_river"]

    # for debugging, only look at one case (that happens to be lovely)
    # ewb_cases = [n for n in ewb_cases if n.case_id_number == 95]

    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = cases.build_case_operators(
        ewb_cases, defaults.get_brightband_evaluation_objects()
    )

    print("Loading in the results")
    # load in the results
    hres_ar_results = pd.read_pickle(basepath + "saved_data/hres_ar_results.pkl")
    gc_ar_results = pd.read_pickle(basepath + "saved_data/bb_graphcast_ar_results.pkl")
    pang_ar_results = pd.read_pickle(basepath + "saved_data/bb_pangu_ar_results.pkl")
    aifs_ar_results = pd.read_pickle(basepath + "saved_data/bb_aifs_ar_results.pkl")

    # per-case pickle directories (written by src/plots/compute_ar_plot_data.py)
    hres_dir = Path(basepath) / "saved_data/hres_ar_graphics"
    gc_dir = Path(basepath) / "saved_data/gc_bb_ar_graphics"
    pang_dir = Path(basepath) / "saved_data/pang_bb_ar_graphics"
    aifs_dir = Path(basepath) / "saved_data/aifs_bb_ar_graphics"
    era5_dir = Path(basepath) / "saved_data/era5_ar_graphics"

    lead_times_to_plot = [10*24, 7*24, 5*24, 3*24, 24]

    print(f"Plotting {len(ewb_cases)} cases with n_jobs={args.n_jobs}", flush=True)

    if args.plot_era_separately:
        era_results = Parallel(n_jobs=args.n_jobs, backend="loky")(
            delayed(_plot_era_only)(mc, era5_dir, basepath)
            for mc in ewb_cases
        )
        for r in era_results:
            print(r, flush=True)

    results = Parallel(n_jobs=args.n_jobs, backend="loky")(
        delayed(_plot_case)(
            mc, args.plot_era_separately,
            hres_dir, gc_dir, pang_dir, aifs_dir, era5_dir,
            lead_times_to_plot, basepath,
        )
        for mc in ewb_cases
    )
    for r in results:
        print(r, flush=True)

    print("Done", flush=True)
