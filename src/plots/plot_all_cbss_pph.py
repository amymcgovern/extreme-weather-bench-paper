# setup all the imports
import argparse
import pickle
from pathlib import Path

import matplotlib
# Force the non-interactive Agg backend before pyplot is imported. This is
# essential for the parallel plotting path (loky workers must not try to open
# a GUI display) and is harmless in the single-process path.
matplotlib.use("Agg")

import cartopy.crs as ccrs  # noqa: E402
import matplotlib.gridspec as gridspec  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import extremeweatherbench as ewb  # noqa: E402
from joblib import Parallel, delayed  # noqa: E402
from joblib.externals.loky import get_reusable_executor  # noqa: E402

import src.plots.plotting_utils as plot_utils  # noqa: E402
import src.plots.results_utils as results_utils  # noqa: E402
import src.plots.severe_convection_utils as severe_utils  # noqa: E402


def _load_case(model_dir: Path, case_id: int):
    """Load a single case's ``{"cbss": ..., "pph": ...}`` pickle.

    Returns None if the file doesn't exist.

    Older pickles produced by `compute_cbss_pph_examples.py` (before the
    materialize-before-pickle fix) store both `cbss` and `pph` as dask-backed
    xarray Datasets whose graphs dominate the pickle size (~106 MB on disk
    for ~1.5 MB of logical data on BB models) and make every downstream
    `.sel(...)` in the plot loop walk the dask scheduler. Force `.load()`
    here so we pay the materialization cost exactly once, regardless of
    pickle vintage. On new (numpy-backed) pickles this is a no-op.
    """
    p = model_dir / f"case_{case_id}.pkl"
    if not p.exists():
        return None
    with open(p, "rb") as f:
        payload = pickle.load(f)
    for k, v in list(payload.items()):
        if hasattr(v, "load"):
            payload[k] = v.load()
    return payload


def _plot_case(
    my_case,
    my_lsr,
    hres_dir: Path,
    gc_dir: Path,
    pang_dir: Path,
    aifs_dir: Path,
    lead_times_to_plot,
    basepath: str,
) -> str:
    """Build and save the 4-row severe-convection PNG for one case.

    Designed to be executed inside a joblib ``loky`` worker so many cases can
    render concurrently. Everything the worker needs (model directories, this
    case's LSRs, output path) is passed by value; no globals are referenced.
    Returns a short status string for logging.
    """
    my_id = my_case.case_id_number

    n_cols = len(lead_times_to_plot)
    n_rows = 4

    width_per_col = 3
    height_per_row = 3
    total_width = width_per_col * n_cols
    total_height = height_per_row * n_rows

    fig = plt.figure(figsize=(total_width, total_height))
    try:
        print(f"Plotting case {my_id}", flush=True)

        gs = gridspec.GridSpec(
            n_rows, n_cols, figure=fig,
            wspace=0.1, hspace=0.1,
            left=0.05, right=0.95, top=0.90, bottom=0.1,
        )
        axs = [[fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree())
                for j in range(n_cols)] for i in range(n_rows)]
        axs = np.array(axs)

        hres_raw = _load_case(hres_dir, my_id)
        gc_raw   = _load_case(gc_dir,   my_id)
        pang_raw = _load_case(pang_dir, my_id)
        aifs_raw = _load_case(aifs_dir, my_id)

        if hres_raw is not None:
            cbss_hres, pph_hres = hres_raw["cbss"], hres_raw["pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                title = f"{lead_time_hours} hours"
                left_label = "HRES" if i == 0 else None
                plot_cbss_pph_panel(
                    cbss_hres, pph_hres, my_case, lsrs=my_lsr,
                    ax=axs[0, i], title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label,
                )
        else:
            print(f"Skipping HRES for case {my_id}: no per-case pickle at {hres_dir}/case_{my_id}.pkl", flush=True)

        if gc_raw is not None:
            cbss_gc, pph_gc = gc_raw["cbss"], gc_raw["pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                left_label = "GraphCast" if i == 0 else None
                plot_cbss_pph_panel(
                    cbss_gc, pph_gc, my_case, lsrs=my_lsr, ax=axs[1, i],
                    title="", lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label,
                )
        else:
            print(f"Skipping GraphCast for case {my_id}: no per-case pickle at {gc_dir}/case_{my_id}.pkl", flush=True)

        if pang_raw is not None:
            cbss_pang, pph_pang = pang_raw["cbss"], pang_raw["pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                left_label = "Pangu" if i == 0 else None
                plot_cbss_pph_panel(
                    cbss_pang, pph_pang, my_case, lsrs=my_lsr, ax=axs[2, i],
                    title="", lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label,
                )
        else:
            print(f"Skipping Pangu for case {my_id}: no per-case pickle at {pang_dir}/case_{my_id}.pkl", flush=True)

        if aifs_raw is not None:
            cbss_aifs, pph_aifs = aifs_raw["cbss"], aifs_raw["pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                left_label = "AIFS" if i == 0 else None
                plot_cbss_pph_panel(
                    cbss_aifs, pph_aifs, my_case, lsrs=my_lsr, ax=axs[3, i],
                    title="", lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label,
                )
        else:
            print(f"Skipping AIFS for case {my_id}: no per-case pickle at {aifs_dir}/case_{my_id}.pkl", flush=True)

        cmap, norm, levels = severe_utils.setup_cbss_colormap_and_levels()
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        plot_utils.add_horizontal_colorbar_below(
            fig,
            sm,
            [axs[n_rows - 1, j] for j in range(n_cols)],
            n_subplots=n_cols,
            levels=levels,
            label=r"Craven-Brooks Significant Severe (m$^{3}$/s$^{3}$)",
            label_fontsize=24,
            tick_labelsize=18,
        )

        fig.suptitle(f"Case {my_id}: {my_case.title} on {my_case.start_date}", fontsize=32, y=0.98)
        out_path = basepath + f"graphics/severe/severe_case_{my_id}.png"
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        return f"ok case {my_id} -> {out_path}"
    finally:
        plt.close(fig)


# to plot the targets, we need to run the pipeline for each case and target

def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = ewb.inputs.PPH()
    pph = ewb.evaluate.run_pipeline(ewb_case, pph_target)
    cbss = ewb.evaluate.run_pipeline(ewb_case, forecast_source)

    return cbss, pph

def get_lsr_from_case_op(my_case, case_operators_with_targets_established):
    for (id, case_info) in case_operators_with_targets_established:
        if id == my_case.case_id_number:
            if case_info.attrs["source"] == "local_storm_reports":
                return case_info
   
def plot_cbss_pph_panel(cbss, pph, my_case, lsrs, ax=None, title=None, lead_time_hours=0, 
    gridlines_kwargs={}, geographic_features_kwargs={}, left_label=None):
    my_bbox = dict()
    my_bbox["latitude_min"] = my_case.location.latitude_min
    my_bbox["latitude_max"] = my_case.location.latitude_max
    my_bbox["longitude_min"] = my_case.location.longitude_min
    my_bbox["longitude_max"] = my_case.location.longitude_max

    
    try:
        # grab the valid time to plot and get the pph and lsrs for that time
        valid_time = cbss.craven_brooks_significant_severe.valid_time
        my_pph = pph.sel(valid_time=valid_time).practically_perfect_hindcast.squeeze()

        # grab the lsrs for this valid_time. Newer EWB versions return LSRs in a
        # sparse form: dims ``(valid_time, location)`` with ``latitude`` and
        # ``longitude`` as non-index coordinates on ``location`` (each entry is
        # one report). We can derive hail/tornado dataframes directly from that
        # sparse form -- no need to unstack via ``stack_dataarray_from_dims``,
        # which fails on the plain PandasIndex layout with
        # ``conflicting dimensions for multi-index product variables``.
        lsrs_sel = lsrs.sel(valid_time=valid_time)
        if "valid_time" in lsrs_sel.dims:
            lsrs_sel = lsrs_sel.squeeze("valid_time", drop=True)

        report_type_arr = np.asarray(lsrs_sel["report_type"].values).ravel()
        lat_arr = np.asarray(lsrs_sel["latitude"].values).ravel()
        lon_arr = np.asarray(lsrs_sel["longitude"].values).ravel()

        if report_type_arr.size == 0:
            hail_data = pd.DataFrame(columns=["latitude", "longitude"])
            tornado_data = pd.DataFrame(columns=["latitude", "longitude"])
        else:
            hail_mask = report_type_arr == 2
            tornado_mask = report_type_arr == 3
            hail_data = pd.DataFrame(
                {"latitude": lat_arr[hail_mask], "longitude": lon_arr[hail_mask]}
            )
            tornado_data = pd.DataFrame(
                {"latitude": lat_arr[tornado_mask], "longitude": lon_arr[tornado_mask]}
            )

        ax, mappable = severe_utils.plot_cbss_forecast_panel(
            cbss_data=cbss.craven_brooks_significant_severe.squeeze(),
            target_date=my_case.start_date,
            lead_time_hours=lead_time_hours,
            bbox=my_bbox,
            ax=ax,
            pph_data=my_pph,
            tornado_reports=tornado_data,
            hail_reports=hail_data,
            title=title,
            alpha=0.6,
            gridlines_kwargs=gridlines_kwargs,
            geographic_features_kwargs=geographic_features_kwargs,
            left_label=left_label,
        )
        return ax, mappable
    except Exception as e:
        # Last-resort fallback: something upstream of the panel plotting broke.
        # We log with the case id so the failure is diagnosable rather than
        # silently rendering an empty axis.
        case_id = getattr(my_case, "case_id_number", "?")
        print(
            f"Warning: plot_cbss_pph_panel failed for case {case_id}, lead={lead_time_hours}h: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )
        return None, None

    

def get_stats(results, forecast_source, my_case, lead_time_days=[1, 3, 5, 7, 10]):
    # list the statistics for each case
    tp_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='TruePositives', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    tp_mean = tp_all["value"].mean("case_id_number")

    fn_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='local_storm_reports', metric='FalseNegatives', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    fn_mean = fn_all["value"].mean("case_id_number")
    
    csi_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='CriticalSuccessIndex', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    csi_mean = csi_all["value"].mean("case_id_number")

    far_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='FalseAlarmRatio', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)

    far_mean = far_all["value"].mean("case_id_number")

    es_all = results_utils.subset_results_to_xarray(results_df=results, 
        forecast_source=forecast_source, 
        target_source='practically_perfect_hindcast', metric='EarlySignal', 
        case_id_list=[my_case.case_id_number], lead_time_days=lead_time_days)
    es_mean = es_all["value"].mean("case_id_number")
    
    return [tp_mean.values, fn_mean.values, csi_mean.values, far_mean.values, es_mean.values]

if __name__ == "__main__":
    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    # load in all of the events in the yaml file
    ewb_cases = ewb.cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]

    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = ewb.cases.build_case_operators(
        ewb_cases, ewb.defaults.get_brightband_evaluation_objects()
    )

    parser = argparse.ArgumentParser(
            description="Plot all CBSS and PPH cases."
    )
    parser.add_argument(
        "--marginal",
        action="store_true",
        default=False,
        help="Plot for marginal cases (default: False)",
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=1,
        help="Number of parallel worker processes for per-case plotting (loky "
             "backend). Default: 1 (sequential).",
    )

    args = parser.parse_args()

    if (args.marginal):
        # load the marginal severe cases
        marginal_severe_yaml_path = Path(ewb.__file__).parent / "data" / "marginal_severe_convection_cases.yaml"
        marginal_severe_cases = ewb.cases.load_individual_cases_from_yaml(marginal_severe_yaml_path)
        marginal_severe_cases = [n for n in marginal_severe_cases if n.event_type == "severe_convection"]
        marginal_severe_case_operators = ewb.cases.build_case_operators(
            marginal_severe_cases, ewb.defaults.get_brightband_evaluation_objects()
        )
        ewb_cases = marginal_severe_cases
        case_operators = marginal_severe_case_operators

    
    # build out all of the expected data to evalate the case (we need this so we can plot
    # the LSR reports)
    case_operators = ewb.cases.build_case_operators(
        ewb_cases, ewb.defaults.get_brightband_evaluation_objects()
    )

    # load in all the case info (note this takes awhile in non-parallel form as it has to
    # run all the target information for each case)
    # this will return a list of tuples with the case id and the target dataset
    print("running the pipeline for each case and target")
    parallel = Parallel(n_jobs=32, return_as="generator", backend="loky")
    case_operators_with_targets_established_generator = parallel(
        delayed(
            lambda co: (
                co.case_metadata.case_id_number,
                ewb.evaluate.run_pipeline(co.case_metadata, co.target),
            )
        )(case_operator)
        for case_operator in case_operators
    )
    case_operators_with_targets_established = list(
        case_operators_with_targets_established_generator
    )
    # this will throw a bunch of errors below but they're not consequential. this releases
    # the memory as it shuts down the workers
    get_reusable_executor().shutdown(wait=True)

    # Match how compute_cbss_pph_examples.py wrote the per-case dirs: only the
    # marginal-severe YAML uses its own tree. Regular severe runs (paper subsets
    # or full) all share the same per-case pickle directories -- the plot loop
    # skips cases whose pickle isn't present via _load_case returning None.
    suffix = "_marginal" if args.marginal else ""

    hres_dir = Path(basepath) / f"saved_data/hres_severe_graphics{suffix}"
    gc_dir   = Path(basepath) / f"saved_data/gc_bb_severe_graphics{suffix}"
    pang_dir = Path(basepath) / f"saved_data/pang_bb_severe_graphics{suffix}"
    aifs_dir = Path(basepath) / f"saved_data/aifs_bb_severe_graphics{suffix}"

    lead_times_to_plot = [10*24, 7*24, 5*24, 3*24, 24]

    # Precompute the LSR target for each case so workers get only the tiny bit
    # they need, rather than being shipped the whole target list.
    lsr_by_case = {
        cid: target
        for (cid, target) in case_operators_with_targets_established
        if getattr(target, "attrs", {}).get("source") == "local_storm_reports"
    }

    print(f"Plotting {len(ewb_cases)} cases (n_jobs={args.n_jobs})", flush=True)
    results = Parallel(n_jobs=args.n_jobs, backend="loky")(
        delayed(_plot_case)(
            my_case,
            lsr_by_case.get(my_case.case_id_number),
            hres_dir, gc_dir, pang_dir, aifs_dir,
            lead_times_to_plot,
            basepath,
        )
        for my_case in ewb_cases
    )
    for r in results:
        print(r, flush=True)