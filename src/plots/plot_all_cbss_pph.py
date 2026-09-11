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


def _vertical_pph_colorbar(fig, sm, ax, levels=None, label="Practically Perfect Hindcast"):
    """Draw a vertical PPH colorbar just to the right of ``ax``.

    Mirrors the file-local ``_vertical_colorbar`` helper in
    ``plot_all_heat_freeze.py``, hand-rolled rather than reusing
    ``plotting_utils.add_scorecard_colorbar_right`` because that helper
    hardcodes ``extend="both"``, which doesn't fit PPH's one-sided range.

    ``ax`` here spans multiple GridSpec rows, but Cartopy's ``GeoAxes``
    enforces an equal aspect ratio by shrinking+recentering the *drawn* axes
    box within that allocation rather than filling it -- ``get_position()``
    only reflects that shrink after a draw, and only when asked for the
    "active" (not "original") position. Without forcing the draw here, the
    colorbar would span the full (undrawn-on) GridSpec cell instead of
    sitting snug against the actual map.
    """
    fig.canvas.draw()
    pos = ax.get_position(original=False)
    gap, width = 0.012, 0.014
    cax = fig.add_axes([pos.x1 + gap, pos.y0, width, pos.height])
    cbar = fig.colorbar(sm, cax=cax, orientation="vertical", extend="max")
    if levels is not None:
        cbar.set_ticks(list(levels))
    cbar.set_label(label, fontsize=16, rotation=270, labelpad=18)
    cbar.ax.tick_params(labelsize=12)
    return cbar


def _plot_case(
    my_case,
    my_lsr,
    hres_dir: Path,
    gc_dir: Path,
    pang_dir: Path,
    aifs_dir: Path,
    era5_dir: Path,
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

    n_lead_cols = len(lead_times_to_plot)
    n_cols = n_lead_cols + 1  # + 1 truth column (ERA5 CBSS + PPH + LSR reports)
    n_rows = 4

    width_per_col = 3
    total_width = width_per_col * n_lead_cols + width_per_col * 1.05

    # Nominal margins used only to size total_height (below); the real
    # GridSpec recomputes top afterward to reserve a fixed number of inches
    # for the suptitle regardless of total_height (see below).
    _nominal_top, _nominal_bottom = 0.90, 0.10
    _probe_kwargs = dict(
        wspace=0.1, hspace=0.1,
        left=0.05, right=0.90, top=_nominal_top, bottom=_nominal_bottom,
        width_ratios=[1.0] * n_lead_cols + [1.05],
    )

    # Size the figure height to the case's bbox aspect ratio so Cartopy's
    # equal-aspect map axes don't need to shrink every panel to fit --
    # otherwise that shrink leaves uniform padding above/below every row
    # (worse the more elongated the case's bbox is), which reads as an
    # oversized gap between rows. Clamp both the aspect ratio and the
    # resulting height to a sane range so a pathologically thin/tall bbox
    # doesn't produce an absurd figure.
    lat_span = my_case.location.latitude_max - my_case.location.latitude_min
    lon_span = my_case.location.longitude_max - my_case.location.longitude_min
    case_aspect = (lat_span / lon_span) if lon_span else 1.0
    case_aspect = float(np.clip(case_aspect, 0.35, 2.0))

    # cell_width_frac/cell_height_frac are the GridSpec-implied fraction of
    # total figure width/height a lead-column cell occupies -- purely a
    # function of the GridSpec geometry above (margins, wspace/hspace,
    # width_ratios), independent of the figure's physical size. Solving
    # cell_height_inches / cell_width_inches == case_aspect for total_height
    # (given total_width is fixed) makes a lead-column cell's *drawn*
    # aspect already match case_aspect, so no shrink is needed.
    _probe_fig = plt.figure()
    _bottoms, _tops, _lefts, _rights = gridspec.GridSpec(
        n_rows, n_cols, **_probe_kwargs
    ).get_grid_positions(_probe_fig)
    plt.close(_probe_fig)
    cell_width_frac = _rights[0] - _lefts[0]
    cell_height_frac = _tops[0] - _bottoms[0]
    total_height = case_aspect * total_width * (cell_width_frac / cell_height_frac)
    total_height = float(np.clip(total_height, 6.0, 18.0))

    # Reserve a fixed ~1.2in at the top for the suptitle + truth-panel title
    # regardless of total_height -- a fixed *fraction* like the nominal 0.90
    # above gives fewer absolute inches once total_height shrinks for
    # wide-bbox cases, which collides the truth panel's title with the
    # figure suptitle.
    top = max(0.6, 1 - 1.2 / total_height)
    gs_kwargs = dict(_probe_kwargs, top=top)

    fig = plt.figure(figsize=(total_width, total_height))
    try:
        print(f"Plotting case {my_id}", flush=True)

        gs = gridspec.GridSpec(n_rows, n_cols, figure=fig, **gs_kwargs)
        axs = [[fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree())
                for j in range(n_lead_cols)] for i in range(n_rows)]
        axs = np.array(axs)
        # Truth panel sits in the top-right cell only (rows 1-3 of that
        # column are left unallocated), matching plot_all_ar.py's ERA5
        # column layout.
        truth_ax = fig.add_subplot(gs[0, n_lead_cols], projection=ccrs.PlateCarree())

        hres_raw = _load_case(hres_dir, my_id)
        gc_raw   = _load_case(gc_dir,   my_id)
        pang_raw = _load_case(pang_dir, my_id)
        aifs_raw = _load_case(aifs_dir, my_id)
        era5_raw = _load_case(era5_dir, my_id)

        # PPH contours and LSR report markers are shown only in the dedicated
        # truth-column panel below (show_overlay=False here), not repeated on
        # every one of the 4x5 model/lead panels.
        if hres_raw is not None:
            cbss_hres, pph_hres = hres_raw["cbss"], hres_raw["pph"]
            for i, lead_time_hours in enumerate(lead_times_to_plot):
                title = f"{lead_time_hours} hours"
                left_label = "HRES" if i == 0 else None
                plot_cbss_pph_panel(
                    cbss_hres, pph_hres, my_case, lsrs=my_lsr,
                    ax=axs[0, i], title=title, lead_time_hours=lead_time_hours,
                    gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                    left_label=left_label, show_overlay=False,
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
                    left_label=left_label, show_overlay=False,
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
                    left_label=left_label, show_overlay=False,
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
                    left_label=left_label, show_overlay=False,
                )
        else:
            print(f"Skipping AIFS for case {my_id}: no per-case pickle at {aifs_dir}/case_{my_id}.pkl", flush=True)

        # Truth column: ERA5 CBSS with PPH contours + LSR report markers on
        # top, in the top-right cell (matching plot_all_ar.py's ERA5 column).
        era5_bbox = dict(
            latitude_min=my_case.location.latitude_min,
            latitude_max=my_case.location.latitude_max,
            longitude_min=my_case.location.longitude_min,
            longitude_max=my_case.location.longitude_max,
        )
        if era5_raw is not None:
            _, _, legend_elements = plot_cbss_pph_panel(
                era5_raw["cbss"], era5_raw["pph"], my_case, lsrs=my_lsr,
                ax=truth_ax, title="ERA5 CBSS +\nPPH & LSR Reports",
                lead_time_hours=None,
                gridlines_kwargs={"show_left_labels": False, "show_bottom_labels": False},
                show_overlay=True,
            )
            if legend_elements:
                # Legend goes below the truth panel, in the row right under
                # it (otherwise unused), rather than overlaid on the map.
                legend_ax = fig.add_subplot(gs[1, n_lead_cols])
                legend_ax.axis("off")
                legend_ax.legend(
                    handles=legend_elements, loc="upper center",
                    frameon=False, fontsize=11,
                )
            pph_cmap, pph_norm, pph_levels = severe_utils.setup_pph_colormap_and_levels()
            sm_pph = plt.cm.ScalarMappable(cmap=pph_cmap, norm=pph_norm)
            sm_pph.set_array([])
            _vertical_pph_colorbar(fig, sm_pph, truth_ax, levels=pph_levels)
        else:
            print(f"Skipping ERA5 truth panel for case {my_id}: no per-case pickle at {era5_dir}/case_{my_id}.pkl", flush=True)
            plot_utils.add_geographic_features(truth_ax)
            lon_min, lon_max = plot_utils.convert_bbox_longitude(era5_bbox)
            truth_ax.set_extent(
                [lon_min, lon_max, era5_bbox["latitude_min"], era5_bbox["latitude_max"]],
                crs=ccrs.PlateCarree(),
            )
            plot_utils.setup_gridlines(
                truth_ax, show_left_labels=False, show_bottom_labels=False,
            )
            truth_ax.set_title("ERA5 CBSS +\nPPH & LSR Reports", fontsize=18)
            truth_ax.text(
                0.5, 0.5, "No ERA5 data", transform=truth_ax.transAxes,
                ha="center", va="center", fontsize=14, color="gray", style="italic",
            )

        cmap, norm, levels = severe_utils.setup_cbss_colormap_and_levels()
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        plot_utils.add_horizontal_colorbar_below(
            fig,
            sm,
            [axs[n_rows - 1, j] for j in range(n_lead_cols)],
            n_subplots=n_lead_cols,
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
    gridlines_kwargs={}, geographic_features_kwargs={}, left_label=None, show_overlay=True):
    """Plot one CBSS panel.

    ``lead_time_hours`` may be ``None`` when ``cbss`` has no lead_time
    dimension (e.g. an ERA5 truth panel computed via ``ewb.inputs.ERA5`` as a
    "forecast" source, which only carries a valid_time dim) -- in that case
    ``cbss.craven_brooks_significant_severe.squeeze()`` is already a plain
    (lat, lon) slice and is passed straight through.

    ``show_overlay`` controls whether PPH contours and LSR report markers are
    drawn on top of the CBSS fill. The per-model/per-lead panels pass
    ``show_overlay=False`` so those reference layers only appear once, in the
    dedicated truth-column panel (which calls this with ``show_overlay=True``,
    the default).
    """
    my_bbox = dict()
    my_bbox["latitude_min"] = my_case.location.latitude_min
    my_bbox["latitude_max"] = my_case.location.latitude_max
    my_bbox["longitude_min"] = my_case.location.longitude_min
    my_bbox["longitude_max"] = my_case.location.longitude_max


    try:
        if show_overlay:
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
            #
            # ``lsrs`` can legitimately be None for marginal-severe (non-event) cases
            # where no LSRs are associated with the case. Treat that as "no reports"
            # rather than an error so the CBSS/PPH panels still render.
            if lsrs is None:
                hail_data = pd.DataFrame(columns=["latitude", "longitude"])
                tornado_data = pd.DataFrame(columns=["latitude", "longitude"])
            else:
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
        else:
            my_pph = None
            hail_data = None
            tornado_data = None

        ax, mappable, legend_elements = severe_utils.plot_cbss_forecast_panel(
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
        return ax, mappable, legend_elements
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
        return None, None, []

    

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
    parser.add_argument(
        "--case_ids",
        nargs="+",
        default=[],
        help="Optional case_id_number filter, comma-separated (default: all cases).",
    )

    args = parser.parse_args()

    if len(args.case_ids) > 0:
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

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

    if args.case_ids is not None:
        wanted = set(args.case_ids)
        ewb_cases = [c for c in ewb_cases if c.case_id_number in wanted]

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
    era5_dir = Path(basepath) / f"saved_data/era5_severe_graphics{suffix}"

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
            hres_dir, gc_dir, pang_dir, aifs_dir, era5_dir,
            lead_times_to_plot,
            basepath,
        )
        for my_case in ewb_cases
    )
    for r in results:
        print(r, flush=True)