# setup all the imports
import argparse
import importlib
import pickle
from pathlib import Path

import extremeweatherbench as ewb
from extremeweatherbench import data
from joblib import Parallel, delayed

from src.data.severe_forecast_setup import (
    SevereEvaluationSetup,
    SevereForecastSetup,
)

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"


def _process_case(
    case,
    forecast,
    out_dir: Path,
    overwrite: bool,
    pph_target,
    fallback_forecast=None,
    label: str = "",
) -> str:
    """Compute CBSS + PPH for a single case and pickle it as ``{"cbss": ..., "pph": ...}``."""
    cid = case.case_id_number
    out_path = out_dir / f"case_{cid}.pkl"
    if out_path.exists() and not overwrite:
        return f"[{label}] skip case {cid} (exists)"

    print(f"Computing CBSS + PPH for {label} case {cid}", flush=True)

    cbss = ewb.evaluate.run_pipeline(case, forecast)
    if len(cbss) == 0 and fallback_forecast is not None:
        print(f"Fallback forecast found for {label} case {cid}", flush=True)
        cbss = ewb.evaluate.run_pipeline(case, fallback_forecast)
    if len(cbss) == 0:
        return f"[{label}] empty case {cid}"

    pph = ewb.evaluate.run_pipeline(case, pph_target)

    # Materialize the derived CBSS/PPH dask graphs to numpy before pickling.
    # `run_pipeline` returns per-tile dask arrays (cbss chunks=(1, 1, lat, lon)
    # over lead_time), so a naive `pickle.dump` serializes the graph and blows
    # the file up to ~70x its logical size on BB models (~106 MB pickle for
    # ~1.5 MB of data) and makes every downstream `.sel(...)` walk the graph.
    cbss = cbss.load()
    pph = pph.load()

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump({"cbss": cbss, "pph": pph}, f)
    return f"[{label}] ok case {cid}"


def _run_model(
    label: str,
    ewb_cases,
    forecast,
    out_dir: Path,
    n_jobs: int,
    overwrite: bool,
    pph_target,
    fallback_forecast=None,
) -> None:
    """Dispatch per-case CBSS+PPH computation across worker processes for one model.

    Uses the ``loky`` (process) backend rather than threads because the CBSS
    pipeline calls into numba ``parallel=True`` kernels and the only numba
    threading layer available in this venv is ``workqueue``, which is not
    thread-safe (concurrent access aborts the interpreter). Each worker process
    gets its own numba runtime, sidestepping the issue.
    """
    print(
        f"Computing CBSS+PPH for {label} ({len(ewb_cases)} cases, n_jobs={n_jobs})",
        flush=True,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    results = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(_process_case)(
            c, forecast, out_dir, overwrite, pph_target,
            fallback_forecast=fallback_forecast, label=label,
        )
        for c in ewb_cases
    )
    for r in results:
        print(r, flush=True)


if __name__ == "__main__":
    # Bump NOFILE before joblib spawns workers so they don't inherit the
    # Linux default 1024 and crash mid-run with "Too many open files"
    # against the arraylake/icechunk backends.
    from src.data.fd_limit import raise_fd_soft_limit
    raise_fd_soft_limit()

    parser = argparse.ArgumentParser(
        description="Run severe evaluation against ExtremeWeatherBench cases."
    )
    parser.add_argument(
        "--run_hres",
        action="store_true",
        default=False,
        help="Run HRES evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_fourv2",
        action="store_true",
        default=False,
        help="Run FOURv2 evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_gc",
        action="store_true",
        default=False,
        help="Run GC evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run PANG evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_aifs",
        action="store_true",
        default=False,
        help="Run AIFS evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_graphcast",
        action="store_true",
        default=False,
        help="Run BB Graphcast evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_pangu",
        action="store_true",
        default=False,
        help="Run BB Pangu evaluation (default: False)",
    )

    parser.add_argument(
        "--case_ids",
        nargs="+",
        default=[],
        help="Case IDs to run (default: all)",
    )

    parser.add_argument(
        "--run_marginal",
        action="store_true",
        default=False,
        help="Use the marginal severe cases instead",
    )

    parser.add_argument(
        "--n_jobs",
        type=int,
        default=1,
        help="Number of parallel worker processes per model (loky backend; "
             "processes are used instead of threads because the CBSS numba "
             "kernels are not thread-safe in this venv). Default: 1",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Recompute cases whose per-case pickle already exists. Default: skip existing (resume-friendly).",
    )

    args = parser.parse_args()

    if len(args.case_ids) > 0:
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

    print(f"Case IDs: {args.case_ids}", flush=True)

    if args.run_marginal:
        events_yaml_file = importlib.resources.files(data).joinpath(
            "marginal_severe_convection_cases.yaml"
        )
        ewb_cases = ewb.cases.load_individual_cases_from_yaml(events_yaml_file)
        ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]
    else:
        ewb_cases = ewb.cases.load_ewb_events_yaml_into_case_list()
        ewb_cases = [n for n in ewb_cases if n.event_type == "severe_convection"]

    if args.case_ids is not None:
        ewb_cases = [n for n in ewb_cases if n.case_id_number in args.case_ids]

    # Only differentiate output dirs by which YAML the cases came from. Per-case
    # pickles are keyed by case_id, so subset runs via --case_ids just add or
    # overwrite files in the same directory as full runs; no separate output
    # tree is needed for that. Marginal cases come from a distinct YAML and get
    # their own tree to avoid confusion.
    suffix = "_marginal" if args.run_marginal else ""

    saved_data_root = Path(basepath) / "saved_data"

    # instantiate the PPH target once (was previously re-created for every case)
    pph_target = ewb.inputs.PPH()

    severe_forecast_setup = SevereForecastSetup()
    severe_evaluation_setup = SevereEvaluationSetup()

    # hack to open each icechunk / zarr forecast handle only once per model
    hres_severe_forecast = None
    bb_hres_severe_forecast = None
    cira_fourv2_severe_forecast = None
    gc_severe_forecast = None
    pang_severe_forecast = None
    bb_graphcast_severe_forecast = None
    bb_pangu_severe_forecast = None
    bb_aifs_severe_forecast = None

    if args.run_hres:
        if hres_severe_forecast is None:
            hres_severe_forecast = severe_forecast_setup.get_hres_severe_convection_forecast()
        if bb_hres_severe_forecast is None:
            bb_hres_severe_forecast = severe_forecast_setup.get_bb_hres_severe_convection_forecast()
        _run_model(
            label="hres",
            ewb_cases=ewb_cases,
            forecast=hres_severe_forecast,
            out_dir=saved_data_root / f"hres_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
            fallback_forecast=bb_hres_severe_forecast,
        )

    if args.run_cira_fourv2:
        if cira_fourv2_severe_forecast is None:
            cira_fourv2_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Fourv2", "IFS")
        _run_model(
            label="fourv2_cira",
            ewb_cases=ewb_cases,
            forecast=cira_fourv2_severe_forecast,
            out_dir=saved_data_root / f"fourv2_cira_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    if args.run_cira_gc:
        if gc_severe_forecast is None:
            gc_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Graphcast", "IFS")
        _run_model(
            label="gc_cira",
            ewb_cases=ewb_cases,
            forecast=gc_severe_forecast,
            out_dir=saved_data_root / f"gc_cira_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    if args.run_cira_pangu:
        if pang_severe_forecast is None:
            pang_severe_forecast = severe_forecast_setup.get_cira_severe_convection_forecast("Pangu", "IFS")
        _run_model(
            label="pang_cira",
            ewb_cases=ewb_cases,
            forecast=pang_severe_forecast,
            out_dir=saved_data_root / f"pang_cira_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    if args.run_bb_graphcast:
        if bb_graphcast_severe_forecast is None:
            bb_graphcast_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("graphcast")
        _run_model(
            label="gc_bb",
            ewb_cases=ewb_cases,
            forecast=bb_graphcast_severe_forecast,
            out_dir=saved_data_root / f"gc_bb_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    if args.run_bb_pangu:
        if bb_pangu_severe_forecast is None:
            bb_pangu_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("panguweather")
        _run_model(
            label="pang_bb",
            ewb_cases=ewb_cases,
            forecast=bb_pangu_severe_forecast,
            out_dir=saved_data_root / f"pang_bb_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    if args.run_bb_aifs:
        if bb_aifs_severe_forecast is None:
            bb_aifs_severe_forecast = severe_forecast_setup.get_bb_severe_convection_forecast("aifs-single")
        _run_model(
            label="aifs_bb",
            ewb_cases=ewb_cases,
            forecast=bb_aifs_severe_forecast,
            out_dir=saved_data_root / f"aifs_bb_severe_graphics{suffix}",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            pph_target=pph_target,
        )

    print("Done", flush=True)
