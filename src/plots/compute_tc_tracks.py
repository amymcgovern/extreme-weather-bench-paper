# setup all the imports
import argparse
import pickle
from pathlib import Path

from extremeweatherbench import cases, evaluate, inputs
from joblib import Parallel, delayed

from src.data.tc_forecast_setup import TropicalCycloneForecastSetup

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

# Shared IBTrACS target -- open once at module import so worker processes
# inherit the setup instead of re-opening the parquet per case per model.
ibtracs_target = inputs.IBTrACS()


def _maybe_load(obj):
    """Materialize a dask-backed xarray Dataset/DataArray to numpy, else pass through.

    `evaluate.run_pipeline` returns dask-backed data for gridded forecasts; a
    naive `pickle.dump` then serializes the whole task graph, which for BB
    icechunk models blows up the on-disk pickle to many times its logical size
    and forces every downstream `.sel(...)` to walk the dask scheduler
    (same pathology fixed in compute_ar_plot_data.py and compute_cbss_pph_examples.py).
    Duck-type `.load()` so this is a no-op for already-materialized objects
    such as IBTrACS DataFrames.
    """
    if hasattr(obj, "load"):
        return obj.load()
    return obj


def _process_case(
    case,
    forecast,
    out_dir: Path,
    overwrite: bool,
    fallback_forecast=None,
    label: str = "",
) -> str:
    """Compute TC target + forecast tracks for a single case and pickle them.

    Writes ``out_dir/case_{cid}.pkl`` as ``{"target": ..., "forecast": ...}``.
    Returns a short status string for logging.
    """
    cid = case.case_id_number
    out_path = out_dir / f"case_{cid}.pkl"
    if out_path.exists() and not overwrite:
        return f"[{label}] skip case {cid} (exists)"

    print(f"Computing TC tracks for {label} case {cid}", flush=True)

    target_data = evaluate.run_pipeline(
        case_metadata=case, input_data=ibtracs_target,
    )
    forecast_data = evaluate.run_pipeline(
        case_metadata=case, input_data=forecast, _target_dataset=target_data,
    )
    if len(forecast_data) == 0 and fallback_forecast is not None:
        print(f"Fallback forecast for {label} case {cid}", flush=True)
        forecast_data = evaluate.run_pipeline(
            case_metadata=case, input_data=fallback_forecast,
            _target_dataset=target_data,
        )
    if len(forecast_data) == 0:
        return f"[{label}] empty case {cid}"

    target_data = _maybe_load(target_data)
    forecast_data = _maybe_load(forecast_data)

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump({"target": target_data, "forecast": forecast_data}, f)
    return f"[{label}] ok case {cid}"


def _run_model(
    label: str,
    ewb_cases,
    forecast,
    out_dir: Path,
    n_jobs: int,
    overwrite: bool,
    fallback_forecast=None,
) -> None:
    """Dispatch per-case TC-track computation across worker processes for one model.

    Uses the ``loky`` (process) backend so each worker gets its own numba
    runtime (the TC track detection has parallel=True kernels with the same
    thread-safety caveats we hit on CBSS) and its own icechunk/arraylake
    session -- concurrent reads on a shared session serialize inside the Rust
    backend, negating threading benefits.
    """
    print(
        f"Computing TC tracks for {label} ({len(ewb_cases)} cases, "
        f"n_jobs={n_jobs})",
        flush=True,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    results = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(_process_case)(
            c, forecast, out_dir, overwrite,
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
        description="Run tropical cyclone track evaluation against EWB cases."
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
        help="Run CIRA FOURv2 evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_gc",
        action="store_true",
        default=False,
        help="Run CIRA Graphcast evaluation (default: False)",
    )
    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run CIRA Pangu evaluation (default: False)",
    )
    parser.add_argument(
        "--run_bb_aifs",
        action="store_true",
        default=False,
        help="Run BB AIFS evaluation (default: False)",
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
        "--n_jobs",
        type=int,
        default=1,
        help=(
            "Number of parallel worker processes per model (loky backend). "
            "Default: 1 (sequential)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help=(
            "Recompute cases whose per-case pickle already exists. "
            "Default: skip existing (resume-friendly)."
        ),
    )
    args = parser.parse_args()

    # Case ID parsing matches compute_ar_plot_data.py / compute_cbss_pph_examples.py:
    # accepts either `--case_ids 1 2 3` or `--case_ids "1,2,3"`.
    if len(args.case_ids) > 0:
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

    print(f"Case IDs: {args.case_ids}", flush=True)

    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "tropical_cyclone"]
    if args.case_ids is not None:
        ewb_cases = [n for n in ewb_cases if n.case_id_number in args.case_ids]

    saved_data_root = Path(basepath) / "saved_data"

    tc_forecast_setup = TropicalCycloneForecastSetup()

    # Cache forecast handles so each source is opened at most once per invocation.
    hres_forecast = None
    bb_hres_forecast = None
    cira_fourv2_forecast = None
    cira_gc_forecast = None
    cira_pangu_forecast = None
    bb_aifs_forecast = None
    bb_graphcast_forecast = None
    bb_pangu_forecast = None

    if args.run_hres:
        if hres_forecast is None:
            hres_forecast = tc_forecast_setup.get_hres_forecast()
        if bb_hres_forecast is None:
            bb_hres_forecast = tc_forecast_setup.get_bb_hres_forecast()
        _run_model(
            label="hres",
            ewb_cases=ewb_cases,
            forecast=hres_forecast,
            out_dir=saved_data_root / "hres_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            fallback_forecast=bb_hres_forecast,
        )

    if args.run_cira_fourv2:
        if cira_fourv2_forecast is None:
            cira_fourv2_forecast = tc_forecast_setup.get_cira_tc_forecast("Fourv2", "IFS")
        _run_model(
            label="fourv2_cira",
            ewb_cases=ewb_cases,
            forecast=cira_fourv2_forecast,
            out_dir=saved_data_root / "fourv2_cira_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_cira_gc:
        if cira_gc_forecast is None:
            cira_gc_forecast = tc_forecast_setup.get_cira_tc_forecast("Graphcast", "GFS")
        _run_model(
            label="gc_cira",
            ewb_cases=ewb_cases,
            forecast=cira_gc_forecast,
            out_dir=saved_data_root / "gc_cira_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_cira_pangu:
        if cira_pangu_forecast is None:
            cira_pangu_forecast = tc_forecast_setup.get_cira_tc_forecast("Pangu", "IFS")
        _run_model(
            label="pang_cira",
            ewb_cases=ewb_cases,
            forecast=cira_pangu_forecast,
            out_dir=saved_data_root / "pang_cira_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_aifs:
        if bb_aifs_forecast is None:
            bb_aifs_forecast = tc_forecast_setup.get_bb_tc_forecast("aifs-single")
        _run_model(
            label="aifs_bb",
            ewb_cases=ewb_cases,
            forecast=bb_aifs_forecast,
            out_dir=saved_data_root / "aifs_bb_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_graphcast:
        if bb_graphcast_forecast is None:
            bb_graphcast_forecast = tc_forecast_setup.get_bb_tc_forecast("graphcast")
        _run_model(
            label="gc_bb",
            ewb_cases=ewb_cases,
            forecast=bb_graphcast_forecast,
            out_dir=saved_data_root / "gc_bb_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_pangu:
        if bb_pangu_forecast is None:
            bb_pangu_forecast = tc_forecast_setup.get_bb_tc_forecast("panguweather")
        _run_model(
            label="pang_bb",
            ewb_cases=ewb_cases,
            forecast=bb_pangu_forecast,
            out_dir=saved_data_root / "pang_bb_tc_tracks",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    print("Done", flush=True)
