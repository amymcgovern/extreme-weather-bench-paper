# setup all the imports
import argparse
import pickle
from pathlib import Path

from extremeweatherbench import (
    cases,
    evaluate,
)
from joblib import Parallel, delayed

from src.data.ar_forecast_setup import (
    AtmosphericRiverEvaluationSetup,
    AtmosphericRiverForecastSetup,
)


def _process_case(
    case,
    forecast,
    out_dir: Path,
    overwrite: bool,
    fallback_forecast=None,
    label: str = "",
) -> str:
    """Compute IVT for a single case and pickle it to ``out_dir/case_{id}.pkl``.

    Returns a short status string suitable for logging.
    """
    case_id = case.case_id_number
    out_path = out_dir / f"case_{case_id}.pkl"
    if out_path.exists() and not overwrite:
        return f"[{label}] skip case {case_id} (exists)"

    print(f"Computing IVT for {label} case {case_id}", flush=True)

    ivt = evaluate.run_pipeline(case, forecast)
    if len(ivt) == 0 and fallback_forecast is not None:
        print(f"Fallback forecast found for {label} case {case_id}", flush=True)
        ivt = evaluate.run_pipeline(case, fallback_forecast)
    if len(ivt) == 0:
        return f"[{label}] empty case {case_id}"

    # Materialize the derived IVT dask graph into plain numpy before pickling.
    # `run_pipeline` returns the derived variables as a per-tile dask array
    # (chunks=(lat, lon, 1, 1)), so a naive `pickle.dump` serializes ~1000
    # tiny tasks per variable per case. On BB models that inflates the pickle
    # to ~5x its logical size (~180 MB vs 37 MB) and forces plot_all_ar to
    # walk the graph on every subplot (~40s per model row vs ~0.2s materialized).
    ivt = ivt.load()

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(ivt, f)
    return f"[{label}] ok case {case_id}"


def _run_model(
    label: str,
    ewb_cases,
    forecast,
    out_dir: Path,
    n_jobs: int,
    overwrite: bool,
    fallback_forecast=None,
) -> None:
    """Dispatch per-case IVT computation across threads for one model."""
    print(f"Computing IVT for {label} ({len(ewb_cases)} cases, n_jobs={n_jobs})", flush=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    results = Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(_process_case)(
            c, forecast, out_dir, overwrite,
            fallback_forecast=fallback_forecast, label=label,
        )
        for c in ewb_cases
    )
    for r in results:
        print(r, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run atmospheric river evaluation against ExtremeWeatherBench cases."
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
        "--run_era5",
        action="store_true",
        default=False,
        help="Run ERA5 evaluation (default: False)",
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
        help="Number of parallel workers per model (threading backend). Default: 1",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Recompute cases whose per-case pickle already exists. Default: skip existing (resume-friendly).",
    )

    args = parser.parse_args()

    # make the basepath - change this to your local path
    basepath = Path.home() / "extreme-weather-bench-paper" / ""
    basepath = str(basepath) + "/"

    # convert the case ids to integers
    if len(args.case_ids) > 0:
        # split the list by commas and convert to integers
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

    print(f"Case IDs: {args.case_ids}", flush=True)

    atmospheric_river_forecast_setup = AtmosphericRiverForecastSetup()
    atmospheric_river_evaluation_setup = AtmosphericRiverEvaluationSetup()

    # load in all of the events in the yaml file
    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [n for n in ewb_cases if n.event_type == "atmospheric_river"]

    # if we are subsetting the cases, do it here
    if args.case_ids is not None:
        ewb_cases = [n for n in ewb_cases if n.case_id_number in args.case_ids]

    saved_data_root = Path(basepath) / "saved_data"

    # this is a hack to handle only opening icechunk once per model
    hres_ar_forecast = None
    bb_hres_ar_forecast = None
    cira_fourv2_ar_forecast = None
    gc_ar_forecast = None
    pang_ar_forecast = None
    bb_graphcast_ar_forecast = None
    bb_pangu_ar_forecast = None
    bb_aifs_ar_forecast = None
    era5 = None

    if args.run_hres:
        if hres_ar_forecast is None:
            hres_ar_forecast = atmospheric_river_forecast_setup.get_hres_forecast(include_ivt=True)
        if bb_hres_ar_forecast is None:
            bb_hres_ar_forecast = atmospheric_river_forecast_setup.get_bb_hres_forecast(include_ivt=True)
        _run_model(
            label="hres",
            ewb_cases=ewb_cases,
            forecast=hres_ar_forecast,
            out_dir=saved_data_root / "hres_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
            fallback_forecast=bb_hres_ar_forecast,
        )

    if args.run_cira_fourv2:
        if cira_fourv2_ar_forecast is None:
            cira_fourv2_ar_forecast = atmospheric_river_forecast_setup.get_cira_forecast("Fourv2", "IFS", include_ivt=True)
        _run_model(
            label="fourv2_cira",
            ewb_cases=ewb_cases,
            forecast=cira_fourv2_ar_forecast,
            out_dir=saved_data_root / "fourv2_cira_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_cira_gc:
        if gc_ar_forecast is None:
            gc_ar_forecast = atmospheric_river_forecast_setup.get_cira_forecast("Graphcast", "IFS", include_ivt=True)
        _run_model(
            label="gc_cira",
            ewb_cases=ewb_cases,
            forecast=gc_ar_forecast,
            out_dir=saved_data_root / "gc_cira_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_cira_pangu:
        if pang_ar_forecast is None:
            pang_ar_forecast = atmospheric_river_forecast_setup.get_cira_forecast("Pangu", "IFS", include_ivt=True)
        _run_model(
            label="pang_cira",
            ewb_cases=ewb_cases,
            forecast=pang_ar_forecast,
            out_dir=saved_data_root / "pang_cira_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_graphcast:
        if bb_graphcast_ar_forecast is None:
            bb_graphcast_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("graphcast", include_ivt=True)
        _run_model(
            label="gc_bb",
            ewb_cases=ewb_cases,
            forecast=bb_graphcast_ar_forecast,
            out_dir=saved_data_root / "gc_bb_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_pangu:
        if bb_pangu_ar_forecast is None:
            bb_pangu_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("panguweather", include_ivt=True)
        _run_model(
            label="pang_bb",
            ewb_cases=ewb_cases,
            forecast=bb_pangu_ar_forecast,
            out_dir=saved_data_root / "pang_bb_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_bb_aifs:
        if bb_aifs_ar_forecast is None:
            bb_aifs_ar_forecast = atmospheric_river_forecast_setup.get_bb_ar_forecast("aifs-single", include_ivt=True)
        _run_model(
            label="aifs_bb",
            ewb_cases=ewb_cases,
            forecast=bb_aifs_ar_forecast,
            out_dir=saved_data_root / "aifs_bb_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    if args.run_era5:
        if era5 is None:
            era5 = atmospheric_river_forecast_setup.get_era5(include_ivt=True)
        _run_model(
            label="era5",
            ewb_cases=ewb_cases,
            forecast=era5,
            out_dir=saved_data_root / "era5_ar_graphics",
            n_jobs=args.n_jobs,
            overwrite=args.overwrite,
        )

    print("Done", flush=True)
