"""Pre-compute per-case heat / freeze forecast + truth snapshots for plotting.

For each ``heat_wave`` / ``freeze`` case in ``events.yaml``, pickles one
xarray Dataset per model at ``saved_data/<model>_heat_freeze_graphics[_maxlow]/
case_<id>.pkl``. Each pickle holds ``surface_air_temperature`` on
``(lead_time, latitude, longitude)`` at the requested lead times valid at the
case's anchor timestep, plus a scalar ``anchor_valid_time`` coord and an
``anchor`` attr so the plotter can label panels correctly.

The anchor timestep is computed once per case from ERA5 with either
``--anchor peak_day`` (default; ERA5 spatial-mean max for heat / min for
freeze) or ``--anchor max_low`` (heat only; timestamp of the warmest daily
minimum, matching what ``ewb.metrics.MaximumLowestMeanAbsoluteError`` uses
internally). ``--anchor max_low`` skips freeze cases with a log line.

Usage:
    python -m src.plots.compute_heat_freeze_plot_data \
        --run_hres --run_bb_aifs --run_bb_graphcast --run_bb_pangu \
        --run_era5 --run_ghcn --n_jobs 4

    python -m src.plots.compute_heat_freeze_plot_data \
        --run_hres --run_era5 --run_ghcn --anchor max_low --n_jobs 4

Per-model / per-truth outputs mirror the ``compute_ar_plot_data.py`` / 
``compute_tc_tracks.py`` pattern: one small materialized pickle per case,
threading backend so the forecast archives can stay opened once per run.
"""

from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path
from typing import Optional

import extremeweatherbench as ewb
import numpy as np
import polars as pl
import xarray as xr
from extremeweatherbench import cases, utils
from joblib import Parallel, delayed

REPO_ROOT = Path(__file__).resolve().parents[2]
# heat_freeze_forecast_setup imports ``check_icechunk`` bare; keep it working
# without editing that module by making src/data importable as a top-level.
_SRC_DATA = REPO_ROOT / "src" / "data"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(_SRC_DATA) not in sys.path:
    sys.path.insert(0, str(_SRC_DATA))

from src.data.heat_freeze_forecast_setup import HeatFreezeForecastSetup  # noqa: E402

# Lead times shown in the per-case figure. Kept identical to plot_all_ar for
# consistency of the reader's mental model across figures.
LEAD_HOURS = [240, 168, 120, 72, 24]


def _load_era5_full() -> xr.Dataset:
    """Open the ERA5 heatwave target and normalize its axes for slicing.

    Renames ``time -> valid_time`` and folds longitudes into [-180, 180]
    so downstream case slicing works uniformly with negative-longitude
    case bboxes. Kept as a plain function so it can be reused by both
    anchor resolution and ``--run_era5``.
    """
    era5 = (
        ewb.defaults.era5_heatwave_target
        .open_and_maybe_preprocess_data_from_source()
    )
    if "time" in era5.dims and "valid_time" not in era5.dims:
        era5 = era5.rename({"time": "valid_time"})
    era5 = utils.convert_longitude_to_180(era5)
    return era5


def _open_forecast_for_plotting(forecast) -> xr.Dataset:
    """Return a forecast Dataset with EWB-canonical names and 180 longitudes.

    ``open_and_maybe_preprocess_data_from_source`` returns raw source
    variables (e.g. ``t2m`` for HRES); the forecast object's
    ``variable_mapping`` and ``preprocess`` are normally applied further
    down the pipeline in ``evaluate.run_pipeline``. We're bypassing that
    pipeline (nothing to derive -- ``surface_air_temperature`` is the raw
    variable) so replicate the rename + preprocess manually so we can
    ``sel`` by canonical name.
    """
    ds = forecast.open_and_maybe_preprocess_data_from_source()
    mapping = getattr(forecast, "variable_mapping", None) or {}
    rename = {k: v for k, v in mapping.items() if k in ds.variables}
    if rename:
        ds = ds.rename(rename)
    preprocess = getattr(forecast, "preprocess", None)
    if preprocess is not None:
        try:
            ds = preprocess(ds)
        except Exception as exc:  # noqa: BLE001
            # Some preprocess functions expect the full evaluate.run_pipeline
            # scaffolding around them; if that trips here, fall through with a
            # warning rather than crashing the whole compute run.
            print(
                f"[warn] forecast preprocess raised {exc!r}; using raw ds"
                " (surface_air_temperature should already be present after"
                " rename)",
                flush=True,
            )
    ds = utils.convert_longitude_to_180(ds)
    return ds


def _slice_era5_case_t2(
    era5: xr.Dataset,
    case,
) -> xr.DataArray:
    """Return ``2m_temperature`` on (valid_time, latitude, longitude) for one case."""
    return era5["2m_temperature"].sel(
        valid_time=slice(case.start_date, case.end_date),
        latitude=slice(case.location.latitude_max, case.location.latitude_min),
        longitude=slice(
            case.location.longitude_min, case.location.longitude_max
        ),
    )


def _resolve_anchor(
    spatial_mean: xr.DataArray,
    anchor: str,
    event_type: str,
) -> np.datetime64:
    """Compute the anchor valid_time for one case's spatial-mean 2 m T.

    ``spatial_mean`` must be a 1D DataArray on ``valid_time`` (already
    ``.mean(["latitude","longitude"])``).

    * ``anchor="peak_day"``: max (heat) or min (freeze) of the spatial
      mean over the case window.
    * ``anchor="max_low"`` (heat only): timestamp of the warmest daily
      minimum, computed with the same helper stack the
      ``MaximumLowestMeanAbsoluteError`` metric uses in
      ``_compute_metric`` (per-day groupby via
      ``utils.min_if_all_timesteps_present`` with the target's temporal
      resolution, then ``.max()``, then
      ``utils.maybe_get_closest_timestamp_to_center_of_valid_times``).
    """
    if anchor == "peak_day":
        idx_time = (
            spatial_mean.idxmax()
            if event_type == "heat_wave"
            else spatial_mean.idxmin()
        )
        return np.datetime64(idx_time.values)

    if anchor == "max_low":
        if event_type != "heat_wave":
            raise ValueError(
                "max_low anchor is heat-only; freeze cases should be filtered"
                " out before calling _resolve_anchor."
            )
        tr = utils.determine_temporal_resolution(spatial_mean)
        daily_min = spatial_mean.groupby("valid_time.dayofyear").map(
            utils.min_if_all_timesteps_present,
            time_resolution_hours=tr,
        )
        max_min_val = daily_min.max()
        candidates = spatial_mean.where(
            spatial_mean == max_min_val, drop=True
        ).valid_time
        anchor_time = utils.maybe_get_closest_timestamp_to_center_of_valid_times(
            candidates, spatial_mean.valid_time
        )
        return np.datetime64(anchor_time.values[0])

    raise ValueError(f"Unknown anchor {anchor!r}")


def _slice_bbox(
    da: xr.DataArray,
    case,
    lat_desc: bool,
) -> xr.DataArray:
    """Bbox-slice on ``latitude``/``longitude`` accounting for ordering.

    ERA5 stores latitudes descending (90 -> -90) so we pass max..min;
    BB models sometimes store ascending. Detect at runtime rather than
    hardcoding the convention.
    """
    lat_slice = (
        slice(case.location.latitude_max, case.location.latitude_min)
        if lat_desc
        else slice(case.location.latitude_min, case.location.latitude_max)
    )
    return da.sel(
        latitude=lat_slice,
        longitude=slice(
            case.location.longitude_min, case.location.longitude_max
        ),
    )


def _forecast_snapshot(
    fc_ds: xr.Dataset,
    case,
    anchor_valid_time: np.datetime64,
    lead_hours_list: list[int],
) -> Optional[xr.Dataset]:
    """Assemble a (lead_time, lat, lon) Dataset for one case + model.

    Each lead is selected independently so a lead-time gap in the archive
    (e.g. the init_time - lead pair isn't in the ledger) skips that column
    instead of failing the whole case. Returns ``None`` when no lead has
    usable data.
    """
    lat_desc = bool(
        fc_ds.latitude.size > 1
        and float(fc_ds.latitude.values[0]) > float(fc_ds.latitude.values[-1])
    )
    lead_slices: list[xr.DataArray] = []
    kept_leads: list[int] = []
    for lead_h in lead_hours_list:
        # Anchors can land on odd hours (max_low commonly picks the
        # overnight-min timestep) so init = anchor - lead doesn't always
        # match the archive's 00/12Z init cadence. Pick the nearest init
        # within +/- 12 h so the panel still reflects "roughly a <lead_h>
        # day forecast valid at the anchor" even when the exact pair is
        # missing.
        target_init = anchor_valid_time - np.timedelta64(lead_h, "h")
        lead_td = np.timedelta64(lead_h * 3600, "s")
        try:
            snap = fc_ds["surface_air_temperature"].sel(lead_time=lead_td)
        except KeyError:
            continue
        try:
            snap = snap.sel(
                init_time=target_init,
                method="nearest",
                tolerance=np.timedelta64(12, "h"),
            )
        except KeyError:
            continue
        snap = _slice_bbox(snap, case, lat_desc)
        if snap.size == 0:
            continue
        # Drop non-spatial coords that would break the concat below.
        for extra in ("init_time", "lead_time", "valid_time"):
            if extra in snap.coords:
                snap = snap.drop_vars(extra)
        lead_slices.append(snap)
        kept_leads.append(lead_h)

    if not lead_slices:
        return None

    stacked = xr.concat(lead_slices, dim="lead_time").assign_coords(
        lead_time=np.array(
            [h * 3600 for h in kept_leads], dtype="timedelta64[s]"
        ),
    )
    out = stacked.to_dataset(name="surface_air_temperature")
    out = out.assign_coords(anchor_valid_time=anchor_valid_time)
    return out.load()


def _era5_truth_snapshot(
    era5: xr.Dataset,
    case,
    anchor_valid_time: np.datetime64,
) -> Optional[xr.Dataset]:
    """Return ERA5 ``2m_temperature`` sliced to case bbox at ``anchor_valid_time``."""
    da = era5["2m_temperature"].sel(valid_time=anchor_valid_time)
    da = _slice_bbox(da, case, lat_desc=True)
    if da.size == 0:
        return None
    ds = da.rename("surface_air_temperature").to_dataset()
    ds = ds.assign_coords(anchor_valid_time=anchor_valid_time)
    return ds.load()


def _ghcn_truth_snapshot(
    ghcn_target,
    ghcn_lf: pl.LazyFrame,
    case,
    anchor_valid_time: np.datetime64,
) -> Optional[xr.Dataset]:
    """Return a sparse GHCN station snapshot at ``anchor_valid_time``.

    Uses the target's own ``subset_data_to_case`` + ``_custom_convert_to_dataset``
    so the parquet -> xarray path stays in one place. Longitudes come back
    on 0..360 -- we normalize to -180..180 so scatter plots line up with
    the forecast/ERA5 panels.
    """
    sub_lf = ghcn_target.subset_data_to_case(ghcn_lf, case)
    ds = ghcn_target._custom_convert_to_dataset(sub_lf)
    if len(ds.data_vars) == 0 or "surface_air_temperature" not in ds:
        return None
    if "valid_time" not in ds.dims:
        return None
    try:
        sel = ds.sel(valid_time=anchor_valid_time)
    except KeyError:
        return None
    if sel.sizes.get("location", 0) == 0:
        return None
    # Keep only the temperature var; the reset_coords() default promotes
    # station-level scalars (station id, name, etc.) to data_vars which
    # bloats the pickle for no plotting benefit. latitude / longitude are
    # already non-dim coords on ``location`` so they ride along.
    slim = sel[["surface_air_temperature"]]
    # Fold 0..360 lon values into -180..180 to match forecast/ERA5 panels.
    lon = slim["longitude"].values
    slim = slim.assign_coords(
        longitude=("location", np.where(lon > 180, lon - 360.0, lon))
    )
    slim = slim.assign_coords(anchor_valid_time=anchor_valid_time)
    return slim.load()


def _log(msg: str) -> str:
    """Print + return so callers can print live progress and still return status."""
    print(msg, flush=True)
    return msg


def _process_case_forecast(
    case,
    fc_ds: xr.Dataset,
    anchor_valid_time: np.datetime64,
    out_dir: Path,
    overwrite: bool,
    label: str,
    fallback_fc_ds: Optional[xr.Dataset] = None,
) -> str:
    """Compute + pickle one forecast snapshot for one case.

    When ``fallback_fc_ds`` is provided and the primary forecast returns
    no leads at the case anchor (e.g. the case predates the primary
    archive's time range), retry with the fallback. This mirrors the
    HRES WB2 -> BB HRES pattern in ``compute_ar_plot_data.py``.
    """
    cid = case.case_id_number
    out_path = out_dir / f"case_{cid}.pkl"
    if out_path.exists() and not overwrite:
        return _log(f"[{label}] skip case {cid} (exists)")

    _log(
        f"[{label}] computing case {cid} ({case.event_type}, "
        f"anchor={anchor_valid_time})"
    )
    try:
        ds = _forecast_snapshot(fc_ds, case, anchor_valid_time, LEAD_HOURS)
    except Exception as exc:  # noqa: BLE001
        return _log(f"[{label}] error case {cid}: {exc!r}")

    if ds is None and fallback_fc_ds is not None:
        _log(
            f"[{label}] primary empty for case {cid}; retrying fallback archive"
        )
        try:
            ds = _forecast_snapshot(
                fallback_fc_ds, case, anchor_valid_time, LEAD_HOURS,
            )
        except Exception as exc:  # noqa: BLE001
            return _log(f"[{label}] fallback error case {cid}: {exc!r}")

    if ds is None:
        return _log(
            f"[{label}] empty case {cid} (no leads available at anchor)"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(ds, f)
    return _log(f"[{label}] ok case {cid}")


def _process_case_era5(
    case,
    era5: xr.Dataset,
    anchor_valid_time: np.datetime64,
    out_dir: Path,
    overwrite: bool,
) -> str:
    cid = case.case_id_number
    out_path = out_dir / f"case_{cid}.pkl"
    if out_path.exists() and not overwrite:
        return _log(f"[era5] skip case {cid} (exists)")
    _log(
        f"[era5] computing case {cid} ({case.event_type}, "
        f"anchor={anchor_valid_time})"
    )
    try:
        ds = _era5_truth_snapshot(era5, case, anchor_valid_time)
    except Exception as exc:  # noqa: BLE001
        return _log(f"[era5] error case {cid}: {exc!r}")
    if ds is None:
        return _log(f"[era5] empty case {cid}")
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(ds, f)
    return _log(f"[era5] ok case {cid}")


def _process_case_ghcn(
    case,
    ghcn_target,
    ghcn_lf: pl.LazyFrame,
    anchor_valid_time: np.datetime64,
    out_dir: Path,
    overwrite: bool,
) -> str:
    cid = case.case_id_number
    out_path = out_dir / f"case_{cid}.pkl"
    if out_path.exists() and not overwrite:
        return _log(f"[ghcn] skip case {cid} (exists)")
    _log(
        f"[ghcn] computing case {cid} ({case.event_type}, "
        f"anchor={anchor_valid_time})"
    )
    try:
        ds = _ghcn_truth_snapshot(ghcn_target, ghcn_lf, case, anchor_valid_time)
    except Exception as exc:  # noqa: BLE001
        return _log(f"[ghcn] error case {cid}: {exc!r}")
    if ds is None:
        return _log(f"[ghcn] empty case {cid} (no stations at anchor)")
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(ds, f)
    return _log(f"[ghcn] ok case {cid}")


def _run_model(
    label: str,
    ewb_cases: list,
    fc_ds: xr.Dataset,
    anchor_times: dict[int, np.datetime64],
    out_dir: Path,
    n_jobs: int,
    overwrite: bool,
    fallback_fc_ds: Optional[xr.Dataset] = None,
) -> None:
    fallback_note = " (with fallback)" if fallback_fc_ds is not None else ""
    print(
        f"Computing {label} snapshots for {len(ewb_cases)} cases "
        f"(n_jobs={n_jobs}, out={out_dir}){fallback_note}",
        flush=True,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    # Workers print live via _log; discard the returned aggregate.
    Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(_process_case_forecast)(
            c, fc_ds, anchor_times[c.case_id_number],
            out_dir, overwrite, label,
            fallback_fc_ds=fallback_fc_ds,
        )
        for c in ewb_cases
        if c.case_id_number in anchor_times
    )


def _resolve_all_anchors(
    ewb_cases: list,
    anchor: str,
) -> dict[int, np.datetime64]:
    """Compute anchor times for every case up front (single ERA5 open)."""
    era5 = _load_era5_full()
    anchors: dict[int, np.datetime64] = {}
    for c in ewb_cases:
        try:
            t2 = _slice_era5_case_t2(era5, c)
            sm = t2.mean(["latitude", "longitude"]).compute()
            anchors[c.case_id_number] = _resolve_anchor(sm, anchor, c.event_type)
            print(
                f"[anchor] case {c.case_id_number} ({c.event_type}) "
                f"{anchor} -> {anchors[c.case_id_number]}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[anchor] error case {c.case_id_number}: {exc!r}",
                flush=True,
            )
    return anchors


def _anchor_suffix(anchor: str) -> str:
    return "" if anchor == "peak_day" else "_maxlow"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Compute per-case heat/freeze forecast + truth snapshots for the"
            " plot_all_heat_freeze pipeline."
        )
    )
    parser.add_argument("--run_hres", action="store_true", default=False)
    parser.add_argument("--run_bb_aifs", action="store_true", default=False)
    parser.add_argument("--run_bb_graphcast", action="store_true", default=False)
    parser.add_argument("--run_bb_pangu", action="store_true", default=False)
    parser.add_argument("--run_era5", action="store_true", default=False)
    parser.add_argument("--run_ghcn", action="store_true", default=False)
    parser.add_argument(
        "--anchor",
        choices=["peak_day", "max_low"],
        default="peak_day",
        help=(
            "Anchor timestep to build the per-case forecast around."
            " 'peak_day' (default) works for both heat and freeze."
            " 'max_low' is heat-only and matches the anchor used by"
            " MaximumLowestMeanAbsoluteError."
        ),
    )
    parser.add_argument(
        "--case_ids",
        nargs="+",
        default=[],
        help="Case IDs to run (default: all heat + freeze cases).",
    )
    parser.add_argument("--n_jobs", type=int, default=1)
    parser.add_argument("--overwrite", action="store_true", default=False)
    args = parser.parse_args()

    basepath = Path.home() / "extreme-weather-bench-paper"
    saved_data_root = basepath / "saved_data"

    if args.case_ids:
        args.case_ids = [int(n) for n in args.case_ids[0].split(",")]
    else:
        args.case_ids = None

    ewb_cases = cases.load_ewb_events_yaml_into_case_list()
    ewb_cases = [
        c for c in ewb_cases if c.event_type in {"heat_wave", "freeze"}
    ]
    if args.case_ids is not None:
        ewb_cases = [c for c in ewb_cases if c.case_id_number in args.case_ids]

    if args.anchor == "max_low":
        pre = len(ewb_cases)
        ewb_cases = [c for c in ewb_cases if c.event_type == "heat_wave"]
        dropped = pre - len(ewb_cases)
        if dropped:
            print(
                f"[anchor=max_low] skipping {dropped} freeze cases; anchor is"
                " heat-only",
                flush=True,
            )

    if not ewb_cases:
        print("No cases to process.", flush=True)
        sys.exit(0)

    suffix = _anchor_suffix(args.anchor)
    print(
        f"Resolving anchors ({args.anchor}) for {len(ewb_cases)} cases...",
        flush=True,
    )
    anchor_times = _resolve_all_anchors(ewb_cases, args.anchor)

    setup = HeatFreezeForecastSetup()

    # Open each forecast only once so all cases share the same handle.
    if args.run_hres:
        # WB2 HRES covers 2016 -> early 2023; BB HRES starts 2023-01-01. Using
        # WB2 as primary and BB as fallback (mirroring compute_ar_plot_data.py's
        # HRES pattern) means every case in the joint 2016..now window gets a
        # forecast to plot, and the output path stays ``hres_heat_freeze_graphics``.
        wb2_fc = setup.get_hres_heat_freeze_forecast()
        bb_fc = setup.get_bb_hres_heat_freeze_forecast()
        wb2_ds = _open_forecast_for_plotting(wb2_fc)
        bb_ds = _open_forecast_for_plotting(bb_fc)
        _run_model(
            "hres", ewb_cases, wb2_ds, anchor_times,
            saved_data_root / f"hres_heat_freeze_graphics{suffix}",
            args.n_jobs, args.overwrite,
            fallback_fc_ds=bb_ds,
        )

    if args.run_bb_aifs:
        fc = setup.get_bb_heat_freeze_forecast("aifs-single")
        fc_ds = _open_forecast_for_plotting(fc)
        _run_model(
            "aifs_bb", ewb_cases, fc_ds, anchor_times,
            saved_data_root / f"aifs_bb_heat_freeze_graphics{suffix}",
            args.n_jobs, args.overwrite,
        )

    if args.run_bb_graphcast:
        fc = setup.get_bb_heat_freeze_forecast("graphcast")
        fc_ds = _open_forecast_for_plotting(fc)
        _run_model(
            "gc_bb", ewb_cases, fc_ds, anchor_times,
            saved_data_root / f"gc_bb_heat_freeze_graphics{suffix}",
            args.n_jobs, args.overwrite,
        )

    if args.run_bb_pangu:
        fc = setup.get_bb_heat_freeze_forecast("panguweather")
        fc_ds = _open_forecast_for_plotting(fc)
        _run_model(
            "pang_bb", ewb_cases, fc_ds, anchor_times,
            saved_data_root / f"pang_bb_heat_freeze_graphics{suffix}",
            args.n_jobs, args.overwrite,
        )

    if args.run_era5:
        era5 = _load_era5_full()
        era5_out = saved_data_root / f"era5_heat_freeze_graphics{suffix}"
        era5_out.mkdir(parents=True, exist_ok=True)
        print(
            f"Computing ERA5 truth for {len(ewb_cases)} cases (n_jobs={args.n_jobs})",
            flush=True,
        )
        Parallel(n_jobs=args.n_jobs, backend="threading")(
            delayed(_process_case_era5)(
                c, era5, anchor_times[c.case_id_number], era5_out, args.overwrite,
            )
            for c in ewb_cases
            if c.case_id_number in anchor_times
        )

    if args.run_ghcn:
        ghcn_out = saved_data_root / f"ghcn_heat_freeze_graphics{suffix}"
        ghcn_out.mkdir(parents=True, exist_ok=True)
        heat_tgt = ewb.defaults.ghcn_heatwave_target
        freeze_tgt = ewb.defaults.ghcn_freeze_target
        heat_lf = heat_tgt.open_and_maybe_preprocess_data_from_source()
        freeze_lf = freeze_tgt.open_and_maybe_preprocess_data_from_source()
        print(
            f"Computing GHCN truth for {len(ewb_cases)} cases (n_jobs={args.n_jobs})",
            flush=True,
        )
        # GHCN polars LazyFrames are cheap to share; use threading so the
        # inner collect() releases the GIL while the parquet scan streams.
        def _one_ghcn(c):
            tgt = heat_tgt if c.event_type == "heat_wave" else freeze_tgt
            lf = heat_lf if c.event_type == "heat_wave" else freeze_lf
            return _process_case_ghcn(
                c, tgt, lf, anchor_times[c.case_id_number],
                ghcn_out, args.overwrite,
            )

        Parallel(n_jobs=args.n_jobs, backend="threading")(
            delayed(_one_ghcn)(c)
            for c in ewb_cases
            if c.case_id_number in anchor_times
        )

    print("Done", flush=True)
