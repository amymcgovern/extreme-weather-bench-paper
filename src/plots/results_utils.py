import extremeweatherbench as ewb
import numpy as np
import pandas as pd
import xarray as xr
from joblib import Parallel, delayed

# utilities to process the results, mostly used to make plotting easier


def _snap_lead_time_to_bins(
    lead_col: pd.Series,
    bin_days: list[int],
) -> pd.Series:
    """Snap each lead_time to the nearest bin (in days).

    Handles both timedelta and numeric (hours) lead_time
    columns. Values further than half the smallest bin gap
    from any target are left as-is (and filtered out later
    by the ``isin`` check).
    """
    target_hours = np.array([d * 24 for d in bin_days])
    if pd.api.types.is_timedelta64_dtype(lead_col):
        hours = lead_col.dt.total_seconds() / 3600.0
    else:
        hours = lead_col.astype(float)

    h_vals = hours.values.copy()
    result = np.full(len(h_vals), np.nan)
    finite = np.isfinite(h_vals)

    if finite.any():
        dists = np.abs(
            h_vals[finite, None] - target_hours[None, :]
        )
        nearest_idx = dists.argmin(axis=1)
        snapped = target_hours[nearest_idx].astype("float64")

        half_gaps = np.diff(
            target_hours,
            prepend=0,
            append=target_hours[-1] + target_hours[0],
        )
        max_dist = np.minimum(half_gaps[:-1], half_gaps[1:]) / 2
        too_far = dists[np.arange(len(nearest_idx)), nearest_idx] > max_dist[nearest_idx]
        snapped[too_far] = np.nan
        result[finite] = snapped

    return pd.to_timedelta(result, unit="h")


def subset_results_to_xarray(
    results_df,
    forecast_source,
    target_source,
    metric,
    lead_time_days=None,
    case_id_list=None,
    target_variable=None,
    snap_lead_times=False,
):
    """
    takes in one of the overall results tables and returns a multi-dimensional xarray
        for easier plotting.
    parameters:
        results_df: pandas dataframe containing the results
        forecast_source: string, the forecast source
        target_source: string, the target source
        metric: string, the metric to plot
        lead_time_days: list of integers, the lead times to subset the data to
            (None if you don't want to subset by init time)
        case_id_list: list of integers, the case ids to subset the data to
            (None if you don't want to subset)
        snap_lead_times: if True, snap each row's lead_time
            to the nearest bin center before filtering.
            Useful for TC landfall metrics whose lead times
            fall at 6-hour granularity.
    returns:
        subset_xa: xarray dataset containing the subsetted data
    """
    if case_id_list is not None:
        subset = results_df[
            (results_df["forecast_source"] == forecast_source)
            & (results_df["target_source"] == target_source)
            & (results_df["metric"] == metric)
            & (results_df["case_id_number"].isin(case_id_list))
        ]
    else:
        subset = results_df[
            (results_df["forecast_source"] == forecast_source)
            & (results_df["target_source"] == target_source)
            & (results_df["metric"] == metric)
        ]

    if target_variable is not None:
        subset = subset[subset["target_variable"] == target_variable]

    lead_times = [
        np.timedelta64(d, "D") for d in lead_time_days
    ]

    if snap_lead_times:
        subset = subset.copy()
        subset["lead_time"] = _snap_lead_time_to_bins(
            subset["lead_time"], lead_time_days
        )

    subset2 = (
        subset[subset.lead_time.isin(lead_times)]
        .set_index(["lead_time", "case_id_number"])
        .sort_index()
    )

    subset2 = subset2.dropna(subset=['value'])
    subset2 = subset2.reset_index()[['case_id_number','lead_time','value']].groupby(['lead_time','case_id_number',]).mean()
    subset_xa = subset2.to_xarray()

    return subset_xa

def _round_to_nearest_6h(dt64: np.datetime64) -> pd.Timestamp:
    """Round a numpy datetime64 to the nearest 6-hour UTC boundary."""
    return pd.Timestamp(dt64).round("6h")

def _landfall_for_case(
    case_id: int,
    case_meta,
    ibtracs: ewb.targets.IBTrACS,
    raw_ibtracs_df,
) -> tuple[int, xr.DataArray] | None:
    """Process a single case; returns (case_id, landfalls) or None."""
    subset = ibtracs.subset_data_to_case(raw_ibtracs_df.lazy(), case_meta)
    target_ds = ibtracs.maybe_convert_to_dataset(subset)

    if not target_ds or "surface_wind_speed" not in target_ds:
        return None

    target_track = target_ds["surface_wind_speed"]
    target_landfalls = ewb.calc.find_landfalls(target_track)

    if len(target_landfalls) == 0:
        return None
    return (case_id, target_landfalls)


def generate_ibtracs_landfalls(
    n_workers: int = 1,
) -> dict[int, xr.DataArray]:
    """Build a mapping of case_id → landfall DataArray for all TC cases.

    Parameters
    ----------
    n_workers:
        Number of parallel joblib workers. 1 (default) runs serially.
        Pass -1 to use all available CPUs.
    """
    all_cases = ewb.load_cases()
    case_map = {c.case_id_number: c for c in all_cases}

    ibtracs = ewb.targets.IBTrACS()
    # Collect once after renaming to avoid re-downloading per case.
    # maybe_map_variable_names renames SEASON→season, NAME→tc_name, etc.
    raw_ibtracs_lazy = (
        ibtracs.open_and_maybe_preprocess_data_from_source()
        .pipe(ibtracs.maybe_map_variable_names)
    )
    raw_ibtracs_df = raw_ibtracs_lazy.collect(engine="streaming")

    results = Parallel(n_jobs=n_workers)(
        delayed(_landfall_for_case)(case_id, case_meta, ibtracs, raw_ibtracs_df)
        for case_id, case_meta in case_map.items()
    )

    return dict(r for r in results if r is not None)

def _compute_lead_time_to_landfall(
    df: pd.DataFrame,
    ibtracs_landfalls: dict[int, xr.DataArray] | None = None,
) -> pd.Series:
    """Return a Series of lead_time_to_landfall (hours) for every row.

    If the DataFrame already contains a ``target_landfall_valid_time``
    column (produced by ``LandfallMetric._attach_landfall_metadata``),
    that value is used directly — it reflects the same target-matching
    logic (track-start filtering, fallback to next coverable landfall)
    that the metric pipeline applied.

    Otherwise, falls back to re-deriving landfall times from IBTrACS
    by picking the first landfall whose valid_time is after init_time.

    The landfall time is rounded to the nearest 6-hour boundary and
    lead time is ``(landfall_rounded - init_time)`` in hours.
    Rows with no landfall receive NaN.

    Parameters
    ----------
    df:
        Results DataFrame; must contain ``case_id_number`` and
        ``init_time`` columns.
    ibtracs_landfalls:
        Pre-computed output of ``generate_ibtracs_landfalls()``.
        Only used when ``target_landfall_valid_time`` is not
        present in *df*.
    """
    lead_series = pd.Series(np.nan, index=df.index, dtype=float)

    if "target_landfall_valid_time" in df.columns:
        valid = df["target_landfall_valid_time"].notna()
        lf_times = pd.to_datetime(
            df.loc[valid, "target_landfall_valid_time"]
        )
        init_times = pd.to_datetime(
            df.loc[valid, "init_time"]
        )
        rounded = lf_times.apply(_round_to_nearest_6h)
        lead_series.loc[valid] = (
            (rounded - init_times).dt.total_seconds() / 3600.0
        )
        return lead_series

    if ibtracs_landfalls is None:
        ibtracs_landfalls = generate_ibtracs_landfalls()

    for case_id in df["case_id_number"].unique():
        target_landfalls = ibtracs_landfalls.get(case_id)
        if target_landfalls is None or len(target_landfalls) == 0:
            continue

        target_times = target_landfalls.coords["valid_time"].values
        case_mask = df["case_id_number"] == case_id

        for init_time in df.loc[case_mask, "init_time"].unique():
            init_np = np.datetime64(init_time, "ns")
            next_idx = np.searchsorted(
                target_times, init_np, side="right"
            )
            if next_idx >= len(target_times):
                continue

            landfall_time = target_times[next_idx]
            landfall_rounded = _round_to_nearest_6h(landfall_time)
            lead_h = (
                landfall_rounded - pd.Timestamp(init_time)
            ).total_seconds() / 3600.0

            row_mask = case_mask & (df["init_time"] == init_time)
            lead_series.loc[row_mask] = lead_h

    return lead_series


def subset_results_to_xarray_by_init_time_tropical_cyclone(
    results_df,
    ibtracs_landfalls: dict[int, xr.DataArray] | None = None,
):
    """
    takes in one of the overall results tables and returns a multi-dimensional xarray
        for easier plotting. For this data, lead time is NaN and thus we must 
        compute it by looping through each event's actual time and the init times
        and we can return results by the specific difference in init_time - actual_time.
    parameters:
        ewb_cases: list of ewb.Case objects, needed to compute the lead time 
        results_df: pandas dataframe containing the results
        forecast_source: string, the forecast source
        target_source: string, the target source
        metric: string, the metric to plot
        lead_time_days: list of integers, the lead times to subset the data to. 
           Note that this is difference from the start time of each case rather than computed
           from the lead_time column.
        case_id_list: list of integers, the case ids to subset the data to
            (None if you don't want to subset)
        target_variable: string, the target variable to plot (None if not needed)
        landfalls: list of landfalls, needed to compute the lead time (None if not needed)
    returns:
        subset_xa: xarray dataset containing the subsetted data
    """
    lead_time_series = _compute_lead_time_to_landfall(
        results_df, ibtracs_landfalls=ibtracs_landfalls
    )
    # prepare for xarray conversion (set_index/sort_index return new DataFrame)
    results_df["lead_time"] = lead_time_series
    results_df = results_df.set_index(["lead_time", "case_id_number"]).sort_index()
    try:
        results_xa = results_df.to_xarray()
        return results_xa
    except ValueError:
        return results_df

    

def compute_mean_by_lead_time(
    ewb_cases,
    results_df,
    forecast_source,
    target_source,
    metric,
    lead_time_days,
    case_ids=None,
    target_variable=None,
    snap_lead_times=False,
):
    """Computes the mean of the results by lead time.
    parameters:
        results_df: pandas dataframe containing the results
        forecast_source: string, the forecast source
        target_source: string, the target source
        metric: string, the metric to plot
        lead_times: list of timedelta objects, the lead times to compute the mean for
        case_ids: list of strings, the case ids to subset the data to
        target_variable: string, the target variable to plot (None if not needed)
        snap_lead_times: if True, snap lead times to
            nearest bin center before filtering.
    returns:
        my_mean: numpy array containing the mean of the results by lead time
    """


    if 'DurationMeanError' in metric:
        print("don't call subset_results_to_xarray for tropical cyclones or duration metrics")   
        return None     
    else:   
        subset = subset_results_to_xarray(
            results_df=results_df,
            forecast_source=forecast_source,
            target_source=target_source,
            metric=metric,
            lead_time_days=lead_time_days,
            case_id_list=case_ids,
            target_variable=target_variable,
            snap_lead_times=snap_lead_times,
        )
    my_mean = subset["value"].mean("case_id_number")
    return my_mean


def compute_relative_error(
    ewb_cases,
    results_df,
    forecast_source,
    comparison_results_df,
    comparison_forecast_source,
    target_source,
    metric,
    lead_time_days,
    case_ids=None,
    higher_is_better=False,
    target_variable=None,
    snap_lead_times=False,
):
    """Computes the relative error of the results by lead time Error
    is defined as relative to the comparison results.
    If the metric is better when lower,
        the relative error is computed as (my_mean - comparison_mean) /
            comparison_mean * 100.
    If the metric is better when higher,
        the relative error is computed as (comparison_mean - my_mean) /
            comparison_mean * 100.
    parameters:
        results_df: pandas dataframe containing the results
        comparison_results_df: pandas dataframe containing the comparison results
        comparison_forecast_source: string, the comparison forecast source
        forecast_source: string, the forecast source
        target_source: string, the target source
        metric: string, the metric to plot
        lead_time_days: list of integers, the lead times to compute the relative
            error for
        case_ids: list of strings, the case ids to subset the data to
        higher_is_better: boolean, set to True if the metric is better when higher,
            set to False if the metric is better when lower (default is False)
    returns:
        my_relative_error: numpy array containing the relative error of
            the results by lead time
    """

    my_mean = compute_mean_by_lead_time(    
        ewb_cases,
        results_df,
        forecast_source,
        target_source,
        metric,
        lead_time_days,
        case_ids=case_ids,
        target_variable=target_variable,
        snap_lead_times=snap_lead_times,
    )
    comparison_mean = compute_mean_by_lead_time(
        ewb_cases,
        comparison_results_df,
        comparison_forecast_source,
        target_source,
        metric,
        lead_time_days,
        case_ids=case_ids,
        target_variable=target_variable,
        snap_lead_times=snap_lead_times,
    )

    if higher_is_better:
        my_relative_error = (comparison_mean - my_mean) / comparison_mean * 100
    else:
        my_relative_error = (my_mean - comparison_mean) / comparison_mean * 100

    all_lead_times = pd.to_timedelta(lead_time_days, unit="D")
    mean_missing = ~np.isin(all_lead_times, my_mean.lead_time.values)
    rel_missing = ~np.isin(all_lead_times, my_relative_error.lead_time.values)
    if mean_missing.any() or rel_missing.any():
        print(f"Warning: {metric} for {forecast_source} has less than 5 lead times")
        print(my_mean)
        print(my_relative_error)
    my_mean = my_mean.reindex(lead_time=all_lead_times)
    my_relative_error = my_relative_error.reindex(lead_time=all_lead_times)
    # replace computational nan with 0, then restore structural NaNs
    my_relative_error_arr = np.nan_to_num(my_relative_error)
    my_mean_arr = np.nan_to_num(my_mean)
    my_mean_arr[mean_missing] = np.nan
    my_relative_error_arr[rel_missing] = np.nan
    return (my_mean_arr, my_relative_error_arr)

def compute_relative_error_tropical_cyclone(
    results_df,
    comparison_results_df,
    higher_is_better=False,
    ibtracs_landfalls: dict[int, xr.DataArray] | None = None,
):
    """Computes the relative error of the results for tropical cyclones.
    Because TCs have to make landfall to copute lead time, this is separate
    from the other results.

    If the metric is better when lower,
        the relative error is computed as (my_mean - comparison_mean) /
            comparison_mean * 100.
    If the metric is better when higher,
        the relative error is computed as (comparison_mean - my_mean) /
            comparison_mean * 100.
    parameters:
        results_df: pandas dataframe containing the results
        comparison_results_df: pandas dataframe containing the comparison results
        higher_is_better: boolean, set to True if the metric is better when higher,
            set to False if the metric is better when lower (default is False)
    returns:
        my_relative_error: numpy array containing the relative error of
            the results by lead time
    """

    subset = subset_results_to_xarray_by_init_time_tropical_cyclone(
        results_df=results_df,
        ibtracs_landfalls=ibtracs_landfalls,
    )


    my_mean = subset[['value','lead_time']].groupby(['lead_time']).mean()["value"]
    comparison_mean = comparison_results_df[['value','lead_time']].groupby(['lead_time']).mean()

    if higher_is_better:
        my_relative_error = (comparison_mean - my_mean) / comparison_mean * 100
    else:
        my_relative_error = (my_mean - comparison_mean) / comparison_mean * 100

    # replace nan with 0
    my_relative_error = np.nan_to_num(my_relative_error)
    my_mean = np.nan_to_num(my_mean)

    return (my_mean, my_relative_error)