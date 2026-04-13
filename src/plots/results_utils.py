import extremeweatherbench as ewb
import numpy as np
import pandas as pd

# utilities to process the results, mostly used to make plotting easier


def subset_results_to_xarray(
    results_df,
    forecast_source,
    target_source,
    metric,
    lead_time_days=None,
    case_id_list=None,
    target_variable=None,
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
    returns:
        subset_xa: xarray dataset containing the subsetted data
    """
    # if the case_id_list is not empty, subset to the specific cases
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
        np.timedelta64(lead_time_days[i], "D") for i in range(len(lead_time_days))
    ]

    # prepare for xarray conversion
    subset2 = (
        subset[subset.lead_time.isin(lead_times)]
        .set_index(["lead_time", "case_id_number"])
        .sort_index()
    )
    subset_xa = subset2.to_xarray()

    return subset_xa

def _round_to_nearest_6h(dt64: np.datetime64) -> pd.Timestamp:
    """Round a numpy datetime64 to the nearest 6-hour UTC boundary."""
    return pd.Timestamp(dt64).round("6h")


def _compute_lead_time_to_landfall(df: pd.DataFrame) -> pd.Series:
    """Return a Series of lead_time_to_landfall (hours) for every row.

    For each unique (case_id_number, init_time) pair the function:
    1. Loads IBTrACS best-track data for the corresponding case.
    2. Finds all landfalls in the IBTrACS track geometry.
    3. Picks the first landfall whose valid_time is *after* init_time.
    4. Rounds that landfall time to the nearest 6-hour boundary.
    5. Returns (landfall_rounded - init_time) in fractional hours.

    Rows with no future landfall receive NaN.
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

    lead_series = pd.Series(np.nan, index=df.index, dtype=float)

    for case_id in df["case_id_number"].unique():
        case_meta = case_map.get(case_id)
        if case_meta is None:
            continue


        # subset_data_to_case requires a polars LazyFrame
        subset = ibtracs.subset_data_to_case(raw_ibtracs_df.lazy(), case_meta)
        target_ds = ibtracs.maybe_convert_to_dataset(subset)

        if not target_ds or "surface_wind_speed" not in target_ds:
            continue

        target_track = target_ds["surface_wind_speed"]
        target_landfalls = ewb.calc.find_landfalls(target_track)

        if len(target_landfalls) == 0:
            continue

        # sorted array of interpolated landfall valid_times
        target_times = target_landfalls.coords["valid_time"].values

        case_mask = df["case_id_number"] == case_id

        for init_time, group in df[case_mask].groupby("init_time"):
            init_np = np.datetime64(init_time, "ns")
            next_idx = np.searchsorted(target_times, init_np, side="right")

            if next_idx >= len(target_times):
                continue

            landfall_time = target_times[next_idx]
            landfall_rounded = _round_to_nearest_6h(landfall_time)
            lead_h = (
                landfall_rounded - pd.Timestamp(init_time)
            ).total_seconds() / 3600.0

            lead_series.loc[group.index] = lead_h

    return lead_series


def subset_results_to_xarray_by_init_time_tropical_cyclone(
    results_df,
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
    lead_time_series = _compute_lead_time_to_landfall(results_df)
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
    returns:
        my_mean: numpy array containing the mean of the results by lead time
    """


    if 'DurationMeanError' in metric or 'landfall' in metric:
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
    )
    if higher_is_better:
        my_relative_error = (comparison_mean - my_mean) / comparison_mean * 100
    else:
        my_relative_error = (my_mean - comparison_mean) / comparison_mean * 100

    # replace nan with 0
    my_relative_error = np.nan_to_num(my_relative_error)
    my_mean = np.nan_to_num(my_mean)

    return (my_mean, my_relative_error)

def compute_relative_error_tropical_cyclone(
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
    landfalls=None,
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

    subset = subset_results_to_xarray_by_init_time_tropical_cyclone(
        ewb_cases=ewb_cases,
        results_df=results_df,
        forecast_source=forecast_source,
        target_source=target_source,
        metric=metric,
        lead_time_days=lead_time_days,
        case_ids=case_ids,
        target_variable=target_variable,
        landfalls=landfalls,
    )
    #print("first call to subset gives me")
    #print(subset)
    comparison_subset = subset_results_to_xarray_by_init_time_tropical_cyclone(
        ewb_cases=ewb_cases,
        results_df=comparison_results_df,
        forecast_source=comparison_forecast_source,
        target_source=target_source,
        metric=metric,
        lead_time_days=lead_time_days,
        case_ids=case_ids,
        target_variable=target_variable,
        landfalls=landfalls,
    )
    #print(subset)
    #print(comparison_subset)

    my_mean = subset["value"].mean("case_id_number")
    comparison_mean = comparison_subset["value"].mean("case_id_number")

    if higher_is_better:
        my_relative_error = (comparison_mean - my_mean) / comparison_mean * 100
    else:
        my_relative_error = (my_mean - comparison_mean) / comparison_mean * 100

    # replace nan with 0
    my_relative_error = np.nan_to_num(my_relative_error)
    my_mean = np.nan_to_num(my_mean)

    return (my_mean, my_relative_error)