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

def subset_results_to_xarray_by_init_time_tropical_cyclone(
    ewb_cases,
    results_df,
    forecast_source,
    target_source,
    metric,
    lead_time_days,
    case_ids=None,
    target_variable=None,
    landfalls=None,
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
    # if the case_id_list is not empty, subset to the specific cases
    if case_ids is not None:
        subset = results_df[
            (results_df["forecast_source"] == forecast_source)
            & (results_df["target_source"] == target_source)
            & (results_df["metric"] == metric)
            & (results_df["case_id_number"].isin(case_ids))
        ]
    else:
        subset = results_df[
            (results_df["forecast_source"] == forecast_source)
            & (results_df["target_source"] == target_source)
            & (results_df["metric"] == metric)
        ]

    if target_variable is not None:
        subset = subset[subset["target_variable"] == target_variable]

    # compute the lead times in time deltas so we can do math on it later
    lead_times = [
        np.timedelta64(lead_time_days[i], "D") for i in range(len(lead_time_days))
    ]    
    # print(subset)

    # map case_id_number -> end_date so we can compute lead time per row
    if landfalls is None:
        end_date_by_case = {case.case_id_number: pd.Timestamp(case.end_date) for case in ewb_cases}
    else:
        # landfalls is a list of (case_id, DataArray) tuples; match by case_id directly.
        # ceil to the next 6H boundary (0, 6, 12, 18Z) so we never round down.
        case_id_set = set(case_ids) if case_ids is not None else None
        end_date_by_case = {
            case_id: pd.Timestamp(landfall_da.valid_time.values[0]).ceil('6H')
            for case_id, landfall_da in landfalls
            if (case_id_set is None or case_id in case_id_set)
            and len(landfall_da.valid_time) > 0
        }

    # add lead_time column: for each row, end_date - init_time (on the main subset)
    subset2 = subset.copy()
    subset2["lead_time"] = subset["case_id_number"].map(end_date_by_case) - pd.to_datetime(subset["init_time"])

    # print the init times and lead times column
    # print(subset2)

    # keep only rows whose computed lead time is in the requested lead_times
    # subset2 = subset2.loc[np.isin(subset2["lead_time"].values, lead_times)]
    subset2 = subset2.loc[
        subset2["lead_time"].dt.days.isin(lead_time_days)
        & (subset2["lead_time"].dt.seconds == 0)
    ]
    # prepare for xarray conversion (set_index/sort_index return new DataFrame)
    subset2 = subset2.set_index(["lead_time", "case_id_number"]).sort_index()
    subset_xa = subset2.to_xarray()

    return subset_xa

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