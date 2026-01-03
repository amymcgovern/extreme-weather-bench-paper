import argparse
import datetime
import logging
import time

import icechunk
import xarray as xr
from extremeweatherbench import cases, evaluate, inputs, metrics
from util import DateInterval, InMemoryForecast

mlwp_model_variable_mapping = {
    "2m_temperature": "surface_air_temperature",
    "2m_dewpoint_temperature": "surface_dewpoint_temperature",
    "2m_relative_humidity": "surface_relative_humidity",
    "2m_wind_speed": "surface_wind_speed",
    "2m_wind_from_direction": "surface_wind_from_direction",
    "2m_wind_gust": "surface_wind_gust",
    "2m_wind_gust_direction": "surface_wind_gust_direction",
    "2m_wind_gust_speed": "surface_wind_gust_speed",
}


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# TODO: Change me as needed
# Default Icechunk configuration - "Small" dataset
# DEFAULT_ICECHUNK_BUCKET = "extremeweatherbench"
# DEFAULT_ICECHUNK_PREFIX = "aifs-small-icechunk"
# DEFAULT_SOURCE_CREDENTIALS_PREFIX = "gs://brightband-scratch/darothen/ewb-forecast-archive/aifs-single/"  # Credentials prefix must match the exact prefix stored in the repository config
# REF_INTERVAL = DateInterval(
#     start_date=datetime.date(2024, 3, 1),
#     end_date=datetime.date(2024, 6, 30),
# )

## Alternative Icechunk configuration - Full 4-year archive
DEFAULT_ICECHUNK_BUCKET = "extremeweatherbench"
DEFAULT_ICECHUNK_PREFIX = "aifs-single_20210102-20241231_icechunk"
DEFAULT_SOURCE_CREDENTIALS_PREFIX = "gs://brightband-scratch/darothen/aifs-single-archive/"  # Credentials prefix must match the exact prefix stored in the repository config
REF_INTERVAL = DateInterval(
    start_date=datetime.date(2021, 1, 2),
    end_date=datetime.date(2024, 12, 31),
)


def open_icechunk_dataset(
    bucket: str = DEFAULT_ICECHUNK_BUCKET,
    prefix: str = DEFAULT_ICECHUNK_PREFIX,
    variable_mapping: dict[str, str] | None = None,
    chunks: str | dict | None = "auto",
    source_credentials_prefix: str = DEFAULT_SOURCE_CREDENTIALS_PREFIX,
) -> xr.Dataset:
    """Open a dataset from an Icechunk repository with preprocessing.

    The repository config already knows where the virtual chunks are located.
    We just need to provide credentials broad enough to cover that location.

    Args:
        bucket: GCS bucket containing the Icechunk repository.
        prefix: Prefix within the bucket for the repository.
        variable_mapping: Dictionary mapping source variable names to target names.
        chunks: Chunk specification for xarray (default: "auto").
        source_credentials_prefix: GCS prefix for virtual chunk credentials.
            Should be broad enough to cover wherever the source data lives.

    Returns:
        Preprocessed xarray Dataset ready for evaluation.
    """
    logger.info(f"Opening Icechunk repository at gs://{bucket}/{prefix}")

    # Set up storage
    storage = icechunk.gcs_storage(bucket=bucket, prefix=prefix)

    # Set up credentials for virtual chunks.
    # The repo config knows the exact location; we just provide credentials
    # broad enough to cover it.
    gcs_credentials = icechunk.gcs_from_env_credentials()
    virtual_credentials = icechunk.containers_credentials(
        {source_credentials_prefix: gcs_credentials}
    )

    # Open repository
    repo = icechunk.Repository.open(
        storage, authorize_virtual_chunk_access=virtual_credentials
    )
    session = repo.readonly_session("main")

    # Open dataset
    ds = xr.open_dataset(session.store, engine="zarr", chunks=chunks)
    logger.info(f"Opened dataset with variables: {list(ds.data_vars)}")

    # Apply variable renaming if specified
    if variable_mapping:
        rename_dict = {k: v for k, v in variable_mapping.items() if k in ds.data_vars}
        if rename_dict:
            logger.info(f"Renaming variables: {rename_dict}")
            ds = ds.rename(rename_dict)

    return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run MLWP evaluation against ExtremeWeatherBench cases."
    )

    # Icechunk options
    parser.add_argument(
        "--icechunk-bucket",
        type=str,
        default=DEFAULT_ICECHUNK_BUCKET,
        help=f"GCS bucket for Icechunk repository (default: {DEFAULT_ICECHUNK_BUCKET})",
    )
    parser.add_argument(
        "--icechunk-prefix",
        type=str,
        default=DEFAULT_ICECHUNK_PREFIX,
        help=f"GCS prefix for Icechunk repository (default: {DEFAULT_ICECHUNK_PREFIX})",
    )
    parser.add_argument(
        "--source-prefix",
        type=str,
        default=DEFAULT_SOURCE_CREDENTIALS_PREFIX,
        help=f"GCS prefix for virtual chunk credentials - must match the prefix stored in the Icechunk repo (default: {DEFAULT_SOURCE_CREDENTIALS_PREFIX})",
    )

    args = parser.parse_args()

    case_collection = cases.load_ewb_events_yaml_into_case_collection()
    event_types = ["heat_wave", "freeze", "severe_convection"]

    valid_cases = []
    for case in case_collection.cases:
        case_interval = DateInterval(
            start_date=case.start_date.date(),
            end_date=case.end_date.date(),
        )
        if case.event_type in event_types and REF_INTERVAL.contains(case_interval):
            valid_cases.append(case)

    logger.info(
        f"Found {len(valid_cases)} cases that overlap with the reference interval."
    )
    logger.info("The cases are: ")
    for case in valid_cases:
        logger.info(f"- {case.title} ({case.event_type})")

    subset_case_collection = cases.IndividualCaseCollection(cases=valid_cases)

    # Load forecast data
    start = time.time()

    # Load from Icechunk repository (already concatenated)
    logger.info("Loading data from Icechunk repository...")
    ds = open_icechunk_dataset(
        bucket=args.icechunk_bucket,
        prefix=args.icechunk_prefix,
        variable_mapping=mlwp_model_variable_mapping,
        chunks="auto",
        source_credentials_prefix=args.source_prefix,
    )

    logger.info(ds)
    logger.info(f"Dataset size: {ds.nbytes / 1024 / 1024 / 1024:.2f} GB")
    end = time.time()
    logger.info(f"Time taken to load data: {end - start:.1f} seconds")

    forecast = InMemoryForecast(
        ds,
        name=f"{args.icechunk_bucket}/{args.icechunk_prefix}",
        # NOTE: we have to pass in the variables that will actually be used for this
        # metrics calculations. We can at least bypass the variable mapping by manually
        # processing the datasets for ourselves.
        variables=["surface_air_temperature"],
        variable_mapping=mlwp_model_variable_mapping,
    )
    target = inputs.ERA5(
        variables=["surface_air_temperature"],
        chunks=None,
    )
    evals = [
        inputs.EvaluationObject(
            event_type="heat_wave",
            metric_list=[
                metrics.MaximumMeanAbsoluteError(),
                metrics.RootMeanSquaredError(),
                metrics.MaximumLowestMeanAbsoluteError(),
            ],
            target=target,
            forecast=forecast,
        ),
        inputs.EvaluationObject(
            event_type="freeze",
            metric_list=[
                metrics.MaximumMeanAbsoluteError(),
                metrics.RootMeanSquaredError(),
                metrics.MaximumLowestMeanAbsoluteError(),
            ],
            target=target,
            forecast=forecast,
        ),
        inputs.EvaluationObject(
            event_type="severe_convection",
            metric_list=[
                metrics.ThresholdMetric(
                    metrics=[
                        metrics.CriticalSuccessIndex,
                        metrics.FalseAlarmRatio,
                    ],
                    forecast_threshold=15000,
                    target_threshold=0.3,
                ),
                metrics.EarlySignal(threshold=15000),
            ],
            target=inputs.PPH(
                variables=["practically_perfect_hindcast"],
            ),
            forecast=forecast,
        ),
    ]
    ewb = evaluate.ExtremeWeatherBench(
        case_metadata=subset_case_collection,
        evaluation_objects=evals,
    )
    start = time.time()
    logger.info("Running evaluation...")
    parallel_config = dict(n_jobs=-2, backend="loky")
    out = ewb.run(parallel_config=parallel_config)
    end = time.time()
    logger.info(f"Time taken: {end - start} seconds")
    out.to_csv("eval.heatwaves.csv")
