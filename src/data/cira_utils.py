import icechunk
from extremeweatherbench import inputs

# Retry virtual-chunk fetches from s3://noaa-oar-mlwp-data harder than the
# icechunk default. With many joblib workers hitting S3 at once, individual
# connects hit the AWS SDK's 3.1s connect timeout and a single failure kills
# the whole run.
CIRA_RETRIES = icechunk.StorageRetriesSettings(
    max_tries=20, initial_backoff_ms=500, max_backoff_ms=60_000
)


def get_cira_icechunk_forecast(model_name, variables=None, preprocess=None, name=None):
    """Open a CIRA icechunk forecast like ewb.inputs.get_cira_icechunk, but with
    more aggressive storage retries and without the broken valid_time coord.

    The CIRA icechunk groups carry a 1-D valid_time(lead_time) coordinate that
    only spans the first init (2020-09-30 to 2020-10-10). EWB's
    check_for_missing_data prefers valid_time, so every later case is reported
    as having no data. Dropping it makes EWB fall back to init_time/lead_time.
    """
    if model_name not in inputs.CIRA_MODEL_NAMES:
        raise ValueError(
            f"Model name {model_name} not found in CIRA_MODEL_NAMES: "
            f"{inputs.CIRA_MODEL_NAMES}"
        )

    storage = icechunk.gcs_storage(
        bucket="extremeweatherbench", prefix="cira-icechunk", anonymous=True
    )
    # start from the stored config so the virtual chunk container is kept
    config = icechunk.Repository.fetch_config(storage)
    config.storage = icechunk.StorageSettings(retries=CIRA_RETRIES)

    ds = inputs.open_icechunk_dataset_from_datatree(
        storage,
        model_name,
        config=config,
        authorize_virtual_chunk_access=inputs.CIRA_CREDENTIALS,
    )
    ds = ds.drop_vars("valid_time", errors="ignore")

    return inputs.XarrayForecast(
        ds=ds,
        variables=variables or [],
        variable_mapping=inputs.CIRA_metadata_variable_mapping,
        name=name or model_name,
        preprocess=preprocess or inputs._default_preprocess,
    )
