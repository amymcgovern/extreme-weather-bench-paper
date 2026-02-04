#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "dask",
#   "icechunk",
#   "xarray",
# ]
# ///
import argparse
import logging

import icechunk
import xarray as xr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def open_mlwp_archive_icechunk_dataset(
    model: str,
    src: str = "hres",
    variable_mapping: dict[str, str] | None = None,
    chunks: str | dict | None = "auto",
) -> xr.Dataset:
    bucket = "brightband-public-mlwp-forecast-archive"
    icechunk_prefix = f"{model}.{src}.icechunk"
    source_credentials_prefix = f"gs://{bucket}/{model}/{src}/"

    logger.info(f"Opening Icechunk repository at gs://{bucket}/{icechunk_prefix}")

    # Set up storage
    storage = icechunk.gcs_storage(bucket=bucket, prefix=icechunk_prefix)

    # Set up credentials for virtual chunks.
    # The repo config knows the exact location; we just provide credentials
    # broad enough to cover it.
    gcs_credentials = icechunk.gcs_from_env_credentials()
    logger.info(f"Authorizing virtual chunk access for prefix: {source_credentials_prefix}")
    virtual_credentials = icechunk.containers_credentials({source_credentials_prefix: gcs_credentials})

    # Open repository
    repo = icechunk.Repository.open(storage, authorize_virtual_chunk_access=virtual_credentials)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, choices=["aifs-single", "graphcast", "panguweather"], required=True)
    args = parser.parse_args()

    ds = open_mlwp_archive_icechunk_dataset(model=args.model)

    print(ds)
    print(ds["init_time"].values[[0, -1]])
