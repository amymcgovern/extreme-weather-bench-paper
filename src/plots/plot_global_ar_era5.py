"""Generate a global AR plot from ERA5 data for a specific timestamp.

Loads ERA5 data from WeatherBench2, computes IVT and the AR mask using
the ExtremeWeatherBench derived-variable pipeline, and saves a global map.

Usage:
    python src/plots/plot_global_ar_era5.py --time "2021-10-24T18:00"
    python src/plots/plot_global_ar_era5.py  # defaults to 2021-10-24 18z
"""

import argparse
import sys
from pathlib import Path

import gcsfs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from extremeweatherbench import derived

import src.plots.atmospheric_river_utils as ar_plot_utils


# WeatherBench2 ERA5 6-hourly, 0.25-degree dataset (anon access)
WB2_ERA5_ZARR = (
    "weatherbench2/datasets/era5/1959-2022-6h-1440x721.zarr"
)

# Variable name mapping from WeatherBench2 to EWB convention
WB2_TO_EWB = {
    "u_component_of_wind": "eastward_wind",
    "v_component_of_wind": "northward_wind",
    "specific_humidity": "specific_humidity",
}

OUTPUT_DIR = Path.home() / "plots"


def load_era5_for_timestamp(timestamp: pd.Timestamp) -> xr.Dataset:
    """Load ERA5 wind and humidity data from WeatherBench2 for one timestep.

    Retains the time dimension (renamed to 'valid_time') so that the EWB
    derived-variable pipeline can chunk along it.

    Args:
        timestamp: Target timestamp (must align to 6-hourly ERA5 grid).

    Returns:
        Dataset with eastward_wind, northward_wind, specific_humidity and
        a 'valid_time' dimension.
    """
    fs = gcsfs.GCSFileSystem(token="anon")
    store = fs.get_mapper(WB2_ERA5_ZARR)
    ds = xr.open_zarr(store)

    # Find nearest time index and select with isel to preserve the dim
    time_idx = int(
        np.argmin(np.abs(ds.time.values - np.datetime64(timestamp)))
    )
    ds_t = ds[list(WB2_TO_EWB.keys())].isel(time=[time_idx])
    ds_t = ds_t.load()

    # Rename to EWB expected names and rename time → valid_time
    ds_renamed = ds_t.rename(WB2_TO_EWB).rename({"time": "valid_time"})
    return ds_renamed


def compute_ivt_and_ar_mask(ds: xr.Dataset) -> xr.Dataset:
    """Compute IVT and AR mask using the EWB derived-variable pipeline.

    Args:
        ds: Dataset containing eastward_wind, northward_wind,
            specific_humidity with a level coordinate in hPa.

    Returns:
        Dataset with integrated_vapor_transport and atmospheric_river_mask.
    """
    arv = derived.AtmosphericRiverVariables()
    result = arv.derive_variable(ds)
    return result


def convert_lon_0360_to_180(da: xr.DataArray) -> xr.DataArray:
    """Roll longitudes from 0–360 to –180–180 for Cartopy PlateCarree.

    Args:
        da: DataArray with a 'longitude' dim in [0, 360).

    Returns:
        DataArray with longitude in (–180, 180].
    """
    lon = da.longitude.values
    new_lon = np.where(lon > 180, lon - 360, lon)
    da = da.assign_coords(longitude=new_lon)
    da = da.sortby("longitude")
    return da


def main(timestamp_str: str = "2021-10-24T18:00") -> None:
    """Load ERA5, compute AR fields, and save a global plot.

    Args:
        timestamp_str: ISO-format timestamp string for the target time.
    """
    timestamp = pd.Timestamp(timestamp_str)
    print(f"Loading ERA5 data for {timestamp} ...")
    ds = load_era5_for_timestamp(timestamp)

    print("Computing IVT and AR mask ...")
    ar_ds = compute_ivt_and_ar_mask(ds)

    # Squeeze out the single valid_time dimension for 2-D plotting
    ivt = ar_ds["integrated_vapor_transport"].squeeze("valid_time", drop=True)
    ar_mask = ar_ds["atmospheric_river_mask"].squeeze("valid_time", drop=True)

    # Convert 0–360 longitude to –180–180 for clean global plotting
    ivt = convert_lon_0360_to_180(ivt)
    ar_mask = convert_lon_0360_to_180(ar_mask)

    time_label = timestamp.strftime("%Y-%m-%d %Hz")
    title = f"ERA5 Atmospheric River Mask\nValid {time_label}"

    print("Plotting ...")
    ax = ar_plot_utils.plot_ar_mask_global(
        ivt_data=ivt,
        ar_mask=ar_mask,
        title=title,
        colorbar=True,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fname = timestamp.strftime("global_ar_era5_%Y%m%d_%Hz.png")
    out_path = OUTPUT_DIR / fname
    ax.figure.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(ax.figure)
    print(f"Saved → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Global AR map from ERA5 via WeatherBench2."
    )
    parser.add_argument(
        "--time",
        default="2021-10-24T18:00",
        help="ISO timestamp, e.g. '2021-10-24T18:00' (default: 2021-10-24T18:00)",
    )
    args = parser.parse_args()
    main(args.time)
