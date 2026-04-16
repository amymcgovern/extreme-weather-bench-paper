"""Figure 5: TC track panels for Beryl and Yagi across 4 models.

2 rows (storms) x 4 columns (models) of Cartopy forecast-track maps.
Row 1: TC Beryl (case 155)
Row 2: TC Yagi  (case 162)
Columns: AIFS, Pangu, GraphCast, HRES IFS

Usage:
    python src/plots/figure5_tc_tracks.py
"""

import pathlib

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.cm import ScalarMappable
from matplotlib.colors import ListedColormap
from matplotlib.gridspec import GridSpec

from src.plots.plotting_utils import generate_extent

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
TC_DATA_DIR = (
    pathlib.Path.home() / "code" / "ewb_tc_track_mapper" / "data" / "a2130f5"
)
OUTPUT_DIR = REPO_ROOT / "graphics" / "paper"

MODEL_COLS = [
    ("aifs-single", "AIFS"),
    ("panguweather", "Pangu"),
    ("graphcast", "GraphCast"),
    ("HRES", "HRES IFS"),
]

STORM_ROWS = [
    (155, "TC Beryl"),
    (162, "TC Yagi"),
]

N_ROWS = len(STORM_ROWS)
N_COLS = len(MODEL_COLS)

TITLE_FONTSIZE = 20
TICK_FONTSIZE = 14
CBAR_LABEL_FONTSIZE = 14
CBAR_TICK_FONTSIZE = 12
STORM_LABEL_FONTSIZE = 20


def _setup_colormap(bounds):
    cmap_colors = [
        "#ffffff", "#bde6fa", "#7bbae7", "#4892bd",
        "#49ae62", "#a7d051", "#f9d251", "#f7792f",
        "#e43d28", "#c11b24", "#921318",
    ]
    return mcolors.LinearSegmentedColormap.from_list(
        "custom_cubehelix", cmap_colors,
    ), mcolors.BoundaryNorm(bounds, len(cmap_colors) * 256)


def _wrap_lon(arr):
    """Normalize longitudes to [-180, 180] for Cartopy ax.plot compatibility."""
    return ((arr + 180.0) % 360.0) - 180.0


def _load_nc(nc_path):
    ds = xr.open_dataset(nc_path, decode_timedelta=False)
    track_data = xr.Dataset(
        {
            "latitude": ("detection", ds.detection_lat.values),
            "longitude": (
                "detection", _wrap_lon(ds.detection_lon.values),
            ),
        }
    ).assign_coords(
        init_time=(
            "detection", ds.detection_init_time.values,
        )
    )
    case_title = ds.attrs.get("case_title", "Unknown")
    analysis_track_data = xr.Dataset(
        {
            "latitude": ("obs_time", ds.observed_lat.values),
            "longitude": (
                "obs_time", _wrap_lon(ds.observed_lon.values),
            ),
            "tc_name": (
                "obs_time",
                np.full(len(ds.obs_time), case_title),
            ),
        }
    )
    return track_data, analysis_track_data


EXTENT_CRS = ccrs.Mercator()


def _get_shared_extent(storm_case_id, cell_aspect=(4, 3)):
    """Compute a Mercator extent covering all tracks for a storm.

    Uses ``generate_extent`` with the given *cell_aspect*
    so the resulting extent fills a subplot cell of that
    aspect ratio without Cartopy shrinking the axes.

    Returns extent in Mercator CRS (use with
    ``ax.set_extent(extent, crs=EXTENT_CRS)``).
    """
    all_lons, all_lats = [], []
    for model_dir, _ in MODEL_COLS:
        nc = (
            TC_DATA_DIR / model_dir
            / f"case_{storm_case_id:03d}.nc"
        )
        if nc.exists():
            _, analysis = _load_nc(nc)
            all_lons.extend(
                analysis.longitude.values.tolist()
            )
            all_lats.extend(
                analysis.latitude.values.tolist()
            )
    if not all_lons:
        return None

    pad = 5
    center_lon = (min(all_lons) + max(all_lons)) / 2
    center_lat = (min(all_lats) + max(all_lats)) / 2
    lon_span = max(all_lons) - min(all_lons) + 2 * pad
    lat_span = max(all_lats) - min(all_lats) + 2 * pad

    zoom_lon = lon_span / 4
    zoom_lat = (
        lat_span * cell_aspect[0] / cell_aspect[1] / 4
    )
    zoom = max(zoom_lon, zoom_lat)

    return generate_extent(
        (center_lon, center_lat), zoom, cell_aspect,
    )


def _add_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.25)
    ax.add_feature(
        cfeature.LAND, color="#e8e8e8", alpha=0.8, zorder=0,
    )
    ax.add_feature(cfeature.STATES, linewidth=0.15, alpha=0.4)
    ax.gridlines(
        crs=ccrs.PlateCarree(), draw_labels=False,
        linewidth=0.2, color="gray", alpha=0.25,
        linestyle="--",
    )


def plot_tc_panel(
    track_data,
    analysis_track_data,
    model_name,
    ax,
    fig,
    extent=None,
    *,
    show_col_title=False,
    storm_label=None,
    add_colorbar: bool = True,
):
    """Plot a single TC track panel.

    Returns
    -------
    tuple[ScalarMappable, list[pd.Timestamp]]
        The ScalarMappable encoding the init-time colormap and the list of
        init datetimes, so the caller can build a shared colorbar.
    """
    valid_init_times = []
    valid_groups = []
    for init_time, group in track_data.groupby("init_time"):
        lats = group.latitude.values.flatten()
        lons = group.longitude.values.flatten()
        mask = ~(np.isnan(lats) | np.isnan(lons))
        if mask.any():
            valid_init_times.append(init_time)
            valid_groups.append((lats[mask], lons[mask]))

    n_times = len(valid_init_times)
    cmap, _ = _setup_colormap(
        np.linspace(0.2, 0.8, n_times),
    )
    colors = cmap(
        np.linspace(0.2, 1 - 1 / (n_times + 1), n_times),
    )

    _add_basemap(ax)

    for i, (lats_v, lons_v) in enumerate(valid_groups):
        ax.plot(
            lons_v, lats_v, "-",
            color=colors[i], alpha=0.8,
            linewidth=1.5, transform=ccrs.PlateCarree(),
        )

    ax.plot(
        analysis_track_data.longitude,
        analysis_track_data.latitude,
        "-ok", transform=ccrs.PlateCarree(),
        linewidth=1.2, markersize=2.5,
        markeredgecolor="white", markeredgewidth=0.3,
    )

    if extent is not None:
        ax.set_extent(extent, crs=EXTENT_CRS)

    init_datetimes = [
        pd.to_datetime(str(t)) for t in valid_init_times
    ]
    cmap_ = ListedColormap(colors)
    norm_ = plt.Normalize(vmin=-0.5, vmax=n_times - 0.5)
    sm_ = ScalarMappable(cmap=cmap_, norm=norm_)
    sm_.set_array([])

    if add_colorbar:
        # -- colorbar below panel --
        cbar = fig.colorbar(
            sm_, ax=ax, orientation="horizontal",
            pad=0.04, shrink=0.75, aspect=22,
        )
        cbar.set_label(
            "Initialization Date",
            fontsize=CBAR_LABEL_FONTSIZE, labelpad=3,
        )

        all_tick_labels = [
            dt.strftime("%m/%d") for dt in init_datetimes
        ]
        max_ticks = 5
        if n_times <= max_ticks:
            indices = list(range(n_times))
        else:
            indices = np.round(
                np.linspace(0, n_times - 1, max_ticks)
            ).astype(int).tolist()
            indices = sorted(set(indices))
        tick_pos = np.array(indices, dtype=float)
        tick_labels = [all_tick_labels[i] for i in indices]

        cbar.set_ticks(tick_pos)
        cbar.set_ticklabels(
            tick_labels, fontsize=CBAR_TICK_FONTSIZE,
        )
        cbar.ax.tick_params(
            labelsize=CBAR_TICK_FONTSIZE, pad=2, length=2,
        )
        cbar.outline.set_linewidth(0.4)
        cbar.outline.set_edgecolor("gray")
        cbar.ax.set_xlim(-0.5, n_times - 0.5)

    if show_col_title:
        ax.set_title(
            model_name, fontsize=TITLE_FONTSIZE,
            pad=8, loc="left",
        )

    if storm_label:
        ax.text(
            -0.02, 0.5, storm_label,
            transform=ax.transAxes,
            fontsize=STORM_LABEL_FONTSIZE,
            va="center", ha="right",
            rotation=90,
        )

    return sm_, init_datetimes


def main():
    import matplotlib
    matplotlib.use("Agg")

    fig = plt.figure(figsize=(16, 9))
    gs = GridSpec(
        N_ROWS, N_COLS, figure=fig,
        left=0.01, right=0.99, top=0.92, bottom=0.02,
        wspace=0.04, hspace=0.35,
    )

    cell_aspect = (4, 4.5)
    row_extents = {
        cid: _get_shared_extent(cid, cell_aspect)
        for cid, _ in STORM_ROWS
    }

    for row_idx, (case_id, storm_label) in enumerate(
        STORM_ROWS
    ):
        extent = row_extents[case_id]
        for col_idx, (model_dir, display_name) in enumerate(
            MODEL_COLS
        ):
            nc_path = (
                TC_DATA_DIR / model_dir
                / f"case_{case_id:03d}.nc"
            )
            ax = fig.add_subplot(
                gs[row_idx, col_idx],
                projection=ccrs.PlateCarree(),
            )

            if not nc_path.exists():
                _add_basemap(ax)
                if extent is not None:
                    ax.set_extent(
                        extent, crs=EXTENT_CRS,
                    )
                ax.text(
                    0.5, 0.5, "Data not yet\ngenerated",
                    transform=ax.transAxes,
                    ha="center", va="center",
                    fontsize=7, color="gray", style="italic",
                )
                if row_idx == 0:
                    ax.set_title(
                        display_name,
                        fontsize=TITLE_FONTSIZE,
                         pad=30,
                        loc="left",
                    )
                if col_idx == 0:
                    ax.text(
                        -0.02, 0.5, storm_label,
                        transform=ax.transAxes,
                        fontsize=STORM_LABEL_FONTSIZE,
                        va="center", ha="right",
                        rotation=90,
                    )
                continue

            track_data, analysis_data = _load_nc(nc_path)
            plot_tc_panel(
                track_data, analysis_data, display_name,
                ax=ax, fig=fig, extent=extent,
                show_col_title=(row_idx == 0),
                storm_label=(
                    storm_label if col_idx == 0 else None
                ),
            )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "figure5_tc_tracks.png"
    fig.savefig(out_path, dpi=300)
    print(f"Saved to {out_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
