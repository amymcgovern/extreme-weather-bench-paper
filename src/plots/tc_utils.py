import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.cm import ScalarMappable
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D

fig = plt.figure(figsize=(14, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

# Add map features
ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
ax.add_feature(cfeature.BORDERS, linewidth=0.5)
ax.add_feature(cfeature.OCEAN, color="lightblue", alpha=0.5)
ax.add_feature(cfeature.LAND, color="lightgray", alpha=0.7)
ax.add_feature(cfeature.STATES, linewidth=0.3, alpha=0.7)

# Add gridlines
gl = ax.gridlines(
    crs=ccrs.PlateCarree(),
    draw_labels=True,
    linewidth=0.5,
    color="gray",
    alpha=0.5,
    linestyle="--",
)
gl.top_labels = False
gl.right_labels = False

# First pass: collect init times with valid data for HRES
valid_init_times_hres = []
valid_groups_hres = []

for init_time, group in hres_tracks_by_init.groupby("init_time"):
    # Extract valid storm positions for this init_time
    lats = group.latitude
    lons = group.longitude

    # Flatten the arrays to 1D for plotting
    lats_flat = lats.values.flatten()
    lons_flat = lons.values.flatten()

    # Remove NaN values
    valid_coords = ~(np.isnan(lats_flat) | np.isnan(lons_flat))
    lats_valid = lats_flat[valid_coords]
    lons_valid = lons_flat[valid_coords]

    # Only include if there are valid points
    if len(lats_valid) > 0:
        valid_init_times_hres.append(init_time)
        valid_groups_hres.append((lats_valid, lons_valid))

# First pass: collect init times with valid data for FCN
valid_init_times_fcn = []
valid_groups_fcn = []

for init_time, group in fcn_tracks_by_init.groupby("init_time"):
    # Extract valid storm positions for this init_time
    lats = group.latitude
    lons = group.longitude

    # Flatten the arrays to 1D for plotting
    lats_flat = lats.values.flatten()
    lons_flat = lons.values.flatten()

    # Remove NaN values
    valid_coords = ~(np.isnan(lats_flat) | np.isnan(lons_flat))
    lats_valid = lats_flat[valid_coords]
    lons_valid = lons_flat[valid_coords]

    # Only include if there are valid points
    if len(lats_valid) > 0:
        valid_init_times_fcn.append(init_time)
        valid_groups_fcn.append((lats_valid, lons_valid))

# First pass: collect init times with valid data for Pangu
valid_init_times_pangu = []
valid_groups_pangu = []

for init_time, group in pangu_tracks_by_init.groupby("init_time"):
    # Extract valid storm positions for this init_time
    lats = group.latitude
    lons = group.longitude

    # Flatten the arrays to 1D for plotting
    lats_flat = lats.values.flatten()
    lons_flat = lons.values.flatten()

    # Remove NaN values
    valid_coords = ~(np.isnan(lats_flat) | np.isnan(lons_flat))
    lats_valid = lats_flat[valid_coords]
    lons_valid = lons_flat[valid_coords]

    # Only include if there are valid points
    if len(lats_valid) > 0:
        valid_init_times_pangu.append(init_time)
        valid_groups_pangu.append((lats_valid, lons_valid))

# Create colormaps based on valid init times only
n_times_hres = len(valid_init_times_hres)
n_times_fcn = len(valid_init_times_fcn)
n_times_pangu = len(valid_init_times_pangu)
colors_hres = plt.cm.magma(np.linspace(0.2, 0.8, n_times_hres))
colors_fcn = plt.cm.magma(np.linspace(0.2, 0.8, n_times_fcn))
colors_pangu = plt.cm.magma(np.linspace(0.2, 0.8, n_times_pangu))

# Plot HRES (solid lines)
for i, (lats_valid, lons_valid) in enumerate(valid_groups_hres):
    ax.plot(
        lons_valid,
        lats_valid,
        "-o",
        color=colors_hres[i],
        alpha=0.8,
        markersize=4,
        linewidth=1,
        transform=ccrs.PlateCarree(),
    )

# Plot FCN (dashed lines)
for i, (lats_valid, lons_valid) in enumerate(valid_groups_fcn):
    ax.plot(
        lons_valid,
        lats_valid,
        "--o",
        color=colors_fcn[i],
        alpha=0.8,
        markersize=4,
        linewidth=1,
        transform=ccrs.PlateCarree(),
    )

# Plot Pangu (dotted lines)
for i, (lats_valid, lons_valid) in enumerate(valid_groups_pangu):
    ax.plot(
        lons_valid,
        lats_valid,
        ":o",
        color=colors_pangu[i],
        alpha=0.8,
        markersize=4,
        linewidth=1,
        transform=ccrs.PlateCarree(),
    )

# Create colorbars for initialization times
init_datetimes_hres = [pd.to_datetime(str(t)) for t in valid_init_times_hres]
init_datetimes_fcn = [pd.to_datetime(str(t)) for t in valid_init_times_fcn]
init_datetimes_pangu = [pd.to_datetime(str(t)) for t in valid_init_times_pangu]

# HRES colorbar
cmap_hres = ListedColormap(colors_hres)
norm_hres = plt.Normalize(vmin=-0.5, vmax=n_times_hres - 0.5)
sm_hres = ScalarMappable(cmap=cmap_hres, norm=norm_hres)
sm_hres.set_array([])

cbar_hres = plt.colorbar(
    sm_hres, ax=ax, shrink=0.32, aspect=8, pad=0.02, fraction=0.046
)
cbar_hres.set_label("HRES Init", fontsize=11, fontweight="bold", labelpad=8)
tick_positions_hres = np.arange(n_times_hres)
tick_labels_hres = [dt.strftime("%m/%d\n%HZ") for dt in init_datetimes_hres]
cbar_hres.set_ticks(tick_positions_hres)
cbar_hres.set_ticklabels(tick_labels_hres, fontsize=9)
cbar_hres.ax.tick_params(labelsize=9, pad=2)
cbar_hres.outline.set_linewidth(0.5)
cbar_hres.outline.set_edgecolor("gray")

# FCN colorbar
cmap_fcn = ListedColormap(colors_fcn)
norm_fcn = plt.Normalize(vmin=-0.5, vmax=n_times_fcn - 0.5)
sm_fcn = ScalarMappable(cmap=cmap_fcn, norm=norm_fcn)
sm_fcn.set_array([])

cbar_fcn = plt.colorbar(sm_fcn, ax=ax, shrink=0.32, aspect=8, pad=0.06, fraction=0.046)
cbar_fcn.set_label("FCN Init", fontsize=11, fontweight="bold", labelpad=8)
tick_positions_fcn = np.arange(n_times_fcn)
tick_labels_fcn = [dt.strftime("%m/%d\n%HZ") for dt in init_datetimes_fcn]
cbar_fcn.set_ticks(tick_positions_fcn)
cbar_fcn.set_ticklabels(tick_labels_fcn, fontsize=9)
cbar_fcn.ax.tick_params(labelsize=9, pad=2)
cbar_fcn.outline.set_linewidth(0.5)
cbar_fcn.outline.set_edgecolor("gray")

# Pangu colorbar
cmap_pangu = ListedColormap(colors_pangu)
norm_pangu = plt.Normalize(vmin=-0.5, vmax=n_times_pangu - 0.5)
sm_pangu = ScalarMappable(cmap=cmap_pangu, norm=norm_pangu)
sm_pangu.set_array([])

cbar_pangu = plt.colorbar(
    sm_pangu, ax=ax, shrink=0.32, aspect=8, pad=0.10, fraction=0.046
)
cbar_pangu.set_label("Pangu Init", fontsize=11, fontweight="bold", labelpad=8)
tick_positions_pangu = np.arange(n_times_pangu)
tick_labels_pangu = [dt.strftime("%m/%d\n%HZ") for dt in init_datetimes_pangu]
cbar_pangu.set_ticks(tick_positions_pangu)
cbar_pangu.set_ticklabels(tick_labels_pangu, fontsize=9)
cbar_pangu.ax.tick_params(labelsize=9, pad=2)
cbar_pangu.outline.set_linewidth(0.5)
cbar_pangu.outline.set_edgecolor("gray")

# Set extent to focus on the storm region
ax.set_extent([260, 290, 15, 35], crs=ccrs.PlateCarree())

# Create custom legend elements
hres_line = Line2D(
    [0],
    [0],
    color="gray",
    marker="o",
    linestyle="-",
    markersize=8,
    linewidth=2,
    label="HRES Forecast",
)
fcn_line = Line2D(
    [0],
    [0],
    color="gray",
    marker="o",
    linestyle="--",
    markersize=8,
    linewidth=2,
    label="FCN Forecast",
)
pangu_line = Line2D(
    [0],
    [0],
    color="gray",
    marker="o",
    linestyle=":",
    markersize=8,
    linewidth=2,
    label="Pangu Forecast",
)

# Add legend
ax.legend(
    handles=[hres_line, fcn_line, pangu_line],
    loc="upper right",
    fontsize=12,
    frameon=True,
    fancybox=True,
    shadow=True,
)

ax.set_title(
    "Hurricane Ida Forecast Tracks by Initialization Time",
    fontsize=20,
    loc="left",
    pad=10,
)
plt.tight_layout()
plt.show()
