"""Reviewer-response figure: classic vs. revised IVT colormap, lightness profiles."""
import sys; sys.path.insert(0, sys.argv[2])
import numpy as np, matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from colorspacious import cspace_convert
from src.plots import colormaps

bounds = np.arange(0, 1200, 100); mids = bounds[:-1] + 50
fig, axs = plt.subplots(3, 2, figsize=(11, 6.6), gridspec_kw={"height_ratios": [0.5, 0.5, 3], "hspace": 0.12, "wspace": 0.12})
for col, (style, title) in enumerate([("classic", "Original colormap"), ("perceptual", "Revised colormap")]):
    cmap, norm = colormaps.ivt_colormap_and_norm(style)
    rgb = np.array([cmap(norm(m))[:3] for m in mids])
    J = cspace_convert(rgb, "sRGB1", "CAM02-UCS")[:, 0]
    gray = np.repeat(J[:, None] / 100, 3, axis=1)
    for row, cols in [(0, rgb), (1, gray)]:
        ax = axs[row, col]
        ax.bar(mids, 1, width=100, color=cols, edgecolor="none")
        ax.set_xlim(0, 1100); ax.set_ylim(0, 1); ax.set_yticks([]); ax.set_xticks([])
        for s in ax.spines.values(): s.set_linewidth(0.5)
        if col == 0: ax.set_ylabel(["Color", "Grayscale"][row], rotation=0, ha="right", va="center", fontsize=10)
    axs[0, col].set_title(title, fontsize=14, pad=8)
    ax = axs[2, col]
    ax.bar(mids, J, width=100, color=rgb, edgecolor="black", linewidth=0.4)
    ax.plot(mids, J, "k.-", lw=1.2)
    ax.set_xlim(0, 1100); ax.set_ylim(0, 105)
    ax.set_xticks(bounds[::2])
    ax.set_xlabel("Integrated Vapor Transport (kg m$^{-1}$ s$^{-1}$)", fontsize=11)
    if col == 0: ax.set_ylabel("Perceived lightness, CAM02-UCS J'", fontsize=11)
    else: ax.set_yticklabels([])
    ax.grid(axis="y", lw=0.3, alpha=0.6)
fig.savefig(sys.argv[1], dpi=300, bbox_inches="tight")
