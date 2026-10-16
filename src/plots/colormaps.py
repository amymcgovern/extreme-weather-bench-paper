"""Shared sequential colormap for the AR IVT maps and the TC init-time tracks.

Two styles:

* ``"perceptual"`` (default): white -> light blue -> green -> gold -> red ->
  dark red, with lightness falling monotonically (CAM02-UCS J' 100 -> 25) and
  equal perceptual steps between neighbouring colors. Same hue family as the
  classic ramp, but without the bright-yellow lightness bump that made it a
  rainbow map. Used for the journal submission.
* ``"classic"``: the original white/blue/green/yellow/red ramp, unchanged.

Pick the style with ``set_colormap_style("classic")`` before plotting, or with
the ``EWB_COLORMAP_STYLE`` environment variable. Every plotting helper that
used the old ramp goes through ``get_ramp`` / ``ivt_colormap_and_norm``.
"""

import os
from typing import Optional, Tuple

import matplotlib.colors as mcolors
import numpy as np

STYLES = ("perceptual", "classic")

CLASSIC_COLORS = [
    "#ffffff", "#bde6fa", "#7bbae7", "#4892bd",
    "#49ae62", "#a7d051", "#f9d251", "#f7792f",
    "#e43d28", "#c11b24", "#921318",
]

# The classic hues with lightness forced to fall linearly in CAM02-UCS, then
# resampled to equal CAM02-UCS steps (33 samples; interpolating between them
# keeps each step within ~15 +/- 1 dE).
PERCEPTUAL_COLORS = [
    "#ffffff", "#f0f9fd", "#ddf2fb", "#c9e9f9", "#b5e1f7", "#a1d8f6",
    "#8ccdf5", "#7dc6ec", "#76c4d9", "#71c2c9", "#6cc0ba", "#67beab",
    "#61bb98", "#5ab984", "#54b771", "#5bae5b", "#68a54a", "#759b39",
    "#829227", "#8d8a19", "#9a8008", "#a17801", "#a46c03", "#a76205",
    "#aa5608", "#ae4a0a", "#b13e0c", "#b12f0e", "#a71b0e", "#9c020e",
    "#8b030f", "#79050f", "#69080f",
]

_style = os.environ.get("EWB_COLORMAP_STYLE", "perceptual")


def set_colormap_style(style: str) -> None:
    """Set the style used by every plot after this call."""
    global _style
    if style not in STYLES:
        raise ValueError(f"style must be one of {STYLES}, got {style!r}")
    _style = style


def get_colormap_style() -> str:
    if _style not in STYLES:
        raise ValueError(
            f"EWB_COLORMAP_STYLE must be one of {STYLES}, got {_style!r}"
        )
    return _style


def get_ramp(style: Optional[str] = None) -> mcolors.LinearSegmentedColormap:
    """The continuous white-to-dark-red ramp in the given (or current) style."""
    style = style or get_colormap_style()
    if style == "classic":
        return mcolors.LinearSegmentedColormap.from_list(
            "custom_cubehelix", CLASSIC_COLORS,
        )
    return mcolors.LinearSegmentedColormap.from_list(
        "ewb_perceptual", PERCEPTUAL_COLORS,
    )


def ivt_colormap_and_norm(
    style: Optional[str] = None,
) -> Tuple[mcolors.Colormap, mcolors.BoundaryNorm]:
    """IVT colormap with 100 kg m-1 s-1 bins from 0 to 1100.

    The perceptual style keeps the lowest bin white and spreads the other ten
    over the ramp, so weak IVT stays light and the black AR outline stays
    visible.
    """
    style = style or get_colormap_style()
    bounds = np.arange(0, 1200, 100)
    if style == "classic":
        cmap = get_ramp("classic")
    else:
        ramp = get_ramp("perceptual")
        cmap = mcolors.ListedColormap(
            ["#ffffff"] + [ramp(x) for x in np.linspace(0.1, 1, 10)],
            name="ewb_perceptual_ivt",
        )
    return cmap, mcolors.BoundaryNorm(bounds, cmap.N)
