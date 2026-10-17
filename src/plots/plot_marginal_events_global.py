"""Redraw the global marginal-temperature event map from its saved CSV.

``temperature_events.py global`` (EWB data_prep) detects events and draws
``<stem>_events.png`` in the same run, from memory; it never reads the CSV
back. This script draws the same map from the CSV that run wrote, so the
figure can be remade (e.g. as a PDF) without repeating the ERA5 scan.

Usage:
    python -m src.plots.plot_marginal_events_global \
        --output graphics/paper/marginal_q15_q85_2020-2024_events.pdf
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
_EWB_DATA_PREP = REPO_ROOT / "external" / "ExtremeWeatherBench" / "data_prep"
if str(_EWB_DATA_PREP) not in sys.path:
    sys.path.insert(0, str(_EWB_DATA_PREP))
from temperature_events import plot_events_global  # noqa: E402

DEFAULT_CSV = REPO_ROOT / "saved_data" / "marginal_q15_q85_2020-2024_events.csv"
DEFAULT_OUTPUT = REPO_ROOT / "graphics" / "paper" / "marginal_q15_q85_2020-2024_events.png"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Events CSV from temperature_events.py global.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output path; extension sets the format.")
    parser.add_argument("--start-date", default="2020-01-01", help="Scan start date, for the title.")
    parser.add_argument("--end-date", default="2024-12-31", help="Scan end date, for the title.")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    event_type = df["event_type"].iloc[0]  # e.g. "q0.15-q0.85"
    # Same rule temperature_events.py uses to pick Reds vs Blues: a band with
    # an upper quantile above 0.5 is drawn as heat_wave (Reds).
    plot_event_type = "heat_wave"
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    plot_events_global(
        df,
        event_type=plot_event_type,
        title=(
            f"Detected events ({event_type.replace('_', ' ')})"
            f"\n{args.start_date} to {args.end_date}"
        ),
        output_path=args.output,
    )
    print(f"Saved {args.output} ({len(df)} events)")


if __name__ == "__main__":
    main()
