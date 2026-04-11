import argparse  # noqa: E402
import subprocess
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings(
  "ignore",
  message="Numcodecs codecs are not in the Zarr version 3 specification*",
  category=UserWarning
)

ALL_EVENT_TYPES = ["heat", "freeze", "tc", "severe", "ar"]

_HERE = Path(__file__).parent

EVENT_SCRIPTS = {
    "heat":   _HERE / "run_heat_cases.py",
    "freeze": _HERE / "run_freeze_cases.py",
    "tc":     _HERE / "run_tc_cases.py",
    "severe": _HERE / "run_severe_cases.py",
    "ar":     _HERE / "run_ar_cases.py",
}

MODEL_FLAGS = [
    "run_hres",
    "run_cira_pangu",
    "run_cira_fourv2",
    "run_cira_graphcast",
    "run_bb_aifs",
    "run_bb_graphcast",
    "run_bb_pangu",
]


def parse_args():
    """Parse the arguments for the run script."""
    parser = argparse.ArgumentParser(
        description=(
            "Run ExtremeWeatherBench evaluations for one or more event types."
        )
    )

    parser.add_argument(
        "--event_types",
        nargs="+",
        choices=ALL_EVENT_TYPES + ["all"],
        default=["all"],
        help="Event type(s) to run (default: all)",
    )

    # run whichever model cases the user specifies
    parser.add_argument(
        "--run_hres",
        action="store_true",
        default=False,
        help="Run HRES evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_pangu",
        action="store_true",
        default=False,
        help="Run Pangu evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_fourv2",
        action="store_true",
        default=False,
        help="Run FOURv2 evaluation (default: False)",
    )

    parser.add_argument(
        "--run_cira_graphcast",
        action="store_true",
        default=False,
        help="Run Graphcast evaluation (default: False)",
    )

    parser.add_argument(
        "--run_bb_aifs",
        action="store_true",
        default=False,
        help="Run BB AIFS evaluation (default: False)",
    )

    parser.add_argument(
        "--run_bb_graphcast",
        action="store_true",
        default=False,
        help="Run BB Graphcast evaluation (default: False)",
    )

    parser.add_argument(
        "--run_bb_pangu",
        action="store_true",
        default=False,
        help="Run BB Pangu evaluation (default: False)",
    )

    parser.add_argument(
        "--run_all",
        action="store_true",
        default=False,
        help="Run all model evaluations (default: False)",
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=None,
        help=(
            "Number of jobs to run in parallel. If unset, each event type "
            "script uses its own default."
        ),
    )
    parser.add_argument(
        "--gcs",
        action="store_true",
        default=False,
        help=(
            "Upload result pkl files to gs://extremeweatherbench/tmp/ "
            "with a timestamp suffix after each event type completes."
        ),
    )
    args = parser.parse_args()

    if "all" in args.event_types:
        args.event_types = ALL_EVENT_TYPES

    if args.run_all:
        args.run_hres = True
        args.run_cira_fourv2 = True
        args.run_cira_graphcast = True
        args.run_cira_pangu = True
        args.run_bb_aifs = True
        args.run_bb_graphcast = True
        args.run_bb_pangu = True

    return args


_GCS_BUCKET = "gs://extremeweatherbench/tmp/"
_SAVED_DATA_DIR = Path.home() / "extreme-weather-bench-paper" / "saved_data"


def _upload_new_pkls(modified_after: float, timestamp: str) -> None:
    """Upload any pkl files written after modified_after to GCS."""
    for pkl in _SAVED_DATA_DIR.glob("*.pkl"):
        if pkl.stat().st_mtime >= modified_after:
            dest = _GCS_BUCKET + f"{pkl.stem}_{timestamp}.pkl"
            print(f"  Uploading {pkl.name} -> {dest}")
            subprocess.run(["gsutil", "cp", str(pkl), dest], check=True)


if __name__ == "__main__":
    args = parse_args()

    forward = [f"--{f}" for f in MODEL_FLAGS if getattr(args, f)]
    if args.n_jobs is not None:
        forward += ["--n_jobs", str(args.n_jobs)]

    timestamp = datetime.now().strftime("%Y%m%dT%H%M")

    for event_type in args.event_types:
        script = EVENT_SCRIPTS[event_type]
        print(f"--- Running {event_type} cases ---")
        t_start = time.time()
        subprocess.run([sys.executable, str(script)] + forward, check=True)
        if args.gcs:
            _upload_new_pkls(t_start, timestamp)