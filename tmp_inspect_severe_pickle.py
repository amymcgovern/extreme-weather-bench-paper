"""Inspect a severe (CBSS+PPH) per-case pickle."""

from __future__ import annotations

import pickle
import time
from pathlib import Path


def describe(name, obj, indent=2):
    pfx = " " * indent
    print(f"{pfx}{name}: type={type(obj).__name__}", flush=True)
    if hasattr(obj, "sizes"):
        print(f"{pfx}  sizes: {dict(obj.sizes)}", flush=True)
    if hasattr(obj, "nbytes"):
        print(f"{pfx}  logical nbytes: {obj.nbytes / 1e6:.1f} MB", flush=True)
    if hasattr(obj, "data_vars"):
        for v in obj.data_vars:
            da = obj[v]
            print(f"{pfx}  var {v}: shape={da.shape} dtype={da.dtype} "
                  f"chunks={getattr(da.data, 'chunksize', None)} "
                  f"is_dask={hasattr(da.data, 'dask')}",
                  flush=True)
    elif hasattr(obj, "shape"):
        print(f"{pfx}  shape={obj.shape} dtype={obj.dtype} "
              f"chunks={getattr(obj.data, 'chunksize', None)} "
              f"is_dask={hasattr(obj.data, 'dask')}",
              flush=True)


def main():
    base = Path.home() / "extreme-weather-bench-paper" / "saved_data"
    for d in [
        "hres_severe_graphics",
        "gc_bb_severe_graphics",
        "pang_bb_severe_graphics",
        "aifs_bb_severe_graphics",
    ]:
        # first available case
        pkls = sorted((base / d).glob("case_*.pkl")) if (base / d).exists() else []
        if not pkls:
            continue
        p = pkls[0]
        print(f"\n=== {p} ({p.stat().st_size / 1e6:.1f} MB) ===", flush=True)
        t0 = time.perf_counter()
        with open(p, "rb") as f:
            payload = pickle.load(f)
        print(f"pickle load: {time.perf_counter() - t0:.2f}s", flush=True)
        print(f"top-level keys: {list(payload.keys())}", flush=True)
        for k, v in payload.items():
            describe(k, v)


if __name__ == "__main__":
    main()
