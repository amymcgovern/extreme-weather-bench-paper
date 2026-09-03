"""Raise this process's ``RLIMIT_NOFILE`` soft limit toward the hard limit.

Long parallel evaluations across arraylake / icechunk / zarr sources
open a lot of file handles per worker at once (zarr chunk files,
fsspec sessions, Numba cache ``.nbi`` files, HDF5 handles, etc.). The
Linux default soft ``NOFILE`` is 1024, which is hit almost immediately
with ``n_jobs > ~4`` on the BB icechunk archives and surfaces as::

    OSError: [Errno 24] Too many open files

The interactive login shell on this workstation has been bumped to
1048576, but scripts launched from tmux/screen/systemd/other terminals
often inherit the 1024 default. Call :func:`raise_fd_soft_limit` at the
top of any ``run_*.py`` / ``compute_*.py`` entrypoint so the fix does
not depend on how the shell was configured.
"""

from __future__ import annotations

import resource


def raise_fd_soft_limit(verbose: bool = True) -> tuple[int, int]:
    """Raise ``RLIMIT_NOFILE`` soft to hard; safe no-op on failure.

    Args:
        verbose: When ``True`` (default), print a one-line status so the
            user can confirm in the run log that the limit was raised.

    Returns:
        ``(soft_before, soft_after)`` -- ``soft_after == soft_before``
        indicates the raise was not applied (either already at hard, or
        the OS refused). Child processes spawned via ``fork``/``spawn``
        inherit the raised limit, so this only needs to be called once
        in the parent process.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= hard:
        if verbose:
            print(f"[fd_limit] NOFILE already at hard limit ({soft})", flush=True)
        return soft, soft
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except (ValueError, OSError) as exc:
        if verbose:
            print(
                f"[fd_limit] could not raise NOFILE {soft} -> {hard}: {exc!r}",
                flush=True,
            )
        return soft, soft
    if verbose:
        print(f"[fd_limit] raised NOFILE soft {soft} -> {hard}", flush=True)
    return soft, hard
