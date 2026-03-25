from __future__ import annotations

import os
from contextlib import contextmanager

try:
    from threadpoolctl import threadpool_info, threadpool_limits
except Exception:  # pragma: no cover
    threadpool_info = None
    threadpool_limits = None


_BLAS_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "BLIS_NUM_THREADS",
)


def _coerce_threads(n: int | None) -> int | None:
    if n is None:
        return None
    try:
        n = int(n)
    except Exception:
        return None
    if n <= 0:
        return None
    return n


def _snapshot_env() -> dict[str, str | None]:
    return {k: os.environ.get(k) for k in _BLAS_ENV_VARS}


def _apply_env_limit(n: int | None) -> None:
    if n is None:
        return
    s = str(int(n))
    for k in _BLAS_ENV_VARS:
        os.environ[k] = s


def _restore_env(old_env: dict[str, str | None]) -> None:
    for k, v in old_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def limit_blas_threads(n: int | None = 1) -> None:
    """
    Apply a process-wide BLAS/OpenMP thread limit best-effort.
    Safe to call more than once.
    """
    n = _coerce_threads(n)
    if n is None:
        return

    _apply_env_limit(n)

    if threadpool_limits is None:
        return

    try:
        threadpool_limits(limits=int(n))
    except Exception:
        return


@contextmanager
def blas_thread_limit(n: int | None = 1):
    """
    Context manager version of BLAS thread limiting.

    n <= 0 or None:
      no explicit limit is applied

    n >= 1:
      temporarily limit BLAS/OpenMP pools to n threads
    """
    n = _coerce_threads(n)
    old_env = _snapshot_env()

    controller = None
    try:
        _apply_env_limit(n)

        if n is not None and threadpool_limits is not None:
            try:
                controller = threadpool_limits(limits=int(n))
                controller.__enter__()
            except Exception:
                controller = None

        yield
    finally:
        if controller is not None:
            try:
                controller.__exit__(None, None, None)
            except Exception:
                pass
        _restore_env(old_env)


def print_threadpools() -> None:
    if threadpool_info is None:
        print("[threadpoolctl] not available")
        return

    info = threadpool_info()
    if not info:
        print("[threadpoolctl] no threadpools detected")
        return

    for d in info:
        name = d.get("internal_api", "?")
        lib = d.get("filepath", "?")
        nthreads = d.get("num_threads", "?")
        prefix = d.get("prefix", "?")
        print(f"[threadpoolctl] api={name} prefix={prefix} threads={nthreads} lib={lib}")