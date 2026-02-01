"""
Runtime BLAS thread limiting and diagnostics.

Purpose:
  - avoid hidden multi-threading contention (OpenBLAS/MKL/Accelerate)

Usage (optional):
  from dfm_pipeline.utils.threadpool import limit_blas_threads, print_threadpools
  limit_blas_threads(1)
  print_threadpools()
"""

from __future__ import annotations

try:
    from threadpoolctl import threadpool_info, threadpool_limits
except Exception:  # pragma: no cover
    threadpool_info = None
    threadpool_limits = None


def limit_blas_threads(n: int = 1) -> None:
    if threadpool_limits is None:
        return
    try:
        threadpool_limits(limits=int(n))
    except Exception:
        return


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
        print(f"[threadpoolctl] {name} threads={nthreads} lib={lib}")
