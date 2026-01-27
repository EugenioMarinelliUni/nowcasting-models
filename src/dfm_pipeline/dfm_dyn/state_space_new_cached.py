"""
Cached NEW state-space implementation (drop-in compatible API).

This module is designed to be selected by:
    DFM_STATE_SPACE_IMPL=new_cached

It delegates to the cached implementation you already created under:
    dfm_pipeline.dfm_bm_ml.state_space_new_cached

and exposes the SAME public function names as dfm_dyn.state_space_new:
    - kalman_filter_only(...)
    - kalman_filter_smoother(...)

If the bm_ml cached module is not available for some reason, it falls back to
dfm_dyn.state_space_new (correctness-first).
"""

from __future__ import annotations

from typing import Any

try:
    # Preferred: use your existing cached implementation (where you developed it).
    from dfm_pipeline.dfm_bm_ml import state_space_new_cached as _impl  # type: ignore
except Exception:
    # Fallback: correctness (no caching) rather than breaking imports.
    from . import state_space_new as _impl  # type: ignore


def kalman_filter_only(*args: Any, **kwargs: Any):
    """
    Drop-in alias for the NEW cached implementation.

    Supports either naming convention:
      - _impl.kalman_filter_only(...)          (preferred)
      - _impl.kalman_filter_only_cached(...)   (legacy in your cached module)
    """
    fn = getattr(_impl, "kalman_filter_only", None)
    if fn is None:
        fn = getattr(_impl, "kalman_filter_only_cached", None)
    if fn is None:
        raise AttributeError(f"{_impl.__name__} exposes neither kalman_filter_only nor kalman_filter_only_cached")
    return fn(*args, **kwargs)


def kalman_filter_smoother(*args: Any, **kwargs: Any):
    """
    Drop-in alias for the NEW cached implementation.

    Supports either naming convention:
      - _impl.kalman_filter_smoother(...)          (preferred)
      - _impl.kalman_filter_smoother_cached(...)   (legacy in your cached module)
    """
    fn = getattr(_impl, "kalman_filter_smoother", None)
    if fn is None:
        fn = getattr(_impl, "kalman_filter_smoother_cached", None)
    if fn is None:
        raise AttributeError(
            f"{_impl.__name__} exposes neither kalman_filter_smoother nor kalman_filter_smoother_cached"
        )
    return fn(*args, **kwargs)


# Expose these helpers if present (not required, but keeps parity with new/old modules).
def _cho_factor_pd(*args: Any, **kwargs: Any):
    fn = getattr(_impl, "_cho_factor_pd", None)
    if fn is None:
        raise AttributeError(f"{_impl.__name__} has no attribute _cho_factor_pd")
    return fn(*args, **kwargs)


def _cho_solve(*args: Any, **kwargs: Any):
    fn = getattr(_impl, "_cho_solve", None)
    if fn is None:
        raise AttributeError(f"{_impl.__name__} has no attribute _cho_solve")
    return fn(*args, **kwargs)


def __getattr__(name: str):
    # Forward any other attribute lookups to the underlying implementation.
    try:
        return getattr(_impl, name)
    except AttributeError as e:
        raise AttributeError(f"state_space_new_cached has no attribute {name}") from e


def __dir__():
    base = set(globals().keys())
    impl = set(dir(_impl))
    return sorted(base | impl)


__all__ = [
    "kalman_filter_only",
    "kalman_filter_smoother",
    "_cho_factor_pd",
    "_cho_solve",
]
