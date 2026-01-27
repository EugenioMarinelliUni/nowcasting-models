"""
State-space implementation selector for dfm_dyn.

Controls which implementation is used at runtime via env var:

- DFM_STATE_SPACE_IMPL=old        -> state_space_old
- DFM_STATE_SPACE_IMPL=new_cached -> state_space_new_cached
- (default / anything else)       -> state_space_new

This module intentionally re-exports the public API used by callers
(e.g., dfm_bm_ml/fast/em_fast.py) so import sites remain stable.
"""

from __future__ import annotations

import os
from typing import Any


def _select_impl():
    impl = os.getenv("DFM_STATE_SPACE_IMPL", "").strip().lower()
    if impl == "old":
        from . import state_space_old as ss  # type: ignore
    elif impl == "new_cached":
        from . import state_space_new_cached as ss  # type: ignore
    else:
        from . import state_space_new as ss  # type: ignore
    return ss


_ss = _select_impl()

# ---- Public API wrappers (stable names for callers) ----

def kalman_filter_only(*args: Any, **kwargs: Any):
    return _ss.kalman_filter_only(*args, **kwargs)


def kalman_filter_smoother(*args: Any, **kwargs: Any):
    return _ss.kalman_filter_smoother(*args, **kwargs)


# ---- Optional helpers frequently referenced in profiling / internal code ----
# If the selected impl has these, expose them under the same names.

def _cho_factor_pd(*args: Any, **kwargs: Any):
    fn = getattr(_ss, "_cho_factor_pd", None)
    if fn is None:
        raise AttributeError(f"{_ss.__name__} has no attribute _cho_factor_pd")
    return fn(*args, **kwargs)


def _cho_solve(*args: Any, **kwargs: Any):
    fn = getattr(_ss, "_cho_solve", None)
    if fn is None:
        raise AttributeError(f"{_ss.__name__} has no attribute _cho_solve")
    return fn(*args, **kwargs)


def __getattr__(name: str):
    # Forward any other attribute lookups to the selected implementation.
    try:
        return getattr(_ss, name)
    except AttributeError as e:
        raise AttributeError(f"state_space dispatcher has no attribute {name}") from e


def __dir__():
    # Help IDEs / completion.
    base = set(globals().keys())
    impl = set(dir(_ss))
    return sorted(base | impl)


__all__ = [
    "kalman_filter_only",
    "kalman_filter_smoother",
    "_cho_factor_pd",
    "_cho_solve",
]
