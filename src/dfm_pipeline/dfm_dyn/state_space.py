# src/dfm_pipeline/dfm_dyn/state_space.py

"""
Dispatcher for Kalman filter/smoother implementations.

Selection is controlled by env var DFM_STATE_SPACE_IMPL.
Known values (case-insensitive):
  - "" or "new":            state_space_new
  - "new_refactor":         state_space_new_refactor
  - "new_cached":           state_space_new_cached
  - "old":                  state_space_old
  - "new_uni":              state_space_new_uni
  - "new_uni_numba":        state_space_new_uni_numba

This module re-exports a stable compatibility surface across backends.
When a selected backend does not implement some helper (for example the
univariate backends do not expose build_dfm_state_space), the implementation
falls back to the canonical NEW backend for that helper.
"""

from __future__ import annotations

import os
from typing import Any, cast

from . import state_space_new as _ss_default


def _select_backend():
    impl = os.getenv("DFM_STATE_SPACE_IMPL", "").strip().lower()
    if impl in ("", "new"):
        from . import state_space_new as mod
    elif impl == "new_refactor":
        from . import state_space_new_refactor as mod
    elif impl == "new_cached":
        from . import state_space_new_cached as mod
    elif impl == "old":
        from . import state_space_old as mod
    elif impl == "new_uni":
        from . import state_space_new_uni as mod
    elif impl == "new_uni_numba":
        from . import state_space_new_uni_numba as mod
    else:
        from . import state_space_new as mod
    return impl, mod


_impl_name, _ss_mod = _select_backend()
_ss = cast(Any, _ss_mod)
_ss_default_any = cast(Any, _ss_default)

StateSpaceParams = getattr(_ss, "StateSpaceParams", _ss_default.StateSpaceParams)
KalmanFilterResult = getattr(_ss, "KalmanFilterResult", dict)
KalmanSmootherResult = getattr(_ss, "KalmanSmootherResult", _ss_default.KalmanSmootherResult)

build_companion_transition = getattr(
    _ss,
    "build_companion_transition",
    _ss_default_any.build_companion_transition,
)
build_dfm_state_space = getattr(
    _ss,
    "build_dfm_state_space",
    _ss_default_any.build_dfm_state_space,
)

kalman_filter_only = getattr(_ss, "kalman_filter_only", _ss_default_any.kalman_filter_only)
kalman_filter_smoother = getattr(_ss, "kalman_filter_smoother", _ss_default_any.kalman_filter_smoother)
kalman_filter = getattr(_ss, "kalman_filter", _ss_default_any.kalman_filter)
kalman_smoother = getattr(_ss, "kalman_smoother", _ss_default_any.kalman_smoother)

__all__ = [
    "StateSpaceParams",
    "KalmanFilterResult",
    "KalmanSmootherResult",
    "build_companion_transition",
    "build_dfm_state_space",
    "kalman_filter_only",
    "kalman_filter_smoother",
    "kalman_filter",
    "kalman_smoother",
]
