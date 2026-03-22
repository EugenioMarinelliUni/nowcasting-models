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

This module re-exports:
  StateSpaceParams,
  kalman_filter_only, kalman_filter_smoother,
  kalman_filter, kalman_smoother.
"""

import os
from typing import Any, cast

from .state_space_new import StateSpaceParams as _DefaultSSP

_impl = os.getenv("DFM_STATE_SPACE_IMPL", "").strip().lower()

if _impl in ("", "new"):
    from . import state_space_new as _ss_mod
elif _impl == "new_refactor":
    from . import state_space_new_refactor as _ss_mod
elif _impl == "new_cached":
    from . import state_space_new_cached as _ss_mod
elif _impl == "old":
    from . import state_space_old as _ss_mod
elif _impl == "new_uni":
    from . import state_space_new_uni as _ss_mod
elif _impl == "new_uni_numba":
    from . import state_space_new_uni_numba as _ss_mod
else:
    from . import state_space_new as _ss_mod

_ss = cast(Any, _ss_mod)

StateSpaceParams = getattr(_ss, "StateSpaceParams", _DefaultSSP)
kalman_filter_only = _ss.kalman_filter_only
kalman_filter_smoother = _ss.kalman_filter_smoother

KalmanSmootherResult = getattr(_ss, "KalmanSmootherResult", None)
kalman_filter = getattr(_ss, "kalman_filter", None)
kalman_smoother = getattr(_ss, "kalman_smoother", None)

__all__ = [
    "StateSpaceParams",
    "KalmanSmootherResult",
    "kalman_filter_only",
    "kalman_filter_smoother",
    "kalman_filter",
    "kalman_smoother",
]