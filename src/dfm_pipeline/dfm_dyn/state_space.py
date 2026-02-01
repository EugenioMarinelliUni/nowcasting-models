"""
Implementation selector for the state-space / Kalman routines used by dfm_dyn.

Selection is controlled by env var DFM_STATE_SPACE_IMPL:

  - "old"           -> state_space_old
  - "new"           -> state_space_new
  - "new_cached"    -> state_space_new_cached
  - "new_uni"       -> state_space_new_uni
  - "new_uni_numba" -> state_space_new_uni_numba

Default: "new".

Important:
- We always source the *type definitions* (StateSpaceParams, KalmanSmootherResult)
  from state_space_new to keep a stable public API, even if an alternative
  implementation module does not define them.
- Functions are taken from the selected implementation module when available,
  otherwise they fall back to state_space_new.
"""

from __future__ import annotations

import os

from . import state_space_new as _base

_impl = os.getenv("DFM_STATE_SPACE_IMPL", "").strip().lower()

if _impl == "old":
    from . import state_space_old as _ss
elif _impl == "new_cached":
    from . import state_space_new_cached as _ss
elif _impl == "new_uni":
    from . import state_space_new_uni as _ss
elif _impl in ("new_uni_numba", "new_numba_uni", "uni_numba"):
    from . import state_space_new_uni_numba as _ss
else:
    from . import state_space_new as _ss

# --------
# Types
# --------
StateSpaceParams = _base.StateSpaceParams
KalmanSmootherResult = getattr(_ss, "KalmanSmootherResult", _base.KalmanSmootherResult)

# ------------
# Public API
# ------------
build_companion_transition = getattr(_ss, "build_companion_transition", _base.build_companion_transition)
build_dfm_state_space = getattr(_ss, "build_dfm_state_space", _base.build_dfm_state_space)

kalman_filter_only = getattr(_ss, "kalman_filter_only", _base.kalman_filter_only)
kalman_filter_smoother = getattr(_ss, "kalman_filter_smoother", _base.kalman_filter_smoother)

__all__ = [
    "StateSpaceParams",
    "KalmanSmootherResult",
    "build_companion_transition",
    "build_dfm_state_space",
    "kalman_filter_only",
    "kalman_filter_smoother",
]
