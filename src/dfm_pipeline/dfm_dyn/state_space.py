# ============================================================
# FILE: src/dfm_pipeline/dfm_dyn/state_space.py
# (SELECTOR) Keeps your existing import paths unchanged.
#
# Environment variable:
#   DFM_STATE_SPACE_IMPL=old  -> state_space_old.py
#   DFM_STATE_SPACE_IMPL=new  -> state_space_new.py  (default)
# ============================================================

from __future__ import annotations

import os

_IMPL = os.environ.get("DFM_STATE_SPACE_IMPL", "new").strip().lower()

if _IMPL == "old":
    from .state_space_old import (  # noqa: F401
        StateSpaceParams,
        KalmanSmootherResult,
        build_companion_transition,
        build_dfm_state_space,
        kalman_filter_only,
        kalman_filter_smoother,
    )
else:
    from .state_space_new import (  # noqa: F401
        StateSpaceParams,
        KalmanSmootherResult,
        build_companion_transition,
        build_dfm_state_space,
        kalman_filter_only,
        kalman_filter_smoother,
    )

__all__ = [
    "StateSpaceParams",
    "KalmanSmootherResult",
    "build_companion_transition",
    "build_dfm_state_space",
    "kalman_filter_only",
    "kalman_filter_smoother",
]
