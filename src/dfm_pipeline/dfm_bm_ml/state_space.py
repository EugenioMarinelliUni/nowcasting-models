"""
Thin shim for backward-compatibility.

The BM-DFM code historically imported Kalman routines from:
  dfm_pipeline.dfm_bm_ml.state_space

We now route everything through dfm_pipeline.dfm_dyn.state_space, which
supports multiple implementations via DFM_STATE_SPACE_IMPL.
"""

from __future__ import annotations

import dfm_pipeline.dfm_dyn.state_space as ss  # respects DFM_STATE_SPACE_IMPL

KalmanFilterResult = getattr(ss, "KalmanFilterResult", dict)
KalmanSmootherResult = ss.KalmanSmootherResult

build_companion_transition = ss.build_companion_transition
build_dfm_state_space = ss.build_dfm_state_space

kalman_filter_only = ss.kalman_filter_only
kalman_filter_smoother = ss.kalman_filter_smoother
kalman_filter = ss.kalman_filter
kalman_smoother = ss.kalman_smoother

__all__ = [
    "KalmanFilterResult",
    "KalmanSmootherResult",
    "build_companion_transition",
    "build_dfm_state_space",
    "kalman_filter_only",
    "kalman_filter_smoother",
    "kalman_filter",
    "kalman_smoother",
]
