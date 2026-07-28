"""
Thin compatibility shim for the canonical Kalman implementation.

The BM-DFM code historically imported Kalman routines from this module. All
exports now route to the single validated implementation in
``dfm_pipeline.dfm_dyn.state_space``.
"""

from __future__ import annotations

import dfm_pipeline.dfm_dyn.state_space as ss

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
