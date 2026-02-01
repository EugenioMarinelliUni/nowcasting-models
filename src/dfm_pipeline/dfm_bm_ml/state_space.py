"""
Thin shim for backward-compatibility.

The BM-DFM code historically imported Kalman routines from:
  dfm_pipeline.dfm_bm_ml.state_space

We now route everything through dfm_pipeline.dfm_dyn.state_space, which
supports multiple implementations via DFM_STATE_SPACE_IMPL.
"""

from __future__ import annotations

from dfm_pipeline.dfm_dyn import state_space as ss  # respects DFM_STATE_SPACE_IMPL

KalmanFilterResult = ss.KalmanFilterResult
KalmanSmootherResult = ss.KalmanSmootherResult

kalman_filter_only = ss.kalman_filter_only
kalman_filter_smoother = ss.kalman_filter_smoother
