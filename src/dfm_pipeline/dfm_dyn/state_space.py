"""Canonical Kalman filter/smoother API for the DFM.

The former environment-selectable experimental backends were removed because
they did not share identical filtering and smoothing semantics. All callers now
use the single validated multivariate implementation in ``state_space_new``.
"""

from __future__ import annotations

from .state_space_new import (
    StateSpaceParams,
    KalmanSmootherResult,
    build_companion_transition,
    build_dfm_state_space,
    kalman_filter_only,
    kalman_filter_smoother,
    kalman_filter,
    kalman_smoother,
)

# The canonical filter-only API returns a dictionary. Retain the historical
# public type alias without reintroducing a separate backend-specific class.
KalmanFilterResult = dict

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
