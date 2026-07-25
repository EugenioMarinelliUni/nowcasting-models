from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .steady_state import safe_sym


@dataclass(frozen=True)
class PredictiveMoments:
    state_mean: np.ndarray
    state_cov: np.ndarray
    observation_mean: float
    observation_var: float


def propagate_state_moments(
    mean: np.ndarray,
    covariance: np.ndarray,
    transition: np.ndarray,
    innovation_covariance: np.ndarray,
    steps: int,
) -> tuple[np.ndarray, np.ndarray]:
    a = np.asarray(mean, dtype=float).copy()
    P = np.asarray(covariance, dtype=float).copy()
    A = np.asarray(transition, dtype=float)
    Q = np.asarray(innovation_covariance, dtype=float)
    for _ in range(int(steps)):
        a = A @ a
        P = safe_sym(A @ P @ A.T + Q)
    return a, P


def observation_predictive_moments(
    state_mean: np.ndarray,
    state_cov: np.ndarray,
    measurement_row: np.ndarray,
    measurement_variance: float,
) -> PredictiveMoments:
    z = np.asarray(measurement_row, dtype=float).reshape(-1)
    a = np.asarray(state_mean, dtype=float).reshape(-1)
    P = np.asarray(state_cov, dtype=float)
    mean = float(z @ a)
    var = float(z @ P @ z.T + float(measurement_variance))
    return PredictiveMoments(a, P, mean, max(var, 0.0))


__all__ = ["PredictiveMoments", "propagate_state_moments", "observation_predictive_moments"]
