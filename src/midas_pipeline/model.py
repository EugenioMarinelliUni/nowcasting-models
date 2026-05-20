from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class MIDASModel:
    predictor: str
    weight_scheme: str
    n_monthly_lags: int
    n_y_lags: int
    beta: np.ndarray
    lag_weights: np.ndarray
    weight_params: dict[str, float]
    theta: np.ndarray | None = None
    is_unrestricted: bool = False


@dataclass
class MIDASFitDiagnostics:
    predictor: str
    requested_weight_scheme: str
    actual_weight_scheme: str
    converged: bool
    used_fallback: bool
    objective_value: float
    in_sample_rmse: float
    n_iter: int
    n_train_rows: int
    n_features: int
    status_message: str
    weight_params: dict[str, float]
    best_theta: list[float] | None
    best_start_index: int | None
    all_start_objectives: list[float]


@dataclass
class MIDASFitResult:
    model: MIDASModel
    diagnostics: MIDASFitDiagnostics