from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QRFConfig:
    predictors: list[str]
    n_lags: int = 3
    n_y_lags: int = 2
    min_train_rows: int = 36

    n_estimators: int = 500
    min_samples_leaf: int = 10
    max_features: str | float = "sqrt"
    random_state: int = 0

    # "rf_point" = current point RandomForestRegressor benchmark
    # "qrf" = true Quantile Regression Forest backend
    backend: str = "rf_point"

    # Used only when backend == "qrf"
    quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)