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
    max_features: str | float | int | None = "sqrt"
    random_state: int = 0
    n_jobs: int | None = -1

    # "rf_point" = point RandomForestRegressor benchmark
    # "qrf" = true Quantile Regression Forest backend
    backend: str = "rf_point"

    # Used when backend == "qrf"
    quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)

    # If True, the pseudo-RT runner stores feature-importance diagnostics
    # in pred_df.attrs["feature_importance"].
    save_feature_importance: bool = True
