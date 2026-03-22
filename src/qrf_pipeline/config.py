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