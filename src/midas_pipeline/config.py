from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MIDASConfig:
    predictors: list[str]
    n_monthly_lags: int = 12
    n_y_lags: int = 2
    min_train_rows: int = 30
    beta_start_a: float = 1.5
    beta_start_b: float = 1.5