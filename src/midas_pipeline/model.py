from __future__ import annotations

from dataclasses import dataclass
import numpy as np


@dataclass
class MIDASModel:
    a: float
    b: float
    w: np.ndarray
    beta: np.ndarray
    predictor: str
    n_monthly_lags: int
    n_y_lags: int