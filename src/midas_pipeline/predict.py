from __future__ import annotations

import numpy as np
import pandas as pd

from .model import MIDASModel


def predict_univariate_midas(model: MIDASModel, X_row: pd.Series) -> float:
    xlags = np.array(
        [X_row[f"xlag_{i}"] for i in range(1, model.n_monthly_lags + 1)],
        dtype=float,
    )
    ylags = np.array(
        [X_row[f"ylag_{i}"] for i in range(1, model.n_y_lags + 1)],
        dtype=float,
    )

    xw = float(xlags @ model.w)
    z = np.concatenate([[1.0, xw], ylags])
    return float(z @ model.beta)