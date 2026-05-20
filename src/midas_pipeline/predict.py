from __future__ import annotations

import numpy as np
import pandas as pd

from .model import MIDASModel


def predict_univariate_midas(
    model: MIDASModel,
    x_row: pd.Series | pd.DataFrame,
) -> float:
    if isinstance(x_row, pd.DataFrame):
        if len(x_row) != 1:
            raise ValueError("x_row DataFrame must contain exactly one row")
        row = x_row.iloc[0]
    else:
        row = x_row

    xlags = np.array(
        [float(row[f"xlag_{i}"]) for i in range(1, model.n_monthly_lags + 1)],
        dtype=float,
    )

    ylags = np.array(
        [float(row[f"ylag_{i}"]) for i in range(1, model.n_y_lags + 1)],
        dtype=float,
    )

    if model.is_unrestricted:
        z = np.concatenate([[1.0], xlags, ylags])
    else:
        x_weighted = float(np.dot(xlags, model.lag_weights))
        z = np.concatenate([[1.0, x_weighted], ylags])

    if len(z) != len(model.beta):
        raise ValueError(
            f"Design vector length mismatch: got {len(z)}, expected {len(model.beta)}"
        )

    pred = float(np.dot(z, model.beta))
    return pred