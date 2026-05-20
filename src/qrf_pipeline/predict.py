from __future__ import annotations

import numpy as np
import pandas as pd

from .model import QRFPointModel, QRFQuantileModel


def predict_qrf_point(model: QRFPointModel, X_row: pd.DataFrame) -> float:
    X = X_row[model.feature_columns]
    return float(model.estimator.predict(X)[0])


def predict_qrf_quantiles(
    model: QRFQuantileModel,
    X_row: pd.DataFrame,
) -> dict[float, float]:
    X = X_row[model.feature_columns]
    q_pred = model.estimator.predict(X, quantiles=list(model.quantiles))

    arr = np.asarray(q_pred, dtype=float)

    if arr.ndim == 0:
        arr = arr.reshape(1)

    if arr.ndim == 2:
        arr = arr[0]

    if len(arr) != len(model.quantiles):
        raise RuntimeError(
            f"Quantile prediction length mismatch: got {len(arr)}, "
            f"expected {len(model.quantiles)}"
        )

    return {
        float(q): float(v)
        for q, v in zip(model.quantiles, arr)
    }