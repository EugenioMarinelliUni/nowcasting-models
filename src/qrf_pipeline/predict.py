from __future__ import annotations

import pandas as pd

from .model import QRFPointModel


def predict_qrf_point(model: QRFPointModel, X_row: pd.DataFrame) -> float:
    X = X_row[model.feature_columns]
    return float(model.estimator.predict(X)[0])