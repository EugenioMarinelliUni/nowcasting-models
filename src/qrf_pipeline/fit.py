from __future__ import annotations

import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from .config import QRFConfig
from .model import QRFPointModel


def fit_qrf_point(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    cfg: QRFConfig,
) -> QRFPointModel:
    model = RandomForestRegressor(
        n_estimators=cfg.n_estimators,
        min_samples_leaf=cfg.min_samples_leaf,
        max_features=cfg.max_features,
        random_state=cfg.random_state,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    return QRFPointModel(
        estimator=model,
        feature_columns=list(X_train.columns),
    )