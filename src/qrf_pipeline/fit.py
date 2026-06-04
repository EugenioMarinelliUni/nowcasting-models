from __future__ import annotations

import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from .config import QRFConfig
from .model import QRFPointModel, QRFQuantileModel


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
        n_jobs=cfg.n_jobs,
    )
    model.fit(X_train, y_train)

    return QRFPointModel(
        estimator=model,
        feature_columns=list(X_train.columns),
    )


def fit_qrf_quantile(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    cfg: QRFConfig,
) -> QRFQuantileModel:
    try:
        from quantile_forest import RandomForestQuantileRegressor
    except ImportError as exc:
        raise ImportError(
            "QRF backend requires the 'quantile-forest' package. "
            "Install it with: python -m pip install quantile-forest"
        ) from exc

    if any(q <= 0.0 or q >= 1.0 for q in cfg.quantiles):
        raise ValueError("All quantiles must be strictly between 0 and 1")

    kwargs = {
        "n_estimators": cfg.n_estimators,
        "min_samples_leaf": cfg.min_samples_leaf,
        "max_features": cfg.max_features,
        "random_state": cfg.random_state,
        "default_quantiles": list(cfg.quantiles),
    }

    if cfg.n_jobs is not None:
        kwargs["n_jobs"] = cfg.n_jobs

    model = RandomForestQuantileRegressor(**kwargs)
    model.fit(X_train, y_train)

    return QRFQuantileModel(
        estimator=model,
        feature_columns=list(X_train.columns),
        quantiles=tuple(cfg.quantiles),
    )
