from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from .lag_weights import beta_weights
from .model import MIDASModel


def fit_univariate_midas(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
) -> MIDASModel:
    x_cols = [f"xlag_{i}" for i in range(1, n_monthly_lags + 1)]
    y_cols = [f"ylag_{i}" for i in range(1, n_y_lags + 1)]

    Xx = X_train[x_cols].to_numpy(dtype=float)
    Xy = X_train[y_cols].to_numpy(dtype=float)
    yt = y_train.to_numpy(dtype=float)

    def objective(theta: np.ndarray) -> float:
        a = np.exp(theta[0])
        b = np.exp(theta[1])
        w = beta_weights(n_monthly_lags, a, b)
        xw = Xx @ w
        Z = np.column_stack([np.ones(len(X_train)), xw, Xy])
        beta = np.linalg.lstsq(Z, yt, rcond=None)[0]
        resid = yt - Z @ beta
        return float(np.mean(resid ** 2))

    res = minimize(objective, x0=np.array([0.0, 0.0]), method="L-BFGS-B")
    a = float(np.exp(res.x[0]))
    b = float(np.exp(res.x[1]))
    w = beta_weights(n_monthly_lags, a, b)

    xw = Xx @ w
    Z = np.column_stack([np.ones(len(X_train)), xw, Xy])
    beta = np.linalg.lstsq(Z, yt, rcond=None)[0]

    return MIDASModel(
        a=a,
        b=b,
        w=w,
        beta=beta,
        predictor=predictor,
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
    )