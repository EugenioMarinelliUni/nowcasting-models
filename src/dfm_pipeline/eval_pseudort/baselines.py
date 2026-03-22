from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


def rmse(yhat: np.ndarray, y: np.ndarray) -> float:
    yhat = np.asarray(yhat, float)
    y = np.asarray(y, float)
    m = np.isfinite(yhat) & np.isfinite(y)
    if m.sum() == 0:
        return float("nan")
    e = yhat[m] - y[m]
    return float(np.sqrt(np.mean(e * e)))


def directional_accuracy_qoq(yhat: np.ndarray, y: np.ndarray) -> float:
    """
    Directional accuracy on quarter-to-quarter changes.

    Must match bm_pseudort_fast implementation:
        DA = mean( (diff(pred) >= 0) == (diff(actual) >= 0) )
    computed after dropping NaNs pairwise on (pred, actual).
    """
    p = np.asarray(yhat, float)
    a = np.asarray(y, float)
    m = np.isfinite(p) & np.isfinite(a)
    p = p[m]
    a = a[m]
    if p.size < 2:
        return float("nan")
    return float(np.mean((np.diff(p) >= 0) == (np.diff(a) >= 0)))


def baseline_no_change(y: np.ndarray) -> np.ndarray:
    """
    No-change baseline on the quarterly sequence:
        yhat_t = y_{t-1}
    """
    y = np.asarray(y, float)
    yhat = np.full_like(y, np.nan, dtype=float)
    if y.size >= 2:
        yhat[1:] = y[:-1]
    return yhat


def baseline_ar1_expanding(y: np.ndarray, min_obs: int = 8) -> np.ndarray:
    """
    Expanding-window AR(1) with intercept:
        y_t = a + b y_{t-1} + e_t

    For each t >= 2, fit on data up to t-1 (expanding window) and forecast y_t.
    """
    y = np.asarray(y, float)
    yhat = np.full_like(y, np.nan, dtype=float)

    for t in range(2, len(y)):
        yt = y[1:t]      # y_1..y_{t-1}
        ylag = y[0:t-1]  # y_0..y_{t-2}
        m = np.isfinite(yt) & np.isfinite(ylag)
        if m.sum() < int(min_obs):
            continue

        X = np.column_stack([np.ones(m.sum()), ylag[m]])
        beta = np.linalg.lstsq(X, yt[m], rcond=None)[0]
        a_hat, b_hat = float(beta[0]), float(beta[1])

        if np.isfinite(y[t - 1]):
            yhat[t] = a_hat + b_hat * y[t - 1]

    return yhat


@dataclass(frozen=True)
class BaselineReport:
    miq_csv: str
    n_quarters: int

    dfm_rmse_m1: float
    dfm_rmse_m2: float
    dfm_rmse_m3: float
    dfm_da_m3: float

    nc_rmse: float
    nc_da: float

    ar1_rmse: float
    ar1_da: float


def load_miq_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["target_date"]).set_index("target_date").sort_index()
    required = {"actual", "nowcast_m1", "nowcast_m2", "nowcast_m3"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"MIQ CSV missing columns: {sorted(missing)}")
    return df


def evaluate_miq_csv(path: str | Path, *, min_ar1_obs: int = 8) -> BaselineReport:
    df = load_miq_csv(path)
    y = df["actual"].astype(float).to_numpy()

    m1 = df["nowcast_m1"].astype(float).to_numpy()
    m2 = df["nowcast_m2"].astype(float).to_numpy()
    m3 = df["nowcast_m3"].astype(float).to_numpy()

    yhat_nc = baseline_no_change(y)
    yhat_ar1 = baseline_ar1_expanding(y, min_obs=int(min_ar1_obs))

    return BaselineReport(
        miq_csv=str(path),
        n_quarters=int(np.isfinite(y).sum()),
        dfm_rmse_m1=rmse(m1, y),
        dfm_rmse_m2=rmse(m2, y),
        dfm_rmse_m3=rmse(m3, y),
        dfm_da_m3=directional_accuracy_qoq(m3, y),
        nc_rmse=rmse(yhat_nc, y),
        nc_da=directional_accuracy_qoq(yhat_nc, y),
        ar1_rmse=rmse(yhat_ar1, y),
        ar1_da=directional_accuracy_qoq(yhat_ar1, y),
    )