from __future__ import annotations

import pandas as pd

from rt_benchmarks.targeting import month_of_quarter


def extract_qrf_features(
    Xv: pd.DataFrame,
    yv: pd.Series,
    eval_date: pd.Timestamp,
    predictors: list[str],
    n_lags: int,
    n_y_lags: int,
) -> dict | None:
    """Extract direct-forecast QRF features from a vintage view.

    Lag convention:
    - `<series>_lag1` is the latest available value in the vintage.
    - `<series>_lag2` is the observation immediately before lag1, etc.

    The previous implementation also emitted `<series>_last`, which duplicated
    `<series>_lag1`. That duplicate feature has been removed to avoid redundant
    information and misleading feature-importance totals.
    """
    if n_lags <= 0:
        raise ValueError("n_lags must be positive")
    if n_y_lags < 0:
        raise ValueError("n_y_lags cannot be negative")

    feat = {}
    min_x_obs = max(n_lags, 3)

    for col in predictors:
        if col not in Xv.columns:
            return None

        vals = pd.to_numeric(Xv[col], errors="coerce").dropna()
        if len(vals) < min_x_obs:
            return None

        for j in range(1, n_lags + 1):
            feat[f"{col}_lag{j}"] = float(vals.iloc[-j])

        feat[f"{col}_ma3"] = float(vals.iloc[-3:].mean())
        feat[f"{col}_chg3"] = float(vals.iloc[-1] - vals.iloc[-3])

    yvals = pd.to_numeric(yv, errors="coerce").dropna()
    if len(yvals) < n_y_lags:
        return None

    for j in range(1, n_y_lags + 1):
        feat[f"y_lag{j}"] = float(yvals.iloc[-j])

    feat["moq"] = month_of_quarter(pd.Timestamp(eval_date))
    return feat
