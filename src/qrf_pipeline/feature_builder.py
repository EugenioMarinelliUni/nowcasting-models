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
    feat = {}

    for col in predictors:
        if col not in Xv.columns:
            return None

        vals = Xv[col].dropna()
        if len(vals) < max(n_lags, 3):
            return None

        feat[f"{col}_last"] = float(vals.iloc[-1])
        for j in range(1, n_lags + 1):
            feat[f"{col}_lag{j}"] = float(vals.iloc[-j])

        feat[f"{col}_ma3"] = float(vals.iloc[-3:].mean())
        feat[f"{col}_chg3"] = float(vals.iloc[-1] - vals.iloc[-3])

    yvals = yv.dropna()
    if len(yvals) < n_y_lags:
        return None

    for j in range(1, n_y_lags + 1):
        feat[f"y_lag{j}"] = float(yvals.iloc[-j])

    feat["moq"] = month_of_quarter(pd.Timestamp(eval_date))
    return feat