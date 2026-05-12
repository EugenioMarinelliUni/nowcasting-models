from __future__ import annotations

import pandas as pd


def extract_midas_features(
    Xv: pd.DataFrame,
    yv: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
) -> dict | None:
    if predictor not in Xv.columns:
        return None

    x = Xv[predictor].dropna()
    y = yv.dropna()

    if len(x) < n_monthly_lags or len(y) < n_y_lags:
        return None

    out = {}

    xlags = x.iloc[-n_monthly_lags:][::-1]
    ylags = y.iloc[-n_y_lags:][::-1]

    for i, val in enumerate(xlags, start=1):
        out[f"xlag_{i}"] = float(val)

    for i, val in enumerate(ylags, start=1):
        out[f"ylag_{i}"] = float(val)

    return out