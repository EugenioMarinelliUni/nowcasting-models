from __future__ import annotations

import pandas as pd


def extract_midas_features(
    Xv: pd.DataFrame,
    yv: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
) -> dict[str, float] | None:
    """
    Build one numeric MIDAS feature row for one predictor.

    Output columns:
    - xlag_1, ..., xlag_K
    - ylag_1, ..., ylag_p

    No string columns are included.
    """
    if predictor not in Xv.columns:
        return None

    if n_monthly_lags <= 0:
        raise ValueError("n_monthly_lags must be positive")

    if n_y_lags < 0:
        raise ValueError("n_y_lags cannot be negative")

    x = Xv[predictor].dropna()
    y = yv.dropna()

    if len(x) < n_monthly_lags:
        return None

    if len(y) < n_y_lags:
        return None

    out: dict[str, float] = {}

    xlags = x.iloc[-n_monthly_lags:][::-1]
    for i, val in enumerate(xlags, start=1):
        out[f"xlag_{i}"] = float(val)

    if n_y_lags > 0:
        ylags = y.iloc[-n_y_lags:][::-1]
        for i, val in enumerate(ylags, start=1):
            out[f"ylag_{i}"] = float(val)

    return out