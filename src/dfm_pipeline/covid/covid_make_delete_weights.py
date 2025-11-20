# src/dfm_pipeline/covid/covid_make_delete_weights.py
from __future__ import annotations

import pandas as pd


def _ts_month_end(x: str) -> pd.Timestamp:
    """
    Convert 'YYYY-MM' or 'YYYY-MM-DD' string to month-end Timestamp.
    Assumes your panel is indexed at month-end (as in your DFM pipeline).
    """
    p = pd.Period(x[:7], "M")
    return p.to_timestamp(how="end")


def make_delete_mask(
    index: pd.DatetimeIndex,
    covid_start: str,
    covid_end: str,
) -> pd.Series:
    """
    0/1 mask: False (0) inside COVID window, True (1) outside.

    Parameters
    ----------
    index : monthly DatetimeIndex (month-end in your pipeline)
    covid_start, covid_end : strings 'YYYY-MM' or 'YYYY-MM-DD'

    Returns
    -------
    mask : Series[bool], index = index
    """
    idx = pd.DatetimeIndex(index)
    s = _ts_month_end(covid_start)
    e = _ts_month_end(covid_end)
    keep = ~((idx >= s) & (idx <= e))
    return pd.Series(keep, index=idx, name="covid_keep")


def apply_delete_nan(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
) -> pd.DataFrame:
    """
    Set predictor values to NaN inside the COVID window [start, end].

    This is the natural implementation of 'covid_delete' in your
    make_train_oos_from_raw.py pipeline: you keep pre-/post-COVID OOS,
    but mark COVID months as unusable.

    Parameters
    ----------
    X : DataFrame, standardized predictors (OOS).
    covid_start, covid_end : 'YYYY-MM' or 'YYYY-MM-DD'

    Returns
    -------
    X_out : DataFrame (copy), with values NaN in the COVID window.
    """
    X = X.copy()
    idx = pd.DatetimeIndex(X.index)
    s = _ts_month_end(covid_start)
    e = _ts_month_end(covid_end)
    mask = (idx >= s) & (idx <= e)
    X.loc[mask, :] = float("nan")
    return X
