# src/dfm_pipeline/covid/covid_make_winsorized.py
from __future__ import annotations

import pandas as pd


def _ts_month_end(x: str) -> pd.Timestamp:
    """
    Convert 'YYYY-MM' or 'YYYY-MM-DD' string to a month-end Timestamp.
    """
    p = pd.Period(x[:7], "M")
    return p.to_timestamp(how="end")


def make_covid_window_mask(
    index: pd.Index,
    covid_start: str,
    covid_end: str,
) -> pd.Series:
    """
    Boolean mask inside [covid_start, covid_end] on a monthly (month-end) index.
    """
    dt_idx = pd.DatetimeIndex(index)
    s = _ts_month_end(covid_start)
    e = _ts_month_end(covid_end)
    m = (dt_idx >= s) & (dt_idx <= e)
    return pd.Series(m, index=dt_idx, name="covid_window")


def apply_winsor_sigma(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    clip_sigma: float = 6.0,
) -> pd.DataFrame:
    """
    Clip standardized predictors in the COVID window to [-clip_sigma, +clip_sigma].

    Parameters
    ----------
    X : DataFrame, standardized predictors (OOS).
    covid_start, covid_end : 'YYYY-MM' or 'YYYY-MM-DD'
    clip_sigma : float, symmetric threshold (e.g. 6.0)

    Returns
    -------
    X_out : DataFrame (copy), with values clipped in the COVID window.
    """
    Xw = X.copy()

    mask = make_covid_window_mask(Xw.index, covid_start, covid_end)  # Series[bool]

    # Only clip rows inside the COVID window
    if mask.any():
        Xw.loc[mask, :] = Xw.loc[mask, :].clip(
            lower=-clip_sigma,
            upper=clip_sigma,
            axis=1,  # explicit for type checkers
        )

    return Xw
