# src/dfm_pipeline/covid/covid_make_winsorized.py
from __future__ import annotations

import pandas as pd


def _ts_month_boundary(x: str, monthly_freq: str = "MS") -> pd.Timestamp:
    """
    Convert 'YYYY-MM' or 'YYYY-MM-DD' string to a month boundary Timestamp.

    Parameters
    ----------
    x : str
        Date-like string, at least 'YYYY-MM'.
    monthly_freq : {"MS", "ME"}
        - "MS": normalize to month-start.
        - "ME": normalize to month-end.

    Returns
    -------
    pd.Timestamp
    """
    ym = x[:7]
    p = pd.Period(ym, "M")
    how = "start" if str(monthly_freq).upper() == "MS" else "end"
    return p.to_timestamp(how=how)


def make_covid_window_mask(
    index: pd.Index,
    covid_start: str,
    covid_end: str,
    *,
    monthly_freq: str = "MS",
) -> pd.Series:
    """
    Boolean mask inside [covid_start, covid_end] on a monthly index.

    Parameters
    ----------
    index : Index
        Date index of the panel (monthly).
    covid_start, covid_end : str
        'YYYY-MM' or 'YYYY-MM-DD'.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end.

    Returns
    -------
    mask : Series[bool]
        True for rows in the Covid window, False otherwise.
    """
    dt_idx = pd.DatetimeIndex(index)
    s = _ts_month_boundary(covid_start, monthly_freq=monthly_freq)
    e = _ts_month_boundary(covid_end, monthly_freq=monthly_freq)

    if s > e:
        raise ValueError(f"covid_start {s.date()} > covid_end {e.date()}")

    m = (dt_idx >= s) & (dt_idx <= e)
    return pd.Series(m, index=dt_idx, name="covid_window")


def apply_winsor_sigma(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    clip_sigma: float = 6.0,
    *,
    monthly_freq: str = "MS",
) -> pd.DataFrame:
    """
    Clip standardized predictors in the COVID window to [-clip_sigma, +clip_sigma].

    This assumes X is already standardized (z-scored) on a pre-Covid training
    window. It can be:

      - an OOS panel, or
      - a full standardized panel (train + OOS, 1990..end).

    Parameters
    ----------
    X : DataFrame
        Standardized predictors.
    covid_start, covid_end : str
        'YYYY-MM' or 'YYYY-MM-DD'.
    clip_sigma : float, default 6.0
        Symmetric threshold in standard deviations.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end.

    Returns
    -------
    X_out : DataFrame
        Copy of X, with values clipped to [-clip_sigma, clip_sigma] in the Covid window.
    """
    if X.empty:
        raise ValueError("Input panel X is empty.")

    Xw = X.copy()

    mask = make_covid_window_mask(
        Xw.index,
        covid_start=covid_start,
        covid_end=covid_end,
        monthly_freq=monthly_freq,
    )

    if mask.any():
        Xw.loc[mask, :] = Xw.loc[mask, :].clip(
            lower=-clip_sigma,
            upper=clip_sigma,
            axis=1,
        )

    return Xw
