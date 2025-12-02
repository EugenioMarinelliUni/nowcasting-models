# src/dfm_pipeline/covid/covid_make_delete_weights.py
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
    # Take only YYYY-MM if a full YYYY-MM-DD is provided
    ym = x[:7]
    p = pd.Period(ym, "M")
    how = "start" if str(monthly_freq).upper() == "MS" else "end"
    return p.to_timestamp(how=how)


def make_delete_mask(
    index: pd.DatetimeIndex,
    covid_start: str,
    covid_end: str,
    *,
    monthly_freq: str = "MS",
) -> pd.Series:
    """
    0/1 mask: False (0) inside COVID window, True (1) outside.

    Parameters
    ----------
    index : DatetimeIndex
        Monthly index (month-start or month-end, depending on your pipeline).
    covid_start, covid_end : str
        Bounds of the Covid window, 'YYYY-MM' or 'YYYY-MM-DD'.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end as monthly timestamps.

    Returns
    -------
    mask : Series[bool]
        Boolean mask with the same index; False in Covid window, True outside.
    """
    idx = pd.DatetimeIndex(index)
    s = _ts_month_boundary(covid_start, monthly_freq=monthly_freq)
    e = _ts_month_boundary(covid_end, monthly_freq=monthly_freq)

    if s > e:
        raise ValueError(f"covid_start {s.date()} > covid_end {e.date()}")

    keep = ~((idx >= s) & (idx <= e))
    return pd.Series(keep, index=idx, name="covid_keep")


def apply_delete_nan(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    *,
    monthly_freq: str = "MS",
) -> pd.DataFrame:
    """
    Set predictor values to NaN inside the COVID window [start, end].

    This implements 'covid_delete' by marking all Covid-month observations
    as missing. You can apply this to:

      - a full standardized panel (train + OOS, e.g. 1990..end), or
      - any other monthly panel with a DatetimeIndex.

    Parameters
    ----------
    X : DataFrame
        Standardized predictors (full panel), indexed by dates.
    covid_start, covid_end : str
        Covid window bounds, 'YYYY-MM' or 'YYYY-MM-DD'.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end (month-start vs month-end).

    Returns
    -------
    X_out : DataFrame
        Copy of X, with values set to NaN in the Covid window.
    """
    if X.empty:
        raise ValueError("Input panel X is empty.")

    X_out = X.copy()
    idx = pd.DatetimeIndex(X_out.index)

    s = _ts_month_boundary(covid_start, monthly_freq=monthly_freq)
    e = _ts_month_boundary(covid_end, monthly_freq=monthly_freq)

    if s > e:
        raise ValueError(f"covid_start {s.date()} > covid_end {e.date()}")

    mask = (idx >= s) & (idx <= e)
    if not mask.any():
        # No overlap; return unchanged copy
        return X_out

    X_out.loc[mask, :] = float("nan")
    return X_out
