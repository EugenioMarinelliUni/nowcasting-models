# src/dfm_pipeline/covid/covid_make_dummies_resid.py
from __future__ import annotations

from typing import Tuple

import numpy as np
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


def _build_dummy_matrix(
    idx: pd.Index,
    covid_start: str,
    covid_end: str,
    separate: bool,
    *,
    monthly_freq: str = "MS",
) -> pd.DataFrame:
    """
    Build dummy matrix for the COVID window.

    Parameters
    ----------
    idx : Index
        Date index of the panel (monthly).
    covid_start, covid_end : str
        Covid window bounds, 'YYYY-MM' or 'YYYY-MM-DD'.
    separate : bool
        - False -> one 'covid' dummy (on/off in window)
        - True  -> one dummy per month in the window
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end (month-start vs month-end).

    Returns
    -------
    D : DataFrame
        Dummy matrix aligned to idx.
    """
    dt_idx = pd.DatetimeIndex(idx)
    s = _ts_month_boundary(covid_start, monthly_freq=monthly_freq)
    e = _ts_month_boundary(covid_end, monthly_freq=monthly_freq)

    if s > e:
        raise ValueError(f"covid_start {s.date()} > covid_end {e.date()}")

    if separate:
        # Monthly dates over the window (respecting boundary choice)
        # For month-start panels this will generate month-end, but we match by year/month,
        # not by exact day, so it still selects the right rows.
        months = pd.period_range(s, e, freq="M").to_timestamp(
            how="start" if str(monthly_freq).upper() == "MS" else "end"
        )
        cols = [f"covid_{d.strftime('%Y-%m')}" for d in months]
        D = pd.DataFrame(0.0, index=dt_idx, columns=cols)
        for d, col in zip(months, cols):
            m = (dt_idx.year == d.year) & (dt_idx.month == d.month)
            D.loc[m, col] = 1.0
    else:
        D = pd.DataFrame(
            {"covid": ((dt_idx >= s) & (dt_idx <= e)).astype(float)},
            index=dt_idx,
        )

    return D


def residualize_on_dummy(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    separate: bool = False,
    min_obs: int = 5,
    *,
    monthly_freq: str = "MS",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Regress each series on [const, dummies] and subtract only the dummy component.

    This can be applied to:
      - an OOS panel, or
      - a full standardized panel (train + OOS, e.g. 1990..end).

    Parameters
    ----------
    X : DataFrame
        Standardized predictors (any monthly panel with a DatetimeIndex).
    covid_start, covid_end : str
        'YYYY-MM' or 'YYYY-MM-DD'.
    separate : bool, default False
        If True, use a separate dummy for each month in the Covid window.
        If False, use a single on/off dummy for the whole window.
    min_obs : int, default 5
        Minimum non-NaN observations to run regression for a series.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end (month-start vs month-end).

    Returns
    -------
    X_adj : DataFrame
        Same shape as X; Covid dummy component removed.
    D : DataFrame
        Dummy regressors used (for inspection or reuse).
    """
    if X.empty:
        raise ValueError("Input panel X is empty.")

    X = X.copy()
    dt_idx = pd.DatetimeIndex(X.index)
    X.index = dt_idx  # normalize index type

    # Build dummy matrix aligned with X
    D = _build_dummy_matrix(
        dt_idx,
        covid_start=covid_start,
        covid_end=covid_end,
        separate=separate,
        monthly_freq=monthly_freq,
    )

    # Regress on [const, D]
    const = pd.Series(1.0, index=D.index, name="const")
    Z = pd.concat([const, D], axis=1).to_numpy(dtype=float)

    X_adj = pd.DataFrame(index=D.index, columns=X.columns, dtype=float)

    for sname in X.columns:
        y = X[sname].to_numpy(dtype=float)
        m = ~np.isnan(y)
        if m.sum() < min_obs:
            # too few points; leave series as-is
            X_adj[sname] = X[sname]
            continue

        Zm, ym = Z[m], y[m]
        beta = np.linalg.pinv(Zm.T @ Zm) @ (Zm.T @ ym)

        # Drop const, keep only dummy regressors
        D_all = Z[:, 1:]
        # Subtract only the dummy component
        X_adj[sname] = y - (D_all @ beta[1:])

    return X_adj, D


def apply_dummies_resid(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    separate: bool = False,
    *,
    monthly_freq: str = "MS",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience wrapper: dummy-residualize X and return (X_adj, D).

    This is the natural implementation for a 'covid_dummies' correction:
    - Adjust predictors so that the Covid dummy effect is removed.
    - Optionally, the dummy regressors D can be added to the panel upstream.

    Parameters
    ----------
    X : DataFrame
        Standardized predictors (full panel or OOS).
    covid_start, covid_end : str
        'YYYY-MM' or 'YYYY-MM-DD'.
    separate : bool, default False
        Single dummy vs one-per-month.
    monthly_freq : {"MS", "ME"}, default "MS"
        How to interpret covid_start/covid_end.

    Returns
    -------
    X_adj : DataFrame
    D : DataFrame
    """
    return residualize_on_dummy(
        X,
        covid_start=covid_start,
        covid_end=covid_end,
        separate=separate,
        monthly_freq=monthly_freq,
    )
