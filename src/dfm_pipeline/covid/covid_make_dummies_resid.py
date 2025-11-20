# src/dfm_pipeline/covid/covid_make_dummies_resid.py
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def _ts_month_end(x: str) -> pd.Timestamp:
    """
    Convert 'YYYY-MM' or 'YYYY-MM-DD' string to a month-end Timestamp.
    """
    p = pd.Period(x[:7], "M")
    return p.to_timestamp(how="end")


def _build_dummy_matrix(
    idx: pd.Index,
    covid_start: str,
    covid_end: str,
    separate: bool,
) -> pd.DataFrame:
    """
    Build dummy matrix for the COVID window.

    separate = False -> one 'covid' dummy (on/off in window)
    separate = True  -> one dummy per month in the window
    """
    # Coerce to DatetimeIndex for date comparisons
    dt_idx = pd.DatetimeIndex(idx)
    s = _ts_month_end(covid_start)
    e = _ts_month_end(covid_end)

    if separate:
        # Month-end dates over the window
        months = pd.date_range(s, e, freq="M")
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
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Regress each series on [const, dummies] and subtract only the dummy component.

    Parameters
    ----------
    X : DataFrame, standardized predictors (train or OOS).
    covid_start, covid_end : 'YYYY-MM' or 'YYYY-MM-DD'
    separate : use one dummy per month (True) or single on/off dummy (False)
    min_obs : minimum non-NaN observations to run regression

    Returns
    -------
    X_adj : DataFrame, same shape, with COVID dummy effect removed.
    D     : DataFrame, dummy regressors used (for potential reuse / inspection).
    """
    X = X.copy()
    idx = X.index

    D = _build_dummy_matrix(idx, covid_start, covid_end, separate=separate)

    const = pd.Series(1.0, index=D.index, name="const")
    Z = pd.concat([const, D], axis=1).to_numpy(dtype=float)

    X_adj = pd.DataFrame(index=D.index, columns=X.columns, dtype=float)

    for sname in X.columns:
        y = X[sname].to_numpy(dtype=float)
        m = ~np.isnan(y)
        if m.sum() < min_obs:
            # too few points; leave as-is
            X_adj[sname] = X[sname]
            continue

        Zm, ym = Z[m], y[m]
        beta = np.linalg.pinv(Zm.T @ Zm) @ (Zm.T @ ym)

        D_all = Z[:, 1:]               # drop const
        X_adj[sname] = y - (D_all @ beta[1:])  # subtract dummy component only

    return X_adj, D


def apply_dummies_resid(
    X: pd.DataFrame,
    covid_start: str,
    covid_end: str,
    separate: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience wrapper: dummy-residualize X and return (X_adj, D).

    This is the natural implementation for 'covid_dummies' in make_train_oos_from_raw:
    - Adjust predictors so that COVID dummy effect is removed.
    - Optionally also include the dummy regressors D in the panel.
    """
    return residualize_on_dummy(
        X,
        covid_start=covid_start,
        covid_end=covid_end,
        separate=separate,
    )
