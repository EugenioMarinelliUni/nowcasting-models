# src/dfm_pipeline/preprocessing/fixed_window_standardize.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Literal
import pandas as pd


@dataclass(frozen=True)
class FixedStdStats:
    mean: pd.Series
    std: pd.Series
    start: pd.Timestamp
    end: pd.Timestamp
    nobs: pd.Series


def _month_index(idx, monthly_freq: str = "MS") -> pd.DatetimeIndex:
    di = pd.DatetimeIndex(pd.to_datetime(idx, errors="coerce"))
    # Explicit Literal type to keep static checkers happy
    how: Literal["start", "end"] = "start" if str(monthly_freq).upper() == "MS" else "end"
    return di.to_period("M").to_timestamp(how=how)


def standardize_panel_on_window(
    X_raw: pd.DataFrame,
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    monthly_freq: str = "MS",
    min_obs_per_col: int = 1,
) -> Tuple[pd.DataFrame, FixedStdStats]:
    """
    Standardize (z-score) a stationarized panel using μ,σ computed on a single
    training window. Returns (Z_train, stats). Z_train spans exactly [start, end].
    """
    X = X_raw.copy()
    X.index = _month_index(X.index, monthly_freq=monthly_freq)
    X = X.sort_index().select_dtypes(include="number")

    Xw = X.loc[pd.to_datetime(start):pd.to_datetime(end)]
    if Xw.empty:
        raise ValueError("Empty training slice; check start/end.")

    cnt = Xw.notna().sum()
    keep = cnt[cnt >= int(min_obs_per_col)].index
    Xw = Xw[keep]
    cnt = cnt[keep]

    mu = Xw.mean(skipna=True)
    sd = Xw.std(skipna=True, ddof=0).where(lambda s: s > 1e-12, 1e-12)

    Z = (Xw - mu) / sd

    stats = FixedStdStats(
        mean=mu,
        std=sd,
        start=pd.to_datetime(start),
        end=pd.to_datetime(end),
        nobs=cnt,
    )
    return Z, stats


def standardize_full_panel_on_window(
    X_raw: pd.DataFrame,
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    monthly_freq: str = "MS",
    min_obs_per_col: int = 1,
) -> Tuple[pd.DataFrame, FixedStdStats]:
    """
    Standardize a *full* stationarized panel using μ,σ computed on a single
    training window [start, end].

    - Compute μ,σ on [start, end] (dropping columns with < min_obs_per_col in that window).
    - Apply those μ,σ to the entire monthly history (all dates in X_raw).
    - Return (Z_full, stats), where Z_full covers the full index of X_raw.
    """
    X = X_raw.copy()
    X.index = _month_index(X.index, monthly_freq=monthly_freq)
    X = X.sort_index().select_dtypes(include="number")

    # training window slice
    Xw = X.loc[pd.to_datetime(start):pd.to_datetime(end)]
    if Xw.empty:
        raise ValueError("Empty training slice; check start/end.")

    cnt = Xw.notna().sum()
    keep = cnt[cnt >= int(min_obs_per_col)].index
    Xw = Xw[keep]
    cnt = cnt[keep]

    mu = Xw.mean(skipna=True)
    sd = Xw.std(skipna=True, ddof=0).where(lambda s: s > 1e-12, 1e-12)

    # apply frozen μ,σ to **full** history, but only for kept columns
    X_kept = X[keep]
    Z_full = (X_kept - mu) / sd

    stats = FixedStdStats(
        mean=mu,
        std=sd,
        start=pd.to_datetime(start),
        end=pd.to_datetime(end),
        nobs=cnt,
    )
    return Z_full, stats
