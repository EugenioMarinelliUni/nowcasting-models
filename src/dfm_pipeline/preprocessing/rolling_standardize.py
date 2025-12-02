from __future__ import annotations
from dataclasses import dataclass
from typing import Iterator, Literal, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class RollingStats:
    """Per-column rolling mean/std used at a given cutoff."""
    mean: pd.Series
    std: pd.Series
    count: pd.Series
    window_months: int
    window_start: pd.Timestamp
    cutoff: pd.Timestamp


def _month_index(idx, monthly_freq: str = "MS") -> pd.DatetimeIndex:
    di = pd.DatetimeIndex(pd.to_datetime(idx, errors="coerce"))
    how: Literal["start", "end"] = "start" if str(monthly_freq).upper() == "MS" else "end"
    return di.to_period("M").to_timestamp(how=how)


def _safe_std(std: pd.Series) -> pd.Series:
    # Avoid divide-by-zero; clip tiny std to epsilon
    return std.where(std > 1e-12, 1e-12)


def _slice_contiguous_months(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    s = df.loc[start:end]
    if s.empty:
        raise ValueError(f"Empty slice for [{start.date()}..{end.date()}].")
    expected = (s.index[-1].to_period("M") - s.index[0].to_period("M")).n + 1
    if len(s) != expected:
        raise ValueError(
            f"Index not continuous monthly in [{start.date()}..{end.date()}]: rows={len(s)}, expected={expected}"
        )
    return s


def _rolling_window_bounds(cutoff: pd.Timestamp, window_months: int, monthly_freq: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    # window includes cutoff month and the previous (window_months-1) months
    cutoff = _month_index([cutoff], monthly_freq=monthly_freq)[0]
    start = (cutoff.to_period("M") - (window_months - 1)).to_timestamp(how="start")
    return start, cutoff


def iter_realtime_rolling_standardized(
    X_raw: pd.DataFrame,
    *,
    monthly_freq: str = "MS",
    first_cutoff: str | pd.Timestamp,
    last_cutoff: Optional[str | pd.Timestamp] = None,
    window_months: int = 180,              # e.g., 15 years
    min_obs_per_col: int = 24,
    return_full_hist_until_cutoff: bool = False,
) -> Iterator[Tuple[pd.Timestamp, pd.DataFrame, RollingStats]]:
    """
    Pseudo-real-time rolling standardization.

    For each cutoff month T in [first_cutoff, last_cutoff], compute per-column
    mean/std on the last `window_months` months ending at T (using observed values),
    then z-score either:
      - the *estimation window itself* (default), i.e., last `window_months` months (contiguous), or
      - the *entire history up to T* if `return_full_hist_until_cutoff=True`.

    Yields: (cutoff, Z, stats) where:
      - cutoff: Timestamp at month start/end (per `monthly_freq`)
      - Z: standardized DataFrame (index either the rolling window, or history up to T)
      - stats: RollingStats(mean, std, count, window_months, window_start, cutoff)
    """
    # Normalize index and keep only numeric
    X = X_raw.copy()
    X.index = _month_index(X.index, monthly_freq=monthly_freq)
    X = X.sort_index()
    X = X.select_dtypes(include="number")

    if last_cutoff is None:
        last_cutoff = X.index.max()

    # iterate over monthly cutoffs
    months = pd.period_range(first_cutoff, last_cutoff, freq="M").to_timestamp(how="start")
    for T in months:
        w_start, w_end = _rolling_window_bounds(T, window_months, monthly_freq)
        Xw = _slice_contiguous_months(X, w_start, w_end)

        # rolling stats on window
        mu = Xw.mean(skipna=True)
        sd = _safe_std(Xw.std(skipna=True, ddof=0))
        cnt = Xw.notna().sum()

        # (optional) warn if a column has too few obs in the window
        # weak = cnt[cnt < min_obs_per_col]

        stats = RollingStats(
            mean=mu, std=sd, count=cnt,
            window_months=int(window_months),
            window_start=w_start, cutoff=w_end
        )

        if return_full_hist_until_cutoff:
            Z = (X.loc[:T] - mu) / sd
        else:
            Z = (Xw - mu) / sd

        yield T, Z, stats
