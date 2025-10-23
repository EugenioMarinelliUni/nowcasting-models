from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable, Tuple, List

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TargetStdStats:
    mean: float
    std: float
    start: pd.Timestamp
    end: pd.Timestamp
    nobs: int


# -----------------------------
# I/O helpers
# -----------------------------

def read_quarterly_target(
    raw_csv: Path,
    *,
    date_col: str = "sasdate",
    value_col: str = "gdp_qoq_saar",
) -> pd.Series:
    """
    Load a quarterly target (e.g., GDP QoQ SAAR) from CSV with known columns.
    Returns a Series indexed by DatetimeIndex (quarter timestamps), sorted.
    """
    df = pd.read_csv(raw_csv)
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in {raw_csv} (have: {list(df.columns)[:8]} ...)")
    if value_col not in df.columns:
        raise ValueError(f"Value column '{value_col}' not found in {raw_csv} (have: {list(df.columns)[:8]} ...)")
    dates = pd.to_datetime(df[date_col], errors="coerce")
    vals = pd.to_numeric(df[value_col], errors="coerce")
    yq = pd.Series(vals.values, index=dates).dropna().sort_index()
    yq.name = value_col
    return yq


def build_monthly_index_from_panel(
    panel_csv: Path,
    *,
    date_col: str = "Date",
    monthly_freq: str = "MS",
) -> pd.DatetimeIndex:
    """
    Build the canonical monthly DatetimeIndex from a panel CSV to ensure alignment.
    """
    df = pd.read_csv(panel_csv)
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not in {panel_csv}")
    di = pd.to_datetime(df[date_col], errors="coerce")
    how = "start" if str(monthly_freq).upper() == "MS" else "end"
    idx = pd.DatetimeIndex(pd.PeriodIndex(di, freq="M").to_timestamp(how=how))
    # ensure unique & sorted
    return pd.DatetimeIndex(pd.unique(idx)).sort_values()


# -----------------------------
# Dating & standardization
# -----------------------------

def quarterly_to_monthly(
    yq: pd.Series,
    *,
    monthly_freq: str = "MS",
    place: str = "start",  # "start" (Jan/Apr/Jul/Oct) or "end" (Mar/Jun/Sep/Dec)
) -> pd.Series:
    """
    Map each quarterly observation to a single monthly timestamp.
      - place='start' -> first month of quarter (period-start dating; matches FRED)
      - place='end'   -> last month of quarter
    Non-quarter months remain NaN when reindexed to the full monthly grid later.
    """
    per_q = pd.PeriodIndex(pd.DatetimeIndex(yq.index), freq="Q")
    if place == "start":
        q_ts = per_q.start_time
    else:
        q_ts = per_q.end_time
    per_m = pd.PeriodIndex(q_ts, freq="M")
    monthly_is_ms = str(monthly_freq).upper() == "MS"
    m_ts = per_m.start_time if monthly_is_ms else per_m.end_time
    ym = pd.Series(yq.values, index=m_ts, name=yq.name).sort_index()
    # Rename to standard 'y' label downstream
    ym.name = "y"
    return ym


def standardize_target_on_window(
    ym: pd.Series,
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
) -> tuple[pd.Series, TargetStdStats]:
    """
    Compute μ,σ on the training slice (non-NaN entries only) and z-score.
    Keeps NaNs (non-quarter months) outside observed months.
    """
    ym = pd.Series(pd.to_numeric(ym, errors="coerce"), index=pd.DatetimeIndex(ym.index)).sort_index()
    tr = ym.loc[pd.to_datetime(start):pd.to_datetime(end)].dropna()
    if tr.empty:
        raise ValueError("Training window for target has no non-NaN observations.")
    mu = float(tr.mean())
    sd0 = float(tr.std(ddof=0))
    sd = sd0 if (np.isfinite(sd0) and sd0 > 0) else 1e-12
    yz = (ym - mu) / sd
    yz.name = "y"
    stats = TargetStdStats(mean=mu, std=sd, start=pd.to_datetime(start), end=pd.to_datetime(end), nobs=int(tr.size))
    return yz, stats
