from __future__ import annotations
from pathlib import Path
from typing import Tuple

import pandas as pd


def _ensure_monthly_datetime_index(df: pd.DataFrame, index_name: str = "date") -> pd.DataFrame:
    """
    Ensure DataFrame has a DatetimeIndex with monthly frequency-like entries.
    - If index is not datetime, try to parse it.
    - We do NOT force a freq attribute; we just rely on month-end or month-start stamps.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception as e:
            raise ValueError(f"Cannot parse {index_name} index to datetime: {e}")
    # Sort for safety
    df = df.sort_index()
    return df


def load_panel_csv(path: Path, index_name: str = "date") -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Panel not found: {path}")
    df = pd.read_csv(path, index_col=0)
    return _ensure_monthly_datetime_index(df, index_name=index_name)


def month_after(d: pd.Timestamp) -> pd.Timestamp:
    """Return the first timestamp of the month following `d` (safe for month-ends)."""
    y = d.year + (1 if d.month == 12 else 0)
    m = 1 if d.month == 12 else d.month + 1
    return pd.Timestamp(year=y, month=m, day=1)


def slice_train_oos(
    raw: pd.DataFrame,
    train_start: str,
    train_end: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Slice a raw panel into TRAIN [train_start, train_end], and OOS (train_end exclusive, …raw_end].
    OOS starts at the FIRST day of the next month after `train_end` (if available in data),
    and ends at the last timestamp present in the raw panel.
    """
    raw = _ensure_monthly_datetime_index(raw)
    # Train slice (inclusive)
    train = raw.loc[(raw.index >= pd.to_datetime(train_start)) & (raw.index <= pd.to_datetime(train_end))]
    if train.empty:
        raise ValueError(f"Empty training slice for [{train_start}, {train_end}]. Check dates and raw panel.")
    # OOS slice: from next month after train_end to end of raw
    oos_start = month_after(pd.to_datetime(train_end))
    oos = raw.loc[raw.index >= oos_start]
    return train, oos
