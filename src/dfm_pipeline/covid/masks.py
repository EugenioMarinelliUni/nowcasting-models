# src/dfm_pipeline/covid/masks.py
from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd

from .spec import _month_boundary


def window_mask(index: pd.DatetimeIndex, start: str, end: str, *, monthly_freq: str = "MS") -> pd.Series:
    idx = pd.DatetimeIndex(index)
    s = _month_boundary(start, monthly_freq)
    e = _month_boundary(end, monthly_freq)
    if s > e:
        raise ValueError(f"start {s.date()} > end {e.date()}")
    m = (idx >= s) & (idx <= e)
    return pd.Series(m, index=idx, name="mask")


def optional_window_mask(
    index: pd.DatetimeIndex, window: Optional[Tuple[str, str]], *, monthly_freq: str = "MS"
) -> pd.Series:
    if window is None:
        return pd.Series(np.ones(len(index), dtype=bool), index=index, name="mask")
    return window_mask(index, window[0], window[1], monthly_freq=monthly_freq)


def broadcast_row_mask(row_mask: pd.Series, columns: pd.Index) -> pd.DataFrame:
    arr = np.repeat(row_mask.to_numpy(dtype=bool)[:, None], len(columns), axis=1)
    return pd.DataFrame(arr, index=row_mask.index, columns=list(columns))
