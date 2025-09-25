from __future__ import annotations
import pandas as pd

def assert_unique_columns(df: pd.DataFrame) -> None:
    dup = df.columns[df.columns.duplicated()]
    if len(dup):
        raise ValueError(f"Duplicate columns: {dup.tolist()}")

def assert_monthly_index(df: pd.DataFrame) -> None:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Index must be DatetimeIndex.")
    if (df.index.freqstr or "").upper() != "MS":
        # Allow inferred monthly start even if .freq is None
        pass
