# FILE: src/dfm_pipeline/preselection/alignment.py
#!/usr/bin/env python3
from __future__ import annotations

"""Robust X/y alignment helpers for preselection routines.

The legacy preselection selectors assume that the target is a one-dimensional
Series and that X/y already have exactly aligned indexes. In the QRF workflow
with monthly-aligned quarterly targets and optional quarterly aggregation, the
target can occasionally arrive as a one-column DataFrame or with an index anchor
that differs from the panel. These helpers coerce the target to a clean Series,
normalize date indexes to month-start timestamps, remove duplicate timestamps,
and return numeric aligned objects.
"""

from typing import Tuple

import numpy as np
import pandas as pd


def coerce_target_series(y: pd.Series | pd.DataFrame) -> pd.Series:
    """Return *y* as a numeric one-dimensional Series named ``y``.

    Accepted inputs are a Series or a one-column DataFrame. If a DataFrame has a
    column named ``y``, that column is preferred. Otherwise, if it contains a
    single numeric column, that column is used. Multi-column target frames are
    rejected because they make variable selection ambiguous.
    """
    if isinstance(y, pd.DataFrame):
        if "y" in y.columns:
            out = y["y"]
        elif y.shape[1] == 1:
            out = y.iloc[:, 0]
        else:
            numeric_cols = y.select_dtypes(include=[np.number]).columns.tolist()
            if len(numeric_cols) == 1:
                out = y[numeric_cols[0]]
            else:
                raise ValueError(
                    "Target y must be a Series or a one-column DataFrame. "
                    f"Got DataFrame with columns={list(y.columns)!r}."
                )
    elif isinstance(y, pd.Series):
        out = y
    else:
        raise TypeError(f"Target y must be a pandas Series/DataFrame, got {type(y)!r}.")

    out = pd.to_numeric(out, errors="coerce")
    out = out.copy()
    out.name = "y"
    return out


def normalize_month_index(obj: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
    """Normalize a Series/DataFrame index to sorted month-start timestamps."""
    out = obj.copy()

    if isinstance(out.index, pd.PeriodIndex):
        idx = out.index.to_timestamp(how="start")
    else:
        idx = pd.DatetimeIndex(pd.to_datetime(out.index, errors="coerce"))

    if idx.hasnans:
        raise ValueError("Index contains NaT values after datetime conversion.")

    out.index = idx.to_period("M").to_timestamp(how="start")
    out = out.sort_index()

    # Duplicates can appear after changing month-end anchors to month-start. Use
    # the last value, which is the conservative choice for already-sorted data.
    if out.index.has_duplicates:
        out = out.groupby(level=0).last()

    return out


def align_X_y_dropna(
    X: pd.DataFrame,
    y: pd.Series | pd.DataFrame,
    *,
    drop_constant_columns: bool = True,
    min_nonmissing: int = 3,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Return numeric, month-indexed, aligned ``(X, y)``.

    Only missing target rows are removed here. Missing predictor rows are kept so
    methods can decide whether to drop rows globally or per regressor. Columns
    that are entirely missing, non-numeric, or effectively constant are removed.
    """
    if not isinstance(X, pd.DataFrame):
        raise TypeError(f"X must be a pandas DataFrame, got {type(X)!r}.")

    Xc = normalize_month_index(X)
    yc = normalize_month_index(coerce_target_series(y))

    Xc = Xc.apply(pd.to_numeric, errors="coerce")
    yc = pd.to_numeric(yc, errors="coerce")
    yc.name = "y"

    common = Xc.index.intersection(yc.index)
    Xc = Xc.loc[common].sort_index()
    yc = yc.loc[common].sort_index()

    valid_y = yc.notna()
    Xc = Xc.loc[valid_y]
    yc = yc.loc[valid_y]

    Xc = Xc.dropna(axis=1, how="all")

    keep_cols: list[str] = []
    for col in Xc.columns:
        s = pd.to_numeric(Xc[col], errors="coerce")
        if int(s.notna().sum()) < int(min_nonmissing):
            continue
        if drop_constant_columns and float(s.std(skipna=True)) <= 1e-12:
            continue
        keep_cols.append(str(col))

    Xc = Xc.loc[:, keep_cols]
    return Xc, yc


def concat_y_X_dropna(y: pd.Series | pd.DataFrame, X: pd.DataFrame) -> pd.DataFrame:
    """Safely concatenate target and predictors, then drop rows with any NaNs."""
    Xc, yc = align_X_y_dropna(X, y)
    return pd.concat([yc.rename("y"), Xc], axis=1).dropna()
