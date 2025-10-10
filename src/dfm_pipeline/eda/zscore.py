# src/dfm_pipeline/eda/zscore.py
from __future__ import annotations
from pathlib import Path
from typing import Optional, Tuple
import numpy as np
import pandas as pd

def zscore_panel(
    df: pd.DataFrame,
    train_start: str,
    train_end: str,
    *,
    ddof: int = 0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Z-score the panel using mean/std computed on [train_start, train_end].

    Returns
    -------
    Z : DataFrame
        Z-scored panel over the *full* sample (same index/columns as df).
    stats : DataFrame
        Per-series parameters used: columns ['mean','std'] computed on the training window.
        std==0 replaced with NaN to avoid division-by-zero.
    """
    train = df.loc[train_start:train_end]
    mu = train.mean()
    sd = train.std(ddof=ddof).replace(0, np.nan)
    Z = (df - mu) / sd
    stats = pd.DataFrame({"mean": mu, "std": sd})
    return Z, stats

def zscore_panel_from_csv(
    in_csv: Path,
    *,
    date_col: str = "sasdate",
    train_start: str,
    train_end: str,
    out_csv: Optional[Path] = None,
    stats_csv: Optional[Path] = None,
    date_fmt: str = "%m/%d/%Y",
    float_fmt: str = "%.10g",
    ddof: int = 0,
) -> Tuple[Path, Optional[Path]]:
    """
    Convenience: load CSV, z-score, and write outputs. Returns written paths.
    """
    df = pd.read_csv(in_csv, parse_dates=[date_col], index_col=date_col)
    Z, stats = zscore_panel(df, train_start, train_end, ddof=ddof)

    if out_csv is None:
        out_csv = in_csv.with_name(in_csv.stem + "_z.csv")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    Z.to_csv(out_csv, date_format=date_fmt, float_format=float_fmt)

    stats_path = None
    if stats_csv is not None:
        stats_csv.parent.mkdir(parents=True, exist_ok=True)
        stats.to_csv(stats_csv, float_format=float_fmt)
        stats_path = stats_csv

    return out_csv, stats_path
