from __future__ import annotations

import pandas as pd


def apply_tcode_transformations(df: pd.DataFrame, tcode_map: dict | None = None) -> pd.DataFrame:
    return df.copy()


def standardize_with_frozen_scaler(
    df: pd.DataFrame,
    mean_: pd.Series,
    std_: pd.Series,
) -> pd.DataFrame:
    return (df - mean_) / std_.replace(0.0, 1.0)