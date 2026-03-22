from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_panel_csv(path: str | Path, index_col: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if index_col is not None:
        df[index_col] = pd.to_datetime(df[index_col])
        df = df.set_index(index_col)
    else:
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
        df = df.set_index(df.columns[0])
    df = df.sort_index()
    return df


def load_target_csv(path: str | Path, index_col: str | None = None, value_col: str | None = None) -> pd.Series:
    df = load_panel_csv(path, index_col=index_col)
    if value_col is None:
        value_col = df.columns[0]
    return df[value_col].astype(float)