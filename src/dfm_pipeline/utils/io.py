from __future__ import annotations
from pathlib import Path
import pandas as pd

DATE_COL = "sasdate"

def load_panel_csv(p: Path, date_fmt: str = "%m/%d/%Y") -> pd.DataFrame:
    """
    Load a panel CSV where 'sasdate' might be an index or a column.
    Return a DataFrame with a DatetimeIndex named 'sasdate', sorted.
    """
    p = Path(p)
    try:
        df = pd.read_csv(p, parse_dates=[DATE_COL], index_col=DATE_COL)
    except Exception:
        df = pd.read_csv(p)
        if DATE_COL not in df.columns:
            raise ValueError(f"{p}: '{DATE_COL}' not found as index or column.")
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], format=date_fmt, errors="coerce")
        df = df.set_index(DATE_COL)
    df = df.sort_index()
    df.index.name = DATE_COL
    return df

def write_panel_csv(
    df: pd.DataFrame,
    path: Path,
    date_fmt: str = "%m/%d/%Y",
    float_fmt: str = "%.10g",
    index_label: str = DATE_COL,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df.index.name = index_label
    df.to_csv(path, index_label=index_label, date_format=date_fmt, float_format=float_fmt)

