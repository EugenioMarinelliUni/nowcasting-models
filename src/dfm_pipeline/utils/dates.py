from __future__ import annotations
import pandas as pd

def ensure_monthly_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reindex to a contiguous Month-Start (MS) DatetimeIndex (no gaps), preserving index name.
    """
    idx = pd.DatetimeIndex(df.index).sort_values()
    full = pd.date_range(idx.min(), idx.max(), freq="MS")
    out = df.reindex(full)
    out.index.name = df.index.name or "sasdate"
    return out

