from __future__ import annotations
import unicodedata
import pandas as pd

def clean_colname(name: str) -> str:
    s = unicodedata.normalize("NFKC", str(name)).strip()
    s = s.replace(" ", "_")
    return s

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with sanitized column names."""
    out = df.copy()
    out.columns = [clean_colname(c) for c in out.columns]
    return out
