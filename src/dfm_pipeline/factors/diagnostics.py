from __future__ import annotations
import pandas as pd

def describe_ic_table(table: pd.DataFrame) -> str:
    best = {c: int(table.loc[table[c].idxmin(), "k"]) for c in ["ICp1","ICp2","ICp3"]}
    return f"Best k by ICs: {best}"
