from __future__ import annotations

import pandas as pd


def ensure_dataframe(row: dict) -> pd.DataFrame:
    return pd.DataFrame([row])