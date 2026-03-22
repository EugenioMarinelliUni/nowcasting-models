from __future__ import annotations

from typing import Protocol
import pandas as pd


class PointNowcastModel(Protocol):
    def fit(self, X: pd.DataFrame, y: pd.Series) -> "PointNowcastModel":
        ...

    def predict(self, X: pd.DataFrame) -> pd.Series:
        ...