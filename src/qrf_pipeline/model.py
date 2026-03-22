from __future__ import annotations

from dataclasses import dataclass
from sklearn.ensemble import RandomForestRegressor


@dataclass
class QRFPointModel:
    estimator: RandomForestRegressor
    feature_columns: list[str]