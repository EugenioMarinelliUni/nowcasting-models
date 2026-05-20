from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sklearn.ensemble import RandomForestRegressor


@dataclass
class QRFPointModel:
    estimator: RandomForestRegressor
    feature_columns: list[str]


@dataclass
class QRFQuantileModel:
    estimator: Any
    feature_columns: list[str]
    quantiles: tuple[float, ...]