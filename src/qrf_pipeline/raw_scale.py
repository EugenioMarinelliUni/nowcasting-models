from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import json

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TargetScaler:
    mean: float
    std: float

    def __post_init__(self) -> None:
        if not np.isfinite(self.mean):
            raise ValueError("Target scaler mean must be finite")
        if not np.isfinite(self.std) or self.std <= 0:
            raise ValueError("Target scaler std must be positive and finite")

    def inverse(self, z: float) -> float:
        if not np.isfinite(z):
            return float("nan")
        return float(z) * self.std + self.mean

    def inverse_series(self, values: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(values, errors="coerce")
        return numeric * self.std + self.mean


def load_target_scaler(path: str | Path) -> TargetScaler:
    """Load a target scaler from a JSON file.

    Accepted mean keys: mean, mu, target_mean, y_mean.
    Accepted std keys: std, sigma, target_std, y_std.
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    mean = None
    for key in ("mean", "mu", "target_mean", "y_mean"):
        if key in obj:
            mean = float(obj[key])
            break

    std = None
    for key in ("std", "sigma", "target_std", "y_std"):
        if key in obj:
            std = float(obj[key])
            break

    if mean is None or std is None:
        raise KeyError(
            "Scaler JSON must contain a mean key and a std key. "
            "Accepted mean keys: mean, mu, target_mean, y_mean. "
            "Accepted std keys: std, sigma, target_std, y_std."
        )

    return TargetScaler(mean=mean, std=std)


def _quantile_columns(columns: Iterable[str]) -> list[str]:
    out: list[str] = []
    for col in columns:
        name = str(col)
        if name.startswith("pred_q") and not name.endswith("_raw"):
            out.append(name)
    return out


def inverse_transform_prediction_df(pred_df: pd.DataFrame, scaler: TargetScaler) -> pd.DataFrame:
    """Append raw-scale columns to a standardized QRF prediction DataFrame.

    The function is intentionally explicit: standardized columns are retained,
    and raw-scale columns are written with a `_raw` suffix. Existing `_raw`
    columns are overwritten so that the result is consistent with the supplied
    scaler.

    Transformed columns:
    - pred -> pred_raw
    - actual -> actual_raw
    - pred_qXX -> pred_qXX_raw for all quantile columns present
    """
    out = pred_df.copy()

    for col in ["pred", "actual", *_quantile_columns(out.columns)]:
        if col in out.columns:
            out[f"{col}_raw"] = scaler.inverse_series(out[col])

    return out
