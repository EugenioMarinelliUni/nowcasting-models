from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import pandas as pd


@dataclass(frozen=True)
class TargetScaler:
    mean: float
    std: float

    def inverse(self, z: float) -> float:
        return float(z) * self.std + self.mean


def load_target_scaler(path: str | Path) -> TargetScaler:
    """
    Load a target scaler from a small JSON file.

    Accepted keys:
    - mean, mu, target_mean, y_mean
    - std, sigma, target_std, y_std
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

    if std <= 0:
        raise ValueError("Target scaler std must be positive")

    return TargetScaler(mean=mean, std=std)


def inverse_transform_prediction_df(pred_df: pd.DataFrame, scaler: TargetScaler) -> pd.DataFrame:
    out = pred_df.copy()

    base_cols = ["pred", "actual"] + [c for c in out.columns if c.startswith("pred_q")]
    for col in base_cols:
        if col in out.columns:
            out[f"{col}_raw"] = pd.to_numeric(out[col], errors="coerce") * scaler.std + scaler.mean

    if "pred_raw" not in out.columns and "pred" in out.columns:
        out["pred_raw"] = out["pred_raw"]
    if "actual_raw" not in out.columns and "actual" in out.columns:
        out["actual_raw"] = out["actual_raw"]

    return out
