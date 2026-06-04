from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

from qrf_pipeline.diagnostics import quantile_col_name


@dataclass(frozen=True)
class QuantileCalibration:
    """
    Additive quantile calibration fitted on validation residuals.

    For each quantile q:
        calibrated_q = raw_q + empirical_quantile(actual - raw_q, q)
    """

    additive_adjustments: dict[str, float]

    def as_dict(self) -> dict:
        return {"additive_adjustments": self.additive_adjustments}


def fit_additive_quantile_calibration(
    validation_predictions: pd.DataFrame,
    quantiles: tuple[float, ...],
    *,
    actual_col: str = "actual",
) -> QuantileCalibration:
    if actual_col not in validation_predictions.columns:
        raise ValueError(f"Missing actual column: {actual_col}")

    y = pd.to_numeric(validation_predictions[actual_col], errors="coerce").to_numpy(dtype=float)
    adjustments: dict[str, float] = {}

    for q in quantiles:
        col = quantile_col_name(q)
        if col not in validation_predictions.columns:
            continue

        pred = pd.to_numeric(validation_predictions[col], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(y) & np.isfinite(pred)

        if mask.sum() == 0:
            adjustments[str(float(q))] = 0.0
            continue

        residual = y[mask] - pred[mask]
        adjustments[str(float(q))] = float(np.quantile(residual, q))

    return QuantileCalibration(additive_adjustments=adjustments)


def apply_additive_quantile_calibration(
    pred_df: pd.DataFrame,
    calibration: QuantileCalibration,
) -> pd.DataFrame:
    out = pred_df.copy()

    for q_str, adj in calibration.additive_adjustments.items():
        q = float(q_str)
        col = quantile_col_name(q)
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") + float(adj)

    # Enforce monotonic quantile ordering row-wise for the standard QRF quantiles.
    q_cols = []
    for q_str in calibration.additive_adjustments:
        col = quantile_col_name(float(q_str))
        if col in out.columns:
            q_cols.append((float(q_str), col))

    q_cols = [col for _, col in sorted(q_cols)]
    if q_cols:
        vals = out[q_cols].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        vals_sorted = np.sort(vals, axis=1)
        out.loc[:, q_cols] = vals_sorted

    if "pred_q50" in out.columns:
        out["pred"] = out["pred_q50"]

    return out


def save_calibration(calibration: QuantileCalibration, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(calibration.as_dict(), f, indent=2)
    return path


def load_calibration(path: str | Path) -> QuantileCalibration:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    return QuantileCalibration(
        additive_adjustments={str(k): float(v) for k, v in obj.get("additive_adjustments", {}).items()}
    )
