from __future__ import annotations

import numpy as np
import pandas as pd


def quantile_col_name(q: float) -> str:
    return f"pred_q{int(round(q * 100)):02d}"


def pinball_loss(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    quantile: float,
) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    err = y_true - y_pred
    loss = np.maximum(quantile * err, (quantile - 1.0) * err)
    return float(np.mean(loss))


def compute_quantile_metrics(
    pred_df: pd.DataFrame,
    *,
    actual_col: str = "actual",
    quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90),
) -> dict:
    df = pred_df.copy()

    if actual_col not in df.columns:
        raise ValueError(f"Missing actual column: {actual_col}")

    df = df.dropna(subset=[actual_col])
    if df.empty:
        return {
            "n": 0,
            "pinball": {},
            "mean_pinball": None,
            "coverage_50": None,
            "avg_width_50": None,
            "coverage_80": None,
            "avg_width_80": None,
        }

    pinball: dict[str, float | None] = {}
    finite_losses: list[float] = []

    for q in quantiles:
        col = quantile_col_name(q)
        if col not in df.columns:
            pinball[str(q)] = None
            continue

        g = df.dropna(subset=[col])
        if g.empty:
            pinball[str(q)] = None
            continue

        val = pinball_loss(
            g[actual_col].to_numpy(dtype=float),
            g[col].to_numpy(dtype=float),
            q,
        )
        pinball[str(q)] = val
        finite_losses.append(val)

    out: dict = {
        "n": int(len(df)),
        "pinball": pinball,
        "mean_pinball": float(np.mean(finite_losses)) if finite_losses else None,
        "coverage_50": None,
        "avg_width_50": None,
        "coverage_80": None,
        "avg_width_80": None,
    }

    if "pred_q25" in df.columns and "pred_q75" in df.columns:
        g = df.dropna(subset=["pred_q25", "pred_q75"])
        if not g.empty:
            y = g[actual_col].to_numpy(dtype=float)
            lo = g["pred_q25"].to_numpy(dtype=float)
            hi = g["pred_q75"].to_numpy(dtype=float)
            out["coverage_50"] = float(np.mean((y >= lo) & (y <= hi)))
            out["avg_width_50"] = float(np.mean(hi - lo))

    if "pred_q10" in df.columns and "pred_q90" in df.columns:
        g = df.dropna(subset=["pred_q10", "pred_q90"])
        if not g.empty:
            y = g[actual_col].to_numpy(dtype=float)
            lo = g["pred_q10"].to_numpy(dtype=float)
            hi = g["pred_q90"].to_numpy(dtype=float)
            out["coverage_80"] = float(np.mean((y >= lo) & (y <= hi)))
            out["avg_width_80"] = float(np.mean(hi - lo))

    return out