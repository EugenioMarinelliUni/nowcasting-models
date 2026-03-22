from __future__ import annotations

import numpy as np
import pandas as pd


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def compute_basic_metrics(pred_df: pd.DataFrame) -> dict:
    req = {"actual", "pred"}
    missing = req.difference(pred_df.columns)
    if missing:
        raise ValueError(f"pred_df missing columns: {sorted(missing)}")

    df = pred_df.dropna(subset=["actual", "pred"]).copy()
    if df.empty:
        return {"n": 0, "rmse": None, "mae": None}

    y_true = df["actual"].to_numpy(dtype=float)
    y_pred = df["pred"].to_numpy(dtype=float)

    out = {
        "n": int(len(df)),
        "rmse": rmse(y_true, y_pred),
        "mae": mae(y_true, y_pred),
    }

    if "horizon" in df.columns:
        out["by_horizon"] = {}
        for h, g in df.groupby("horizon"):
            yy = g["actual"].to_numpy(dtype=float)
            pp = g["pred"].to_numpy(dtype=float)
            out["by_horizon"][str(h)] = {
                "n": int(len(g)),
                "rmse": rmse(yy, pp),
                "mae": mae(yy, pp),
            }

    return out