from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


def _finite_mask(*arrays: np.ndarray) -> np.ndarray:
    if not arrays:
        raise ValueError("At least one array is required")
    mask = np.ones(len(arrays[0]), dtype=bool)
    for arr in arrays:
        mask &= np.isfinite(arr)
    return mask


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, q: float) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = _finite_mask(y_true, y_pred)
    if mask.sum() == 0:
        return float("nan")
    err = y_true[mask] - y_pred[mask]
    loss = np.maximum(q * err, (q - 1.0) * err)
    return float(np.mean(loss))


def interval_coverage(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    lower = np.asarray(lower, dtype=float)
    upper = np.asarray(upper, dtype=float)
    mask = _finite_mask(y_true, lower, upper)
    if mask.sum() == 0:
        return float("nan")
    return float(np.mean((y_true[mask] >= lower[mask]) & (y_true[mask] <= upper[mask])))


def interval_width(lower: np.ndarray, upper: np.ndarray) -> float:
    lower = np.asarray(lower, dtype=float)
    upper = np.asarray(upper, dtype=float)
    mask = _finite_mask(lower, upper)
    if mask.sum() == 0:
        return float("nan")
    return float(np.mean(upper[mask] - lower[mask]))


def winkler_score(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray, alpha: float) -> float:
    """
    Winkler interval score for a central 1-alpha prediction interval.
    Lower is better. It rewards narrow intervals and penalizes misses.
    """
    y_true = np.asarray(y_true, dtype=float)
    lower = np.asarray(lower, dtype=float)
    upper = np.asarray(upper, dtype=float)
    mask = _finite_mask(y_true, lower, upper)
    if mask.sum() == 0:
        return float("nan")

    y = y_true[mask]
    lo = lower[mask]
    hi = upper[mask]

    score = hi - lo
    score += (2.0 / alpha) * (lo - y) * (y < lo)
    score += (2.0 / alpha) * (y - hi) * (y > hi)
    return float(np.mean(score))


def approximate_crps_from_quantiles(y_true: np.ndarray, quantile_preds: dict[float, np.ndarray]) -> float:
    """
    Approximate CRPS with averaged quantile scores:
        CRPS ~= 2 * mean_q pinball_q.
    This is a practical discrete-quantile approximation for QRF outputs.
    """
    vals = [
        pinball_loss(y_true, pred, q)
        for q, pred in sorted(quantile_preds.items())
    ]
    vals = [v for v in vals if np.isfinite(v)]
    return float(2.0 * np.mean(vals)) if vals else float("nan")


def quantile_col_name(q: float) -> str:
    return f"pred_q{int(round(q * 100))}"


def compute_qrf_density_diagnostics(
    pred_df: pd.DataFrame,
    quantiles: Iterable[float],
    *,
    actual_col: str = "actual",
    prefix: str = "",
) -> dict:
    """
    Compute QRF density diagnostics:
    - pinball loss per quantile
    - mean pinball
    - approximate CRPS
    - 50% and 80% coverage
    - interval widths
    - Winkler scores
    - absolute coverage errors
    """
    if actual_col not in pred_df.columns:
        raise ValueError(f"Missing actual column: {actual_col}")

    y = pd.to_numeric(pred_df[actual_col], errors="coerce").to_numpy(dtype=float)

    qpreds: dict[float, np.ndarray] = {}
    pinball: dict[str, float] = {}

    for q in quantiles:
        col = f"{prefix}{quantile_col_name(float(q))}"
        if col not in pred_df.columns:
            continue
        arr = pd.to_numeric(pred_df[col], errors="coerce").to_numpy(dtype=float)
        qpreds[float(q)] = arr
        pinball[str(float(q))] = pinball_loss(y, arr, float(q))

    finite_pinball = [v for v in pinball.values() if np.isfinite(v)]
    out: dict = {
        "n": int(np.isfinite(y).sum()),
        "pinball": pinball,
        "mean_pinball": float(np.mean(finite_pinball)) if finite_pinball else float("nan"),
        "crps_approx": approximate_crps_from_quantiles(y, qpreds),
    }

    def _add_interval(q_low: float, q_high: float, nominal: float, label: str) -> None:
        if q_low not in qpreds or q_high not in qpreds:
            out[f"coverage_{label}"] = float("nan")
            out[f"avg_width_{label}"] = float("nan")
            out[f"winkler_{label}"] = float("nan")
            out[f"coverage_error_{label}"] = float("nan")
            return

        lo = qpreds[q_low]
        hi = qpreds[q_high]
        cov = interval_coverage(y, lo, hi)
        out[f"coverage_{label}"] = cov
        out[f"avg_width_{label}"] = interval_width(lo, hi)
        out[f"winkler_{label}"] = winkler_score(y, lo, hi, alpha=1.0 - nominal)
        out[f"coverage_error_{label}"] = abs(cov - nominal) if np.isfinite(cov) else float("nan")

    _add_interval(0.25, 0.75, 0.50, "50")
    _add_interval(0.10, 0.90, 0.80, "80")

    return out


def summarize_by_month_of_quarter(
    pred_df: pd.DataFrame,
    quantiles: Iterable[float] = (0.10, 0.25, 0.50, 0.75, 0.90),
) -> pd.DataFrame:
    df = pred_df.copy()
    df["err"] = pd.to_numeric(df["pred"], errors="coerce") - pd.to_numeric(df["actual"], errors="coerce")
    df["abs_err"] = df["err"].abs()
    df["sq_err"] = df["err"] ** 2

    rows = []
    for moq, g in df.groupby("moq", dropna=False):
        row = {
            "moq": int(moq) if pd.notna(moq) else None,
            "n": int(len(g)),
            "rmse": float(np.sqrt(g["sq_err"].mean())),
            "mae": float(g["abs_err"].mean()),
        }
        row.update(compute_qrf_density_diagnostics(g, quantiles))
        rows.append(row)

    return pd.DataFrame(rows).sort_values("moq").reset_index(drop=True)


def summarize_by_target_quarter(pred_df: pd.DataFrame) -> pd.DataFrame:
    df = pred_df.copy()
    df["err"] = pd.to_numeric(df["pred"], errors="coerce") - pd.to_numeric(df["actual"], errors="coerce")
    df["abs_err"] = df["err"].abs()
    df["sq_err"] = df["err"] ** 2

    return (
        df.groupby("target_date")
        .agg(
            n=("actual", "size"),
            rmse=("sq_err", lambda s: float(np.sqrt(s.mean()))),
            mae=("abs_err", "mean"),
            mean_pred=("pred", "mean"),
            actual=("actual", "first"),
        )
        .reset_index()
    )
