from __future__ import annotations

import numpy as np


def _finite_records(records: list[dict]) -> list[dict]:
    out = []
    for record in records:
        pred = float(record.get("pred", np.nan))
        if np.isfinite(pred):
            copied = dict(record)
            copied["pred"] = pred
            out.append(copied)
    return out


def _score_for_selection(record: dict) -> float:
    validation_rmse = record.get("validation_rmse", np.nan)
    if np.isfinite(float(validation_rmse)):
        return float(validation_rmse)

    in_sample_rmse = record.get("in_sample_rmse", np.nan)
    if np.isfinite(float(in_sample_rmse)):
        return float(in_sample_rmse)

    return float("inf")


def combine_midas_forecasts(
    records: list[dict],
    *,
    method: str,
    trimmed_alpha: float = 0.10,
    top_k: int | None = None,
) -> tuple[float, list[dict]]:
    clean = _finite_records(records)

    if not clean:
        return float("nan"), []

    method = method.lower()

    if top_k is not None and top_k > 0:
        clean = sorted(clean, key=_score_for_selection)[:top_k]

    if method == "top_k":
        if top_k is None or top_k <= 0:
            raise ValueError("top_k combination requires top_k > 0")
        clean = sorted(clean, key=_score_for_selection)[:top_k]
        preds = np.array([float(r["pred"]) for r in clean], dtype=float)
        weight = 1.0 / len(clean)
        selected = []
        for rank, record in enumerate(clean, start=1):
            copied = dict(record)
            copied["selection_rank"] = rank
            copied["combination_weight"] = weight
            selected.append(copied)
        return float(np.mean(preds)), selected

    preds = np.array([float(r["pred"]) for r in clean], dtype=float)

    if method == "mean":
        weight = 1.0 / len(clean)
        selected = []
        for record in clean:
            copied = dict(record)
            copied["selection_rank"] = None
            copied["combination_weight"] = weight
            selected.append(copied)
        return float(np.mean(preds)), selected

    if method == "median":
        selected = []
        for record in clean:
            copied = dict(record)
            copied["selection_rank"] = None
            copied["combination_weight"] = np.nan
            selected.append(copied)
        return float(np.median(preds)), selected

    if method == "trimmed_mean":
        if not (0.0 <= trimmed_alpha < 0.5):
            raise ValueError("trimmed_alpha must be in [0, 0.5)")

        order = np.argsort(preds)
        k = int(np.floor(trimmed_alpha * len(order)))

        if 2 * k >= len(order):
            keep_idx = order
        else:
            keep_idx = order[k : len(order) - k]

        kept = [clean[i] for i in keep_idx]
        kept_preds = np.array([float(r["pred"]) for r in kept], dtype=float)
        weight = 1.0 / len(kept)

        selected = []
        for record in kept:
            copied = dict(record)
            copied["selection_rank"] = None
            copied["combination_weight"] = weight
            selected.append(copied)

        return float(np.mean(kept_preds)), selected

    if method == "inverse_rmse":
        scores = np.array([_score_for_selection(r) for r in clean], dtype=float)
        preds = np.array([float(r["pred"]) for r in clean], dtype=float)

        scores = np.where(np.isfinite(scores), scores, np.inf)
        weights = 1.0 / np.maximum(scores, 1e-8)

        if not np.isfinite(weights).all() or weights.sum() <= 0:
            weight = 1.0 / len(clean)
            selected = []
            for record in clean:
                copied = dict(record)
                copied["selection_rank"] = None
                copied["combination_weight"] = weight
                selected.append(copied)
            return float(np.mean(preds)), selected

        weights = weights / weights.sum()

        selected = []
        for record, weight in zip(clean, weights):
            copied = dict(record)
            copied["selection_rank"] = None
            copied["combination_weight"] = float(weight)
            selected.append(copied)

        return float(np.sum(weights * preds)), selected

    raise ValueError(f"Unknown MIDAS combination method: {method}")