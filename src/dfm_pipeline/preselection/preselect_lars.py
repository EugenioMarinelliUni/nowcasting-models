#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from dfm_pipeline.preselection.baseline_screening.selectors import lars_select


def _abs_corr_all(X: pd.DataFrame, y: pd.Series) -> pd.Series:
    """
    Compute absolute Pearson correlation of each column in X with y.
    Used to build a simple ranking table.
    """
    vals: Dict[str, float] = {}
    for c in X.columns:
        s = pd.concat([X[c], y], axis=1).dropna()
        if len(s) < 3:
            vals[c] = np.nan
        else:
            vals[c] = float(abs(s.iloc[:, 0].corr(s.iloc[:, 1])))
    return pd.Series(vals, dtype="float64").sort_values(ascending=False)


def run_lars_tscv_preselection(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    High-level LARS+TSCV wrapper used by scripts/preselection/run_preselection.py.
    """
    min_features = int(params.get("min_features", 10))
    max_features = int(params.get("max_features", 80))
    dedup_tau = float(params.get("dedup_tau", 0.97))
    cv = int(params.get("cv", 10))
    lars_one_se = bool(params.get("lars_one_se", True))

    selected_vars = lars_select(
        X_rank,
        y_rank,
        min_features=min_features,
        max_features=max_features,
        dedup_tau=dedup_tau,
        cv=cv,
        one_se=lars_one_se,
    )

    abs_corr = _abs_corr_all(X_rank, y_rank)
    rank_df = abs_corr.to_frame(name="abs_corr")
    rank_df["selected"] = rank_df.index.isin(selected_vars)
    rank_df = rank_df.sort_values("abs_corr", ascending=False)

    meta: Dict[str, Any] = {
        "method": "lars_tscv",
        "n_obs": int(len(X_rank)),
        "n_features_in": int(X_rank.shape[1]),
        "n_features_selected": int(len(selected_vars)),
        "hyperparams": {
            "min_features": min_features,
            "max_features": max_features,
            "dedup_tau": dedup_tau,
            "cv": cv,
            "lars_one_se": lars_one_se,
        },
    }

    return rank_df, selected_vars, meta
