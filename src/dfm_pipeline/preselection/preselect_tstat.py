#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from src.dfm_pipeline.preselection.baseline_screening.selectors import tstat_select


def _abs_corr_all(X: pd.DataFrame, y: pd.Series) -> pd.Series:
    """
    Compute absolute Pearson correlation of each column in X with y.
    Used for a simple, unified ranking view across methods.
    """
    vals: Dict[str, float] = {}
    for c in X.columns:
        s = pd.concat([X[c], y], axis=1).dropna()
        if len(s) < 3:
            vals[c] = np.nan
        else:
            vals[c] = float(abs(s.iloc[:, 0].corr(s.iloc[:, 1])))
    return pd.Series(vals, dtype="float64").sort_values(ascending=False)


def run_tstat_preselection(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    High-level t-stat wrapper used by scripts/preselection/run_preselection.py.

    Parameters
    ----------
    X_rank : DataFrame
        Aggregated (typically quarterly) predictor matrix, index aligned with y_rank.
        NOTE: X_rank is *already* aggregated and aligned; we do not call
        _aggregate_monthly_to_quarterly_matrix here.
    y_rank : Series
        Target series on the same index.
    params : dict
        Hyperparameters for t-stat, expected keys:
          - min_features   : int
          - max_features   : int
          - dedup_tau      : float
          - tstat_alpha    : float (optional, default 0.05)
          - tstat_topn     : int   (optional, default 0)
          - hac_lags       : "auto" or int (optional, default "auto")
          - tstat_ar_lags  : int (optional, default 0)
          - x_adl_lags     : int (optional, default 0)
          - adl_score      : str "fstat" or "max_t" (optional, default "fstat")

    Returns
    -------
    rank_df : DataFrame
        Index = variable names, columns:
          - abs_corr  : |corr(X_j, y)| (simple ranking diagnostic)
          - selected  : bool, whether variable was selected by t-stat criteria
    selected_vars : list[str]
        List of selected variable names.
    meta : dict
        Metadata dictionary (hyperparameters, counts, etc.).
    """
    min_features = int(params.get("min_features", 10))
    max_features = int(params.get("max_features", 80))
    dedup_tau = float(params.get("dedup_tau", 0.97))
    tstat_alpha = float(params.get("tstat_alpha", 0.05))
    tstat_topn = int(params.get("tstat_topn", 0))
    hac_lags = params.get("hac_lags", "auto")
    tstat_ar_lags = int(params.get("tstat_ar_lags", 0))
    x_adl_lags = int(params.get("x_adl_lags", 0))
    adl_score = str(params.get("adl_score", "fstat"))

    # IMPORTANT: we pass agg_rule_map=None, agg_rule_default=None because
    # aggregation has already been applied in run_preselection.py.
    selected_vars = tstat_select(
        X_rank,
        y_rank,
        min_features=min_features,
        max_features=max_features,
        dedup_tau=dedup_tau,
        tstat_alpha=tstat_alpha,
        tstat_topn=tstat_topn,
        hac_lags=hac_lags,
        ar_lags=tstat_ar_lags,
        agg_rule_map=None,
        agg_rule_default=None,
        x_adl_lags=x_adl_lags,
        adl_score=adl_score,
    )

    abs_corr = _abs_corr_all(X_rank, y_rank)
    rank_df = abs_corr.to_frame(name="abs_corr")
    rank_df["selected"] = rank_df.index.isin(selected_vars)
    rank_df = rank_df.sort_values("abs_corr", ascending=False)

    meta: Dict[str, Any] = {
        "method": "tstat",
        "n_obs": int(len(X_rank)),
        "n_features_in": int(X_rank.shape[1]),
        "n_features_selected": int(len(selected_vars)),
        "hyperparams": {
            "min_features": min_features,
            "max_features": max_features,
            "dedup_tau": dedup_tau,
            "tstat_alpha": tstat_alpha,
            "tstat_topn": tstat_topn,
            "hac_lags": hac_lags,
            "tstat_ar_lags": tstat_ar_lags,
            "x_adl_lags": x_adl_lags,
            "adl_score": adl_score,
        },
    }

    return rank_df, selected_vars, meta
