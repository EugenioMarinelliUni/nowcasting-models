#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Literal, cast

import pandas as pd


# ---------------------------------------------------------------------------
# Config dataclass used by the runner
# ---------------------------------------------------------------------------


@dataclass
class SelectionConfig:
    """
    Configuration describing where the per-method fractions live in the
    comparison CSV.

    frac_cols: mapping method_name -> column name, e.g.
        {
            "sis":   "frac_selected_sis",
            "tstat": "frac_selected_tstat",
            "lars":  "frac_selected_lars",
        }

    group_col: optional name of the column that contains the group for each
    variable (e.g. "Prices", "Output & Income").
    """

    frac_cols: Dict[str, str]
    group_col: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_comparison(comparison_csv: Path, cfg: SelectionConfig) -> pd.DataFrame:
    df = pd.read_csv(comparison_csv)

    if "variable" not in df.columns:
        raise ValueError(f"{comparison_csv} is missing required column 'variable'.")

    # Ensure all frac columns exist and are numeric; missing -> 0.0
    for m, col in cfg.frac_cols.items():
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Group column optional
    if cfg.group_col is not None and cfg.group_col not in df.columns:
        df[cfg.group_col] = "unknown"

    return df


def _normalize_weights(methods: list[str], weights: Dict[str, float]) -> Dict[str, float]:
    w_used = {m: float(weights.get(m, 0.0)) for m in methods}
    s = sum(v for v in w_used.values() if v > 0)
    if s <= 0:
        # fallback to equal weights
        return {m: 1.0 / len(methods) for m in methods}
    return {m: v / s for m, v in w_used.items()}


# ---------------------------------------------------------------------------
# Strategy A: vote rule (per-method thresholds + min_methods_selected)
# ---------------------------------------------------------------------------


def _run_method_A(
    df: pd.DataFrame,
    cfg: SelectionConfig,
    per_method_threshold: Dict[str, float],
    min_methods_selected: int,
) -> pd.DataFrame:
    """
    Method A: vote rule.

    For each variable and method m:
      vote_m = frac_m >= threshold_m  (if threshold_m exists and >0, else 0)

    Count votes across methods; keep variable if count >= min_methods_selected.

    score_A = simple average of all frac_m (over methods present).
    """
    methods = list(cfg.frac_cols.keys())

    # Build vote matrix
    votes = {}
    for m in methods:
        col = cfg.frac_cols[m]
        thr = float(per_method_threshold.get(m, 0.0))
        if thr > 0.0:
            votes[m] = (df[col] >= thr).astype(int)
        else:
            # if no threshold, method does not contribute votes
            votes[m] = pd.Series(0, index=df.index)

    vote_df = pd.DataFrame(votes)
    df["A_votes"] = vote_df.sum(axis=1)

    # selection mask
    sel_mask = df["A_votes"] >= int(min_methods_selected)
    df_A = df[sel_mask].copy()

    if df_A.empty:
        df["score_A"] = 0.0
        df["in_A"] = False
        return df

    # score_A: mean of frac across methods
    frac_cols = [cfg.frac_cols[m] for m in methods]
    df_A["score_A"] = df_A[frac_cols].mean(axis=1)

    # propagate back
    df["score_A"] = 0.0
    df.loc[df_A.index, "score_A"] = df_A["score_A"]
    df["in_A"] = False
    df.loc[df_A.index, "in_A"] = True

    return df


# ---------------------------------------------------------------------------
# Strategy B: stability score (weighted frac; top_k_B)
# ---------------------------------------------------------------------------


def _run_method_B(
    df: pd.DataFrame,
    cfg: SelectionConfig,
    stability_weights: Dict[str, float],
    top_k_B: int,
) -> pd.DataFrame:
    """
    Method B: stability score.

    score_B = sum_m w_m * frac_m, with weights normalized over methods that
    appear in cfg.frac_cols.
    """
    methods = list(cfg.frac_cols.keys())
    w = _normalize_weights(methods, stability_weights)

    score = 0.0
    for m in methods:
        col = cfg.frac_cols[m]
        score = score + w[m] * df[col]

    df["score_B"] = score

    # sort by descending score_B
    df_sorted = df.sort_values(["score_B", "variable"], ascending=[False, True])

    if top_k_B and top_k_B > 0:
        keep_idx = df_sorted.index[: int(top_k_B)]
    else:
        keep_idx = df_sorted.index

    df["in_B"] = False
    df.loc[keep_idx, "in_B"] = True
    return df


# ---------------------------------------------------------------------------
# Strategy C: rank aggregation (weighted rank; top_k_C)
# ---------------------------------------------------------------------------


def _run_method_C(
    df: pd.DataFrame,
    cfg: SelectionConfig,
    rank_weights: Dict[str, float],
    top_k_C: int,
    tie_method: str,
) -> pd.DataFrame:
    """
    Method C: rank aggregation.

    For each method m:
      rank_m = rank of frac_m (descending, best=1, tie_method as given).

    composite_rank = sum_m w_m * rank_m   (weights normalized over methods used).

    Lower composite_rank is better. Keep the best top_k_C.
    """
    methods = list(cfg.frac_cols.keys())
    w = _normalize_weights(methods, rank_weights)

    rank_parts: Dict[str, pd.Series] = {}
    for m in methods:
        col = cfg.frac_cols[m]
        vals = df[col].fillna(0.0)
        rank_parts[m] = vals.rank(
            ascending=False,
            method=cast(
                Literal["average", "min", "max", "first", "dense"],
                tie_method,
            ),
        )

    rank_df = pd.DataFrame(rank_parts)

    composite = 0.0
    for m in methods:
        composite = composite + w[m] * rank_df[m]

    df["rank_C"] = composite.rank(ascending=True, method="dense")

    df_sorted = df.sort_values(["rank_C", "variable"], ascending=[True, True])

    if top_k_C and top_k_C > 0:
        keep_idx = df_sorted.index[: int(top_k_C)]
    else:
        keep_idx = df_sorted.index

    df["in_C"] = False
    df.loc[keep_idx, "in_C"] = True
    return df


# ---------------------------------------------------------------------------
# High-level reconciliation API used by the runner
# ---------------------------------------------------------------------------


def reconcile_selections(
    comparison_csv: Path,
    out_prefix: Path,
    cfg: SelectionConfig,
    *,
    per_method_threshold: Dict[str, float],
    min_methods_selected: int,
    stability_weights: Dict[str, float],
    top_k_B: int,
    rank_weights: Dict[str, float],
    top_k_C: int,
    tie_method: str = "average",
) -> Dict[str, Path]:
    """
    High-level entry point called by scripts/preselection/run_reconcile_selection.py.

    Parameters
    ----------
    comparison_csv : Path
        CSV with columns: variable, group (optional),
        and frac columns as defined in cfg.frac_cols.

    out_prefix : Path
        Base path for outputs. Files written:
          - f"{out_prefix}_A_panel.json"
          - f"{out_prefix}_B_panel.json"
          - f"{out_prefix}_C_panel.json"
          - f"{out_prefix}_reconciled_table.csv"

    cfg : SelectionConfig
        Tells us which columns correspond to which methods and where
        group information lives.

    per_method_threshold : dict[str, float]
        Method -> threshold for Method A voting.

    min_methods_selected : int
        Minimum methods voting positive in Method A.

    stability_weights : dict[str, float]
        Method -> weight for Method B stability score.

    top_k_B : int
        Number of variables to keep in Method B (<=0 => keep all).

    rank_weights : dict[str, float]
        Method -> weight for Method C rank aggregation.

    top_k_C : int
        Number of variables to keep in Method C (<=0 => keep all).

    tie_method : str
        Tie-breaking passed to pandas.Series.rank in Method C.

    Returns
    -------
    dict[str, Path]
        {
          "A_json": Path(...),
          "B_json": Path(...),
          "C_json": Path(...),
          "table_csv": Path(...),
        }
    """
    comparison_csv = Path(comparison_csv)
    out_prefix = Path(out_prefix)

    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    df = _load_comparison(comparison_csv, cfg)

    # Apply A/B/C in sequence on the same DataFrame
    df = _run_method_A(df, cfg, per_method_threshold, min_methods_selected)
    df = _run_method_B(df, cfg, stability_weights, top_k_B)
    df = _run_method_C(df, cfg, rank_weights, top_k_C, tie_method)

    # Build JSON panels
    group_col = cfg.group_col if cfg.group_col is not None else None

    def _extract_panel(mask_col: str) -> list[dict[str, Any]]:
        sub = df[df[mask_col]].copy()
        cols = ["variable"]
        if group_col is not None and group_col in sub.columns:
            cols.append(group_col)
        out: list[dict[str, Any]] = []
        for _, row in sub[cols].iterrows():
            rec: dict[str, Any] = {"variable": row["variable"]}
            if group_col is not None:
                rec["group"] = row[group_col]
            out.append(rec)
        return out

    A_panel = _extract_panel("in_A")
    B_panel = _extract_panel("in_B")
    C_panel = _extract_panel("in_C")

    # Output paths
    base_str = str(out_prefix)
    A_json = Path(base_str + "_A_panel.json")
    B_json = Path(base_str + "_B_panel.json")
    C_json = Path(base_str + "_C_panel.json")
    table_csv = Path(base_str + "_reconciled_table.csv")

    A_json.write_text(json.dumps({"selected": A_panel}, indent=2), encoding="utf-8")
    B_json.write_text(json.dumps({"selected": B_panel}, indent=2), encoding="utf-8")
    C_json.write_text(json.dumps({"selected": C_panel}, indent=2), encoding="utf-8")

    df.to_csv(table_csv, index=False)

    return {
        "A_json": A_json,
        "B_json": B_json,
        "C_json": C_json,
        "table_csv": table_csv,
    }
