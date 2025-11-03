#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd

# --- project imports (post-refactor) ---
try:
    from dfm_pipeline.preselection.baseline_screening import selectors as _selectors
except ModuleNotFoundError:
    from dfm_pipeline.preselection import selectors as _selectors  # type: ignore[assignment]

from dfm_pipeline.preselection.io import load_X_y


# --------------------------
# helpers for outputs
# --------------------------
def _meta_and_out_paths(panel: str, tag: str, method: str, label: str | None) -> Tuple[Path, Path]:
    suffix = f"__{label}" if label else ""
    meta = Path(f"data/metadata/variants/{panel}__{tag}__preselect_{method}{suffix}.json")
    out_dir = Path(f"dataset/{panel}/preselect/{method}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"X_panel_z__{panel}__{tag}__preselect-{method}{suffix}.csv"
    return meta, out


def _write_outputs(
    panel: str,
    tag: str,
    method: str,
    selected: List[str],
    X_full: pd.DataFrame,
    params: Dict,
    *,
    label: str | None,
) -> None:
    meta_p, out_p = _meta_and_out_paths(panel, tag, method, label)

    payload = {
        "method": method,
        "panel_id": panel,
        "train_tag": tag,
        "params": params,
        "selected": selected,
    }
    meta_p.parent.mkdir(parents=True, exist_ok=True)
    meta_p.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Guard: only save columns that still exist in X_full (defensive)
    keep = [c for c in selected if c in X_full.columns]
    X_full.loc[:, keep].to_csv(out_p, index_label="Date")


def _mask_to_quarter_stamps(X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Keep rows where y is observed (quarter-start stamps in your setup).
    """
    if not X.index.equals(y.index):
        raise ValueError("Index mismatch between X and y. Make sure both share the same monthly index.")
    m = y.notna()
    return X.loc[m], y.loc[m]


# --------------------------
# main runner
# --------------------------
def run_for_one(
    panel: str,
    tag: str,
    method: str,
    *,
    min_features: int,
    max_features: int,
    dedup_tau: float,
    # SIS
    sis_tau: float,
    sis_topn: int,
    # t-stat
    tstat_alpha: float,
    tstat_topn: int,
    tstat_ar_lags: int,
    hac_lags: str | int,
    agg_rule_map: str | None,
    agg_rule_default: str | None,
    x_adl_lags: int,
    adl_score: str,
    # LARS
    cv: int,
    # labeling
    label: str | None,
) -> List[str]:
    # Load standardized training data. Tolerate both (X, y) and (X, y, meta).
    res: Any = load_X_y(panel, tag)
    if not (isinstance(res, tuple) and len(res) >= 2):
        raise ValueError("load_X_y(panel, tag) must return at least (X, y).")
    X: pd.DataFrame = res[0]
    y: pd.Series = res[1]

    # Restrict to quarter stamps (where y is observed)
    Xs, ys = _mask_to_quarter_stamps(X, y)

    # Choose method
    if method == "sis":
        cols = _selectors.sis_select(
            Xs,
            ys,
            min_features=min_features,
            max_features=max_features,
            dedup_tau=dedup_tau,
            sis_tau=sis_tau,
            sis_topn=sis_topn,
        )
    elif method == "tstat":
        cols = _selectors.tstat_select(
            Xs,
            ys,
            min_features=min_features,
            max_features=max_features,
            dedup_tau=dedup_tau,
            tstat_alpha=tstat_alpha,
            tstat_topn=tstat_topn,
            hac_lags=hac_lags,          # "auto" or int
            ar_lags=tstat_ar_lags,      # 0 or 4 typical
            agg_rule_map=agg_rule_map,  # per-series JSON or None
            agg_rule_default=agg_rule_default,  # "sum3m" | "mean3m" | "last" or None
            x_adl_lags=x_adl_lags,      # 0 = off, 1 = ADL(1)
            adl_score=adl_score,        # "fstat" | "max_t"
        )
    elif method == "lars":
        cols = _selectors.lars_select(
            Xs,
            ys,
            min_features=min_features,
            max_features=max_features,
            dedup_tau=dedup_tau,
            cv=cv,
        )
    else:
        raise ValueError("method must be one of {'sis','tstat','lars'}")

    # Persist artifacts
    params = {
        "min_features": int(min_features),
        "max_features": int(max_features),
        "dedup_tau": float(dedup_tau),
        "sis_tau": float(sis_tau),
        "sis_topn": int(sis_topn),
        "tstat_alpha": float(tstat_alpha),
        "tstat_topn": int(tstat_topn),
        "tstat_ar_lags": int(tstat_ar_lags),
        "hac_lags": hac_lags,
        "agg_rule_map": agg_rule_map,
        "agg_rule_default": agg_rule_default,
        "x_adl_lags": int(x_adl_lags),
        "adl_score": adl_score,
        "cv": int(cv),
        "label": label,
    }
    _write_outputs(panel, tag, method, cols, X, params, label=label)

    print(f"{panel} {tag} {method}: selected {len(cols)} vars (label={label or '-'})")
    return cols


def main():
    ap = argparse.ArgumentParser(description="Baseline variable preselection (SIS / t-stat / LARS).")
    ap.add_argument("--panel", required=True, help="Panel id, e.g., 1960_noVIX or 1965_withVIX")
    ap.add_argument("--tag", required=True, help="Training tag, e.g., train1960_2015")
    ap.add_argument("--method", choices=["sis", "tstat", "lars", "all"], default="all")

    # Global guardrails
    ap.add_argument("--min_features", type=int, default=30)
    ap.add_argument("--max_features", type=int, default=80)
    ap.add_argument("--dedup_tau", type=float, default=0.98)

    # SIS knobs
    ap.add_argument("--sis_tau", type=float, default=0.0)
    ap.add_argument("--sis_topn", type=int, default=0)

    # t-stat knobs
    ap.add_argument("--tstat_alpha", type=float, default=0.0)
    ap.add_argument("--tstat_topn", type=int, default=0)
    ap.add_argument("--tstat_ar_lags", type=int, default=0,
                    help="AR lags of y to include in t-stat regressions (0 = none, 4 = AR(4)).")
    ap.add_argument("--hac_lags", default="auto",
                    help='Newey–West HAC lags for t-stat ("auto" or integer).')

    # Aggregation controls (for t-stat)
    ap.add_argument("--agg_rule_map", type=str, default=None,
                    help="Path to per-series aggregation rules JSON (sum3m/mean3m/last per column).")
    ap.add_argument("--agg_rule_default", type=str, default=None,
                    choices=["sum3m", "mean3m", "last"],
                    help="Apply one aggregation rule to all series if no map is provided.")

    # ADL controls (for t-stat)
    ap.add_argument("--x_adl_lags", type=int, default=0,
                    help="Number of short lags of each X to include as a block (0 = off).")
    ap.add_argument("--adl_score", type=str, default="fstat", choices=["fstat", "max_t"],
                    help="Scoring for ADL block: 'fstat' (joint F) or 'max_t' among block coefficients.")

    # LARS knob
    ap.add_argument("--cv", type=int, default=10, help="CV folds for LARS (LassoLarsCV)")

    # Labeling
    ap.add_argument("--label", type=str, default=None,
                    help="Suffix to append to output filenames for this run.")

    args = ap.parse_args()

    methods = ["sis", "tstat", "lars"] if args.method == "all" else [args.method]
    for m in methods:
        try:
            run_for_one(
                args.panel,
                args.tag,
                m,
                min_features=args.min_features,
                max_features=args.max_features,
                dedup_tau=args.dedup_tau,
                sis_tau=args.sis_tau,
                sis_topn=args.sis_topn,
                tstat_alpha=args.tstat_alpha,
                tstat_topn=args.tstat_topn,
                tstat_ar_lags=args.tstat_ar_lags,
                hac_lags=args.hac_lags,
                agg_rule_map=args.agg_rule_map,
                agg_rule_default=args.agg_rule_default,
                x_adl_lags=args.x_adl_lags,
                adl_score=args.adl_score,
                cv=args.cv,
                label=args.label,
            )
        except Exception as e:
            print(f"[ERROR] {args.panel} {args.tag} {m}: {e}")
            raise


if __name__ == "__main__":
    # import sys
    # sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    main()
