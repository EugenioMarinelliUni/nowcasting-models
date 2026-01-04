#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict

from src.dfm_pipeline.preselection.reconcile_methods import (
    SelectionConfig,
    reconcile_selections,
)


def _parse_json_dict(s: str | None, default: Dict[str, float]) -> Dict[str, float]:
    if not s:
        return default
    try:
        obj = json.loads(s)
    except Exception as e:
        raise ValueError(f"Could not parse JSON: {s!r} ({e})")
    if not isinstance(obj, dict):
        raise ValueError(f"Expected a JSON object, got: {obj!r}")
    return {str(k): float(v) for k, v in obj.items()}


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Reconcile SIS / tstat / LARS selection fractions into combined panels "
            "(vote rule, stability score, rank aggregation)."
        )
    )

    ap.add_argument(
        "--comparison-csv",
        required=True,
        help="Path to selection_comparison_..._frac.csv.",
    )
    ap.add_argument(
        "--out-prefix",
        required=True,
        help=(
            "Prefix for outputs. For example "
            "data/metadata/variants_TEST_v2/reconciled_1960_noVIX_TEST_1990_2019"
        ),
    )

    # Method A hyperparameters
    ap.add_argument(
        "--per-method-threshold-json",
        default='{"sis": 0.5, "tstat": 0.5, "lars": 0.3}',
        help=(
            "JSON mapping {method -> threshold} for vote rule. "
            "Example: '{\"sis\":0.5,\"tstat\":0.5,\"lars\":0.3}'"
        ),
    )
    ap.add_argument(
        "--min-methods-selected",
        type=int,
        default=2,
        help="Minimum number of methods that must select a variable in Method A.",
    )

    # Method B hyperparameters
    ap.add_argument(
        "--stability-weights-json",
        default='{"sis": 0.4, "tstat": 0.4, "lars": 0.2}',
        help=(
            "JSON mapping {method -> weight} for stability score. "
            "Example: '{\"sis\":0.4,\"tstat\":0.4,\"lars\":0.2}'"
        ),
    )
    ap.add_argument(
        "--top-k-B",
        type=int,
        default=60,
        help="Number of variables to keep in Method B panel (score_B). <=0 means keep all.",
    )

    # Method C hyperparameters
    ap.add_argument(
        "--rank-weights-json",
        default='{"sis": 1.0, "tstat": 1.0, "lars": 1.0}',
        help=(
            "JSON mapping {method -> weight} for rank aggregation. "
            "Example: '{\"sis\":1.0,\"tstat\":1.0,\"lars\":1.0}'"
        ),
    )
    ap.add_argument(
        "--top-k-C",
        type=int,
        default=60,
        help="Number of variables to keep in Method C panel (rank_C). <=0 means keep all.",
    )
    ap.add_argument(
        "--tie-method",
        default="average",
        help="Tie-breaking mode passed to pandas.Series.rank (default: 'average').",
    )

    args = ap.parse_args()

    comparison_csv = Path(args.comparison_csv)
    out_prefix = Path(args.out_prefix)

    # Config: columns in your selection_comparison_*_frac.csv
    cfg = SelectionConfig(
        frac_cols={
            "sis": "sis_frac",
            "tstat": "tstat_frac",
            "lars": "lars_frac",
        },
        group_col="group",
    )

    # Parse JSON hyperparameters
    per_method_threshold = _parse_json_dict(
        args.per_method_threshold_json,
        default={"sis": 0.5, "tstat": 0.5, "lars": 0.3},
    )
    stability_weights = _parse_json_dict(
        args.stability_weights_json,
        default={"sis": 0.4, "tstat": 0.4, "lars": 0.2},
    )
    rank_weights = _parse_json_dict(
        args.rank_weights_json,
        default={"sis": 1.0, "tstat": 1.0, "lars": 1.0},
    )

    outputs = reconcile_selections(
        comparison_csv=comparison_csv,
        out_prefix=out_prefix,
        cfg=cfg,
        per_method_threshold=per_method_threshold,
        min_methods_selected=args.min_methods_selected,
        stability_weights=stability_weights,
        top_k_B=args.top_k_B,
        rank_weights=rank_weights,
        top_k_C=args.top_k_C,
        tie_method=args.tie_method,
    )

    print(f"[reconcile] input: {comparison_csv}")
    print(f"[reconcile] wrote A-panel JSON: {outputs['A_json']}")
    print(f"[reconcile] wrote B-panel JSON: {outputs['B_json']}")
    print(f"[reconcile] wrote C-panel JSON: {outputs['C_json']}")
    print(f"[reconcile] wrote reconciled table: {outputs['table_csv']}")


if __name__ == "__main__":
    main()
