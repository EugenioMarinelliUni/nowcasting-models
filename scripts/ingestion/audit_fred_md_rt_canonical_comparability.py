#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_comparability import (
    audit_rt_canonical_comparability,
)


ENV_RAW_DIR = "FRED_MD_VINTAGE_RAW_DIR"
DEFAULT_PANEL = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_rt_canonical_2010_2026__candidate.csv"
)
DEFAULT_SOURCE_MAP = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_rt_canonical_2010_2026__candidate__source_map.csv"
)
DEFAULT_OUT = Path(
    "data/metadata/series_maps/"
    "fred_md_rt_canonical_transition_comparability_2010_2026.csv"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Run diagnostic transformed predecessor/successor comparability "
            "checks for RT_CANONICAL source transitions. The audit uses only "
            "historical FRED-MD vintages and does not use forecast outcomes or "
            "arbitrary statistical acceptance thresholds."
        )
    )
    p.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    p.add_argument("--source-map", type=Path, default=DEFAULT_SOURCE_MAP)
    p.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=(
            "Historical FRED-MD raw vintage directory. If omitted, use "
            "FRED_MD_VINTAGE_RAW_DIR."
        ),
    )
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--aligned-out", type=Path, default=None)
    p.add_argument("--summary-out", type=Path, default=None)
    p.add_argument("--recursive", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument(
        "--fail-on-mechanical",
        action="store_true",
        help=(
            "Exit with status 1 if any transition has a C3 mechanical failure. "
            "Statistical diagnostics themselves remain non-binding."
        ),
    )
    return p.parse_args()


def _read(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"{label} not found: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _resolve_raw_dir(args: argparse.Namespace) -> Path:
    if args.raw_dir is not None:
        path = args.raw_dir.expanduser().resolve()
    else:
        env = os.environ.get(ENV_RAW_DIR)
        if not env:
            raise SystemExit(
                "No raw vintage directory supplied. Use --raw-dir or set "
                f"{ENV_RAW_DIR}."
            )
        path = Path(env).expanduser().resolve()

    if not path.exists():
        raise SystemExit(f"Raw vintage directory not found: {path}")
    if not path.is_dir():
        raise SystemExit(f"Expected raw vintage directory: {path}")
    return path


def main() -> int:
    args = parse_args()

    aligned_out = args.aligned_out or args.out.with_name(
        f"{args.out.stem}__aligned.csv"
    )
    summary_out = args.summary_out or args.out.with_name(
        f"{args.out.stem}__summary.csv"
    )
    outputs = [args.out, aligned_out, summary_out]

    existing = [path for path in outputs if path.exists()]
    if existing and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing RT_CANONICAL C3 artifacts: "
            + ", ".join(str(path) for path in existing)
            + ". Use --overwrite to regenerate."
        )

    panel = _read(args.panel, "RT_CANONICAL candidate panel")
    source_map = _read(args.source_map, "RT_CANONICAL candidate source map")
    raw_dir = _resolve_raw_dir(args)

    comparison, aligned, run_summary = audit_rt_canonical_comparability(
        raw_dir,
        panel,
        source_map,
        recursive=args.recursive,
    )

    for path in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)

    comparison.to_csv(args.out, index=False)
    aligned.to_csv(aligned_out, index=False)
    run_summary.to_csv(summary_out, index=False)

    print("FRED-MD RT_CANONICAL C3 comparability audit completed")
    print(f"panel                 : {args.panel}")
    print(f"source_map            : {args.source_map}")
    print(f"raw_dir               : {raw_dir}")
    print(f"comparison_report     : {args.out}")
    print(f"aligned_report        : {aligned_out}")
    print(f"summary               : {summary_out}")
    print()
    print(run_summary.to_string(index=False))

    display_cols = [
        "canonical_id",
        "transition_vintage",
        "old_source",
        "new_source",
        "comparison_mode",
        "n_common_valid",
        "pearson_corr",
        "spearman_corr",
        "old_std",
        "new_std",
        "std_ratio_new_old",
        "rmse_difference",
        "nrmse_old_std",
        "sign_agreement",
        "ols_beta",
        "ols_r2",
        "diagnostic_status",
    ]
    print("\nTransition diagnostics")
    print("----------------------")
    print(comparison[display_cols].to_string(index=False))

    bad = comparison.loc[
        comparison["diagnostic_status"].eq("mechanical_failure")
    ]
    print("\nC3 mechanical failures")
    print("----------------------")
    if bad.empty:
        print("None")
    else:
        print(
            bad[
                [
                    "canonical_id",
                    "transition_vintage",
                    "comparison_mode",
                    "mechanical_failure_reasons",
                ]
            ].to_string(index=False)
        )

    if args.fail_on_mechanical and not bad.empty:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
