#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_panel_eligibility import (
    build_eligibility_report,
    build_summary,
    require_registry_columns,
)

DEFAULT_REGISTRY = Path(
    "data/metadata/series_maps/fred_md_series_registry.csv"
)
DEFAULT_OUT = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_eligibility_2010_2026.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Stage-1 structural eligibility QC on FRED-MD stable "
            "predictor candidates. This command is diagnostic and does not "
            "modify the curated registry or make final inclusion decisions."
        )
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
        help=f"Curated FRED-MD registry (default: {DEFAULT_REGISTRY}).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Detailed QC output CSV (default: {DEFAULT_OUT}).",
    )
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help="Optional summary CSV. Default: <out stem>__summary.csv.",
    )
    parser.add_argument(
        "--scope",
        choices=("ordinary", "all-stable", "rt-stable-candidates"),
        default="ordinary",
        help=(
            "ordinary = unreviewed mechanically stable candidates; "
            "all-stable = every mechanically stable candidate; "
            "rt-stable-candidates = ordinary candidates plus reviewed "
            "stable series explicitly retained for RT_STABLE."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of extreme cases printed for each diagnostic.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacement of existing QC artifacts.",
    )
    return parser.parse_args()


def _print_extremes(report: pd.DataFrame, *, top_n: int) -> None:
    n = min(top_n, len(report))

    print()
    print("Lowest minimum observed fractions")
    print("---------------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_min_n_valid",
                "coverage_min_observed_fraction",
                "coverage_max_pct_missing",
                "coverage_max_leading_missing",
                "qc_status",
            ]
        ]
        .sort_values(
            ["coverage_min_observed_fraction", "raw_series"],
            ascending=[True, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Largest leading-history gaps")
    print("----------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_max_leading_missing",
                "coverage_min_n_valid",
                "coverage_min_observed_fraction",
                "qc_status",
            ]
        ]
        .sort_values(
            ["coverage_max_leading_missing", "raw_series"],
            ascending=[False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Largest internal gaps")
    print("---------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_max_internal_missing",
                "coverage_vintages_with_internal_missing",
                "coverage_min_observed_fraction",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_max_internal_missing",
                "coverage_vintages_with_internal_missing",
                "raw_series",
            ],
            ascending=[False, False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Most persistent internal missingness")
    print("------------------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_vintages_with_internal_missing",
                "coverage_max_internal_missing",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_vintages_with_internal_missing",
                "coverage_max_internal_missing",
                "raw_series",
            ],
            ascending=[False, False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    hard = report.loc[report["qc_hard_fail"]].copy()
    print()
    print("Structural hard failures")
    print("------------------------")
    if hard.empty:
        print("None")
    else:
        print(
            hard[["raw_series", "hard_failure_reasons"]]
            .to_string(index=False)
        )


def main() -> int:
    args = parse_args()

    if args.top_n <= 0:
        raise SystemExit("--top-n must be greater than zero.")
    if not args.registry.exists():
        raise SystemExit(f"Registry not found: {args.registry}")

    summary_out = args.summary_out or args.out.with_name(
        f"{args.out.stem}__summary.csv"
    )
    protected = [
        path for path in (args.out, summary_out) if path.exists()
    ]
    if protected and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing QC artifacts: "
            + ", ".join(str(path) for path in protected)
            + ". Use --overwrite to regenerate them."
        )

    registry = pd.read_csv(
        args.registry,
        dtype=str,
        keep_default_na=False,
    )
    require_registry_columns(registry)

    report = build_eligibility_report(registry, scope=args.scope)
    summary = build_summary(registry, report, scope=args.scope)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.out, index=False)
    summary.to_csv(summary_out, index=False)

    print("FRED-MD panel eligibility QC completed")
    print(f"registry              : {args.registry}")
    print(f"scope                 : {args.scope}")
    print(f"report                : {args.out}")
    print(f"summary               : {summary_out}")
    print()
    print(summary.to_string(index=False))

    _print_extremes(report, top_n=args.top_n)

    print()
    print(
        "NOTE: qc_status is diagnostic and is not a final include/exclude "
        "decision. Leading, trailing, and internal missingness are reported "
        "rather than automatically rejected."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
