#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_transformed_qc import (
    audit_transformed_collection,
)

ENV_RAW_DIR = "FRED_MD_VINTAGE_RAW_DIR"
DEFAULT_REGISTRY = Path(
    "data/metadata/series_maps/fred_md_series_registry.csv"
)
DEFAULT_STAGE1 = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_eligibility_all_stable_2010_2026.csv"
)
DEFAULT_OUT = Path(
    "data/metadata/series_maps/"
    "fred_md_transformed_eligibility_2010_2026.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Stage-2 transformed-series QC for the provisional "
            "RT_STABLE FRED-MD universe. The command uses each historical "
            "vintage's embedded t-codes and never modifies raw source files."
        )
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=(
            "Directory containing raw FRED-MD vintage CSVs. If omitted, "
            f"read {ENV_RAW_DIR}."
        ),
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
        help=f"Curated series registry (default: {DEFAULT_REGISTRY}).",
    )
    parser.add_argument(
        "--stage1-report",
        type=Path,
        default=DEFAULT_STAGE1,
        help=(
            "Stage-1 all-stable QC report used as a gate. Pass an empty "
            "string only by editing the command; normally keep this enabled."
        ),
    )
    parser.add_argument(
        "--start",
        default="2010-01",
        help="First vintage month, YYYY-MM (default: 2010-01).",
    )
    parser.add_argument(
        "--end",
        default="2026-06",
        help="Last vintage month, YYYY-MM (default: 2026-06).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Per-series Stage-2 output (default: {DEFAULT_OUT}).",
    )
    parser.add_argument(
        "--by-vintage-out",
        type=Path,
        default=None,
        help="Optional vintage-by-series output path.",
    )
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help="Optional compact summary output path.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively for raw vintage CSV files.",
    )
    parser.add_argument(
        "--near-zero-std",
        type=float,
        default=None,
        help=(
            "Optional absolute transformed-standard-deviation threshold used "
            "only as a diagnostic flag. Omit on the first research run so "
            "the empirical variance distribution can be inspected before "
            "freezing a threshold."
        ),
    )
    parser.add_argument(
        "--history-cutoff",
        default=None,
        help=(
            "Optional YYYY-MM-DD cutoff for pre-evaluation transformed-history "
            "counts. No history threshold is imposed unless --min-history is "
            "also supplied."
        ),
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=None,
        help=(
            "Optional minimum valid transformed observations up to "
            "--history-cutoff. This is a flag, not a hard failure."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of extreme series printed (default: 20).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacement of existing Stage-2 artifacts.",
    )
    parser.add_argument(
        "--fail-on-hard",
        action="store_true",
        help="Return exit status 1 after writing reports if hard failures exist.",
    )
    return parser.parse_args()


def resolve_raw_dir(args: argparse.Namespace) -> Path:
    if args.raw_dir is not None:
        return args.raw_dir.expanduser().resolve()
    value = os.environ.get(ENV_RAW_DIR)
    if value:
        return Path(value).expanduser().resolve()
    raise SystemExit(
        "No raw directory supplied. Use --raw-dir PATH or set "
        f"{ENV_RAW_DIR}."
    )


def _print_extremes(by_series: pd.DataFrame, *, top_n: int) -> None:
    n = min(top_n, len(by_series))

    print()
    print("Lowest minimum transformed observation counts")
    print("---------------------------------------------")
    print(
        by_series[
            [
                "raw_series",
                "min_transformed_n_valid",
                "min_transformed_observed_fraction",
                "min_transformed_std",
                "stage2_status",
            ]
        ]
        .sort_values(
            ["min_transformed_n_valid", "raw_series"],
            ascending=[True, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Lowest minimum transformed standard deviations")
    print("----------------------------------------------")
    print(
        by_series[
            [
                "raw_series",
                "min_transformed_std",
                "min_transformed_n_valid",
                "n_zero_variance_vintages",
                "n_near_zero_variance_vintages",
                "stage2_status",
            ]
        ]
        .sort_values(
            ["min_transformed_std", "raw_series"],
            ascending=[True, True],
            na_position="last",
        )
        .head(n)
        .to_string(index=False)
    )

    hard = by_series.loc[by_series["stage2_hard_fail"]]
    print()
    print("Stage-2 hard failures")
    print("---------------------")
    if hard.empty:
        print("None")
    else:
        print(
            hard[
                [
                    "raw_series",
                    "n_vintages_hard_fail",
                    "hard_failure_reasons",
                ]
            ].to_string(index=False)
        )


def main() -> int:
    args = parse_args()

    if args.top_n <= 0:
        raise SystemExit("--top-n must be greater than zero.")
    if args.near_zero_std is not None and args.near_zero_std < 0:
        raise SystemExit("--near-zero-std must be non-negative.")
    if args.min_history is not None and args.history_cutoff is None:
        raise SystemExit("--min-history requires --history-cutoff.")
    if args.min_history is not None and args.min_history < 0:
        raise SystemExit("--min-history must be non-negative.")

    raw_dir = resolve_raw_dir(args)
    if not args.registry.exists():
        raise SystemExit(f"Registry not found: {args.registry}")
    if not args.stage1_report.exists():
        raise SystemExit(f"Stage-1 report not found: {args.stage1_report}")

    by_vintage_out = args.by_vintage_out or args.out.with_name(
        f"{args.out.stem}__by_vintage.csv"
    )
    summary_out = args.summary_out or args.out.with_name(
        f"{args.out.stem}__summary.csv"
    )

    protected = [
        path
        for path in (args.out, by_vintage_out, summary_out)
        if path.exists()
    ]
    if protected and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing Stage-2 artifacts: "
            + ", ".join(str(path) for path in protected)
            + ". Use --overwrite to regenerate them."
        )

    registry = pd.read_csv(
        args.registry,
        dtype=str,
        keep_default_na=False,
    )
    stage1 = pd.read_csv(
        args.stage1_report,
        dtype=str,
        keep_default_na=False,
    )

    outputs = audit_transformed_collection(
        raw_dir,
        registry,
        start=args.start,
        end=args.end,
        stage1_report=stage1,
        recursive=args.recursive,
        near_zero_std=args.near_zero_std,
        history_cutoff=args.history_cutoff,
        min_history=args.min_history,
    )

    by_series = outputs["by_series"]
    by_vintage = outputs["by_vintage"]
    summary = outputs["summary"]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    by_series.to_csv(args.out, index=False, float_format="%.10g")
    by_vintage.to_csv(by_vintage_out, index=False, float_format="%.10g")
    summary.to_csv(summary_out, index=False, float_format="%.10g")

    print("FRED-MD Stage-2 transformed-series QC completed")
    print(f"raw_dir               : {raw_dir}")
    print(f"registry              : {args.registry}")
    print(f"stage1_report         : {args.stage1_report}")
    print(f"vintage_range         : {args.start} -> {args.end}")
    print(f"series_report         : {args.out}")
    print(f"by_vintage_report     : {by_vintage_out}")
    print(f"summary               : {summary_out}")
    print()
    print(summary.to_string(index=False))

    _print_extremes(by_series, top_n=args.top_n)

    print()
    print(
        "NOTE: on the first Stage-2 run, do not set --near-zero-std or an "
        "arbitrary minimum-history threshold. Inspect the empirical "
        "distributions first, then freeze any research policy independently "
        "of final evaluation performance."
    )

    if args.fail_on_hard and bool(by_series["stage2_hard_fail"].any()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
