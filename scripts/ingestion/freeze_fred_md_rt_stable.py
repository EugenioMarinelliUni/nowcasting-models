#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_stable import (
    build_rt_stable_specification,
    build_rt_stable_summary,
)


DEFAULT_REGISTRY = Path(
    "data/metadata/series_maps/"
    "fred_md_series_registry.csv"
)

DEFAULT_STAGE1 = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_eligibility_"
    "rt_stable_candidates_2010_2026.csv"
)

DEFAULT_STAGE2 = Path(
    "data/metadata/series_maps/"
    "fred_md_transformed_eligibility_2010_2026.csv"
)

DEFAULT_OUT = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_rt_stable_2010_2026.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze the authoritative FRED-MD RT_STABLE "
            "raw predictor specification from the curated "
            "registry and validated Stage-1/Stage-2 QC "
            "reports. No forecast-performance information "
            "is used."
        )
    )

    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
    )

    parser.add_argument(
        "--stage1-report",
        type=Path,
        default=DEFAULT_STAGE1,
    )

    parser.add_argument(
        "--stage2-report",
        type=Path,
        default=DEFAULT_STAGE2,
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
    )

    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    return parser.parse_args()


def _read_csv(
    path: Path,
    *,
    label: str,
) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(
            f"{label} not found: {path}"
        )

    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
    )


def main() -> int:
    args = parse_args()

    summary_out = (
        args.summary_out
        or args.out.with_name(
            f"{args.out.stem}__summary.csv"
        )
    )

    existing = [
        path
        for path in (
            args.out,
            summary_out,
        )
        if path.exists()
    ]

    if existing and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing RT_STABLE "
            "artifacts: "
            + ", ".join(
                str(path)
                for path in existing
            )
            + ". Use --overwrite to regenerate."
        )

    registry = _read_csv(
        args.registry,
        label="Registry",
    )

    stage1 = _read_csv(
        args.stage1_report,
        label="Stage-1 report",
    )

    stage2 = _read_csv(
        args.stage2_report,
        label="Stage-2 report",
    )

    specification = (
        build_rt_stable_specification(
            registry,
            stage1,
            stage2,
        )
    )

    summary = build_rt_stable_summary(
        specification
    )

    args.out.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_out.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    specification.to_csv(
        args.out,
        index=False,
    )

    summary.to_csv(
        summary_out,
        index=False,
    )

    print(
        "FRED-MD RT_STABLE specification frozen"
    )
    print(
        f"registry              : {args.registry}"
    )
    print(
        f"stage1_report         : {args.stage1_report}"
    )
    print(
        f"stage2_report         : {args.stage2_report}"
    )
    print(
        f"panel                 : {args.out}"
    )
    print(
        f"summary               : {summary_out}"
    )

    print()
    print(
        summary.to_string(index=False)
    )

    reviewed = specification.loc[
        specification[
            "selection_class"
        ].eq("reviewed_stable_keep"),
        [
            "panel_position",
            "raw_series",
            "tcode",
            "stage1_min_n_valid",
            "stage2_min_transformed_n_valid",
            "stage2_min_transformed_std",
        ],
    ]

    print()
    print("Reviewed stable keeps")
    print("---------------------")

    if reviewed.empty:
        print("None")
    else:
        print(
            reviewed.to_string(index=False)
        )

    print()
    print(
        "NOTE: panel membership is frozen from "
        "registry decisions and QC gates only; "
        "nowcast performance is not used."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())