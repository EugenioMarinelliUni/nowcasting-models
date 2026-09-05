#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from dfm_pipeline.ingestion.fred_md_rt_canonical_review_execution import (
    run_rt_canonical_transition_review,
)


DEFAULT_SPEC = Path(
    "data/metadata/series_maps/"
    "fred_md_rt_canonical_transition_review_spec.csv"
)

DEFAULT_MANIFEST = Path(
    "data/metadata/series_maps/"
    "fred_md_rt_canonical_transition_review__reference_manifest.csv"
)

DEFAULT_REFERENCE_DIR = Path(
    "data/external/fred_transition_review"
)

DEFAULT_OUTPUT_DIR = Path(
    "data/metadata/series_maps"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the threshold-free C4 external-reference "
            "review for RT_CANONICAL source transitions."
        )
    )

    parser.add_argument(
        "--spec",
        type=Path,
        default=DEFAULT_SPEC,
        help=(
            "RT_CANONICAL transition-review specification "
            f"(default: {DEFAULT_SPEC})"
        ),
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=(
            "Committed reference provenance manifest "
            f"(default: {DEFAULT_MANIFEST})"
        ),
    )

    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=DEFAULT_REFERENCE_DIR,
        help=(
            "Git-ignored directory containing the "
            "downloaded FRED reference CSVs "
            f"(default: {DEFAULT_REFERENCE_DIR})"
        ),
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directory for C4 diagnostic reports "
            f"(default: {DEFAULT_OUTPUT_DIR})"
        ),
    )

    parser.add_argument(
        "--rolling-window-months",
        type=int,
        default=60,
        help=(
            "Trailing calendar-month diagnostic window "
            "(default: 60)"
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Replace existing C4 review reports. "
            "Without this flag, existing outputs cause "
            "the command to stop."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.rolling_window_months < 2:
        raise SystemExit(
            "--rolling-window-months must be at least 2"
        )

    result = run_rt_canonical_transition_review(
        spec_path=args.spec,
        manifest_path=args.manifest,
        reference_dir=args.reference_dir,
        output_dir=args.output_dir,
        rolling_window_months=(
            args.rolling_window_months
        ),
        overwrite=args.overwrite,
    )

    print()
    print(
        "RT_CANONICAL C4 transition review completed."
    )
    print(
        "Verified reference series:",
        result["n_reference_series"],
    )
    print(
        "Summary rows:",
        result["n_summary_rows"],
    )
    print(
        "Aligned rows:",
        result["n_aligned_rows"],
    )
    print(
        "Rolling rows:",
        result["n_rolling_rows"],
    )
    print(
        "Rolling window:",
        result["rolling_window_months"],
        "calendar months",
    )
    print()
    print(
        "Summary:",
        result["summary_path"],
    )
    print(
        "Aligned:",
        result["aligned_path"],
    )
    print(
        "Rolling:",
        result["rolling_path"],
    )
    print()
    print(
        "No automatic RT_CANONICAL acceptance or "
        "rejection decision has been made."
    )


if __name__ == "__main__":
    main()