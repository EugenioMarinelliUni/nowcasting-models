#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from dfm_pipeline.ingestion.fred_md_rt_canonical_evidence import (
    build_rt_canonical_evidence_from_paths,
    write_evidence_csv,
)


DEFAULT_ROOT = Path(
    "data/metadata/series_maps"
)

DEFAULT_DOCUMENTARY = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_review__documentary.csv"
)

DEFAULT_C3 = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_comparability_2010_2026.csv"
)

DEFAULT_C4_SUMMARY = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_review__summary.csv"
)

DEFAULT_C4_ROLLING = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_review__rolling.csv"
)

DEFAULT_REVIEW_SPEC = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_review_spec.csv"
)

DEFAULT_SERIES_REVIEWS = (
    DEFAULT_ROOT
    / "fred_md_series_reviews.csv"
)

DEFAULT_OUTPUT = (
    DEFAULT_ROOT
    / "fred_md_rt_canonical_transition_review__evidence.csv"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the combined documentary, C3 and C4 "
            "RT_CANONICAL transition evidence table."
        )
    )

    parser.add_argument(
        "--documentary",
        type=Path,
        default=DEFAULT_DOCUMENTARY,
    )

    parser.add_argument(
        "--c3",
        type=Path,
        default=DEFAULT_C3,
    )

    parser.add_argument(
        "--c4-summary",
        type=Path,
        default=DEFAULT_C4_SUMMARY,
    )

    parser.add_argument(
        "--c4-rolling",
        type=Path,
        default=DEFAULT_C4_ROLLING,
    )

    parser.add_argument(
        "--review-spec",
        type=Path,
        default=DEFAULT_REVIEW_SPEC,
    )

    parser.add_argument(
        "--series-reviews",
        type=Path,
        default=DEFAULT_SERIES_REVIEWS,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    evidence = (
        build_rt_canonical_evidence_from_paths(
            documentary_path=(
                args.documentary
            ),
            c3_path=args.c3,
            c4_summary_path=(
                args.c4_summary
            ),
            c4_rolling_path=(
                args.c4_rolling
            ),
            review_spec_path=(
                args.review_spec
            ),
            series_reviews_path=(
                args.series_reviews
            ),
        )
    )

    path = write_evidence_csv(
        evidence,
        args.output,
        overwrite=args.overwrite,
    )

    print()
    print(
        "RT_CANONICAL transition evidence built."
    )
    print(
        "Canonical concepts:",
        len(evidence),
    )
    print(
        "Documentary reviews complete:",
        int(
            evidence[
                "documentary_review_complete"
            ].sum()
        ),
    )
    print(
        "Output:",
        path,
    )
    print()
    print(
        "No automatic RT_CANONICAL acceptance or "
        "rejection decision has been made."
    )


if __name__ == "__main__":
    main()