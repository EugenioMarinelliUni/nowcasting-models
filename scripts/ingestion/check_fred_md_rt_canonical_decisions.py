#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_decisions import (
    RTCanonicalDecisionValidationError,
    assert_rt_canonical_freezable,
    validate_rt_canonical_decisions,
)


DEFAULT_ROOT = Path(
    "data/metadata/series_maps"
)

DEFAULT_DECISIONS = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_transition_review"
        "__decisions.csv"
    )
)

DEFAULT_EVIDENCE = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_transition_review"
        "__evidence.csv"
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the RT_CANONICAL transition "
            "decision artifact and optionally apply "
            "the final decision-level freezability gate."
        )
    )

    parser.add_argument(
        "--decisions",
        type=Path,
        default=DEFAULT_DECISIONS,
        help=(
            "Path to the RT_CANONICAL transition "
            "decisions CSV."
        ),
    )

    parser.add_argument(
        "--evidence",
        type=Path,
        default=DEFAULT_EVIDENCE,
        help=(
            "Path to the combined RT_CANONICAL "
            "transition evidence CSV."
        ),
    )

    parser.add_argument(
        "--require-freezable",
        action="store_true",
        help=(
            "Apply the stronger freeze gate. "
            "Fails if documentary review is incomplete, "
            "a transition needs further review, or a "
            "transition is excluded from the current "
            "RT_CANONICAL candidate."
        ),
    )

    return parser


def main() -> int:
    args = (
        build_parser()
        .parse_args()
    )

    try:
        decisions = pd.read_csv(
            args.decisions,
            dtype=str,
            keep_default_na=False,
        )

        evidence = pd.read_csv(
            args.evidence,
            dtype=str,
            keep_default_na=False,
        )

        if args.require_freezable:
            validated = (
                assert_rt_canonical_freezable(
                    decisions,
                    evidence=evidence,
                )
            )
        else:
            validated = (
                validate_rt_canonical_decisions(
                    decisions,
                    evidence=evidence,
                )
            )

    except (
        FileNotFoundError,
        pd.errors.EmptyDataError,
        RTCanonicalDecisionValidationError,
    ) as exc:
        print(
            "RT_CANONICAL decision validation: FAIL",
            file=sys.stderr,
        )

        print(
            str(exc),
            file=sys.stderr,
        )

        return 1

    print(
        "RT_CANONICAL decision artifact "
        "validation: PASS"
    )

    print(
        f"Decision rows: {len(validated)}"
    )

    print(
        "Unique concepts: "
        f"{validated['canonical_id'].nunique()}"
    )

    print()

    print(
        "Decision distribution:"
    )

    counts = (
        validated[
            "review_decision"
        ]
        .value_counts()
    )

    for decision, count in (
        counts.items()
    ):
        print(
            f"  {decision}: {count}"
        )

    print()

    print(
        validated[
            [
                "canonical_id",
                "review_decision",
                "reviewer",
                "review_date",
            ]
        ].to_string(
            index=False
        )
    )

    if args.require_freezable:
        print()

        print(
            "RT_CANONICAL decision-level "
            "freeze gate: PASS"
        )

        print(
            "No incomplete, unresolved, or excluded "
            "transition decisions remain."
        )

        print(
            "The candidate is eligible for the "
            "subsequent RT_CANONICAL freeze step."
        )
    else:
        print()

        print(
            "Validation only: no RT_CANONICAL "
            "freeze gate was requested."
        )

    print()

    print(
        "No decisions were generated, changed, "
        "accepted, rejected, or frozen by this command."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )