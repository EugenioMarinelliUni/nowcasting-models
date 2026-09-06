#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_decisions import (
    RTCanonicalDecisionValidationError,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical_freeze import (
    RTCanonicalFreezeError,
    freeze_rt_canonical_from_paths,
)


DEFAULT_ROOT = Path(
    "data/metadata/series_maps"
)

DEFAULT_CANDIDATE_PANEL = (
    DEFAULT_ROOT
    / (
        "fred_md_panel_rt_canonical_"
        "2010_2026__candidate.csv"
    )
)

DEFAULT_CANDIDATE_SOURCE_MAP = (
    DEFAULT_ROOT
    / (
        "fred_md_panel_rt_canonical_"
        "2010_2026__candidate__source_map.csv"
    )
)

DEFAULT_EVIDENCE = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_transition_review"
        "__evidence.csv"
    )
)

DEFAULT_DECISIONS = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_transition_review"
        "__decisions.csv"
    )
)

DEFAULT_OUTPUT = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_frozen_"
        "2010_2026.csv"
    )
)

DEFAULT_MANIFEST = (
    DEFAULT_ROOT
    / (
        "fred_md_rt_canonical_"
        "freeze_manifest.json"
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze the reviewed FRED-MD RT_CANONICAL "
            "2010-2026 candidate into an immutable "
            "source-segment specification and provenance "
            "manifest."
        )
    )

    parser.add_argument(
        "--candidate-panel",
        type=Path,
        default=DEFAULT_CANDIDATE_PANEL,
        help=(
            "Candidate RT_CANONICAL concept-level CSV."
        ),
    )

    parser.add_argument(
        "--candidate-source-map",
        type=Path,
        default=DEFAULT_CANDIDATE_SOURCE_MAP,
        help=(
            "Candidate RT_CANONICAL source-map CSV."
        ),
    )

    parser.add_argument(
        "--evidence",
        type=Path,
        default=DEFAULT_EVIDENCE,
        help=(
            "Combined transition evidence CSV."
        ),
    )

    parser.add_argument(
        "--decisions",
        type=Path,
        default=DEFAULT_DECISIONS,
        help=(
            "Validated manual transition decisions CSV."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=(
            "Frozen RT_CANONICAL CSV to write."
        ),
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=(
            "Freeze provenance manifest JSON to write."
        ),
    )

    parser.add_argument(
        "--expected-start",
        default="2010-01",
        help=(
            "Expected first vintage in the frozen "
            "stable window."
        ),
    )

    parser.add_argument(
        "--expected-end",
        default="2026-06",
        help=(
            "Expected final vintage in the frozen "
            "stable window."
        ),
    )

    parser.add_argument(
        "--freeze-date",
        default=date.today().isoformat(),
        help=(
            "Freeze date in YYYY-MM-DD format. "
            "Defaults to today's local calendar date."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Explicitly allow replacement of existing "
            "frozen CSV/manifest artifacts."
        ),
    )

    return parser


def main() -> int:
    args = (
        build_parser()
        .parse_args()
    )

    try:
        (
            output_path,
            manifest_path,
            manifest,
        ) = freeze_rt_canonical_from_paths(
            candidate_panel_path=(
                args.candidate_panel
            ),
            candidate_source_map_path=(
                args.candidate_source_map
            ),
            evidence_path=(
                args.evidence
            ),
            decisions_path=(
                args.decisions
            ),
            output_csv_path=(
                args.output
            ),
            manifest_path=(
                args.manifest
            ),
            expected_start=(
                args.expected_start
            ),
            expected_end=(
                args.expected_end
            ),
            freeze_date=(
                args.freeze_date
            ),
            overwrite=(
                args.overwrite
            ),
        )

    except (
        FileNotFoundError,
        OSError,
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
        RTCanonicalDecisionValidationError,
        RTCanonicalFreezeError,
    ) as exc:
        print(
            "RT_CANONICAL freeze: FAIL",
            file=sys.stderr,
        )

        print(
            str(exc),
            file=sys.stderr,
        )

        return 1

    counts = manifest[
        "counts"
    ]

    window = manifest[
        "stable_window"
    ]

    print(
        "RT_CANONICAL freeze completed."
    )

    print(
        "Freeze gate: PASS"
    )

    print(
        "Canonical concepts: "
        f"{counts['canonical_concepts']}"
    )

    print(
        "Direct concepts: "
        f"{counts['direct_concepts']}"
    )

    print(
        "Switch-by-vintage concepts: "
        f"{counts['switch_by_vintage_concepts']}"
    )

    print(
        "Source segments: "
        f"{counts['source_segments']}"
    )

    print(
        "Transition boundaries: "
        f"{counts['transition_boundaries']}"
    )

    print(
        "Stable window: "
        f"{window['start']}..{window['end']}"
    )

    print()

    print(
        "Decision distribution:"
    )

    for (
        decision,
        count,
    ) in manifest[
        "decision_distribution"
    ].items():
        print(
            f"  {decision}: {count}"
        )

    print()

    print(
        f"Frozen CSV: {output_path}"
    )

    print(
        f"Manifest: {manifest_path}"
    )

    print()

    print(
        "The frozen specification preserves the "
        "reviewed candidate exactly; no new "
        "statistical acceptance rule or transition "
        "decision was applied by this command."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )