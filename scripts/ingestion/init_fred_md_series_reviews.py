#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_registry_reviews import (
    build_review_template,
)


DEFAULT_EXTRA = (
    "CMRMTSPLx",
    "UMCSENTx",
    "ACOGNO",
    "AMDMNOx",
    "AMDMUOx",
    "ANDENOx",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a sparse FRED-MD documentary-review queue from a "
            "generated registry seed."
        )
    )
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--no-default-extra",
        action="store_true",
        help="Do not add the six special stable-series review cases.",
    )
    parser.add_argument(
        "--extra-series",
        default="",
        help="Comma-separated additional raw mnemonics to include.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.out.exists():
        raise SystemExit(
            f"Refusing to overwrite existing review file: {args.out}"
        )

    seed = pd.read_csv(args.seed)

    extra = set()
    if not args.no_default_extra:
        extra.update(DEFAULT_EXTRA)

    if args.extra_series.strip():
        extra.update(
            x.strip()
            for x in args.extra_series.split(",")
            if x.strip()
        )

    reviews = build_review_template(
        seed,
        extra_series=sorted(extra),
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    reviews.to_csv(args.out, index=False)

    n_transition = int(
        seed["needs_stable_window_transition_review"].sum()
    )

    print("FRED-MD review queue created")
    print(f"seed                  : {args.seed}")
    print(f"reviews               : {args.out}")
    print(f"transition rows       : {n_transition}")
    print(f"total review rows     : {len(reviews)}")
    print()
    print(
        "Blank cells mean 'no decision yet'. Edit only the reviews file; "
        "do not hand-edit the generated seed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
