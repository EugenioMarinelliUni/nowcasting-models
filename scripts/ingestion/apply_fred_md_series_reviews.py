#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_registry_reviews import (
    apply_review_overlay,
    build_review_summary,
    read_reviews_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Overlay human FRED-MD review decisions onto a generated "
            "registry seed and create the curated registry."
        )
    )
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Allow regeneration of the curated registry from seed + reviews."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary_out = args.summary_out or args.out.with_name(
        f"{args.out.stem}__review_summary.csv"
    )

    protected = [p for p in (args.out, summary_out) if p.exists()]
    if protected and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite generated curated artifacts: "
            + ", ".join(str(p) for p in protected)
            + ". Use --overwrite to regenerate them from seed + reviews."
        )

    seed = pd.read_csv(args.seed)
    reviews = read_reviews_csv(args.reviews)

    registry = apply_review_overlay(seed, reviews)
    summary = build_review_summary(registry)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    registry.to_csv(args.out, index=False)
    summary.to_csv(summary_out, index=False)

    print("FRED-MD review overlay applied")
    print(f"seed                  : {args.seed}")
    print(f"reviews               : {args.reviews}")
    print(f"registry              : {args.out}")
    print(f"summary               : {summary_out}")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
