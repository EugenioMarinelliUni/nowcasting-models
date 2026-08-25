#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from dfm_pipeline.ingestion.fred_md_registry import (
    build_registry_seed_from_audit_dir,
    build_registry_summary,
)


DEFAULT_OUT = Path(
    "data/metadata/series_maps/fred_md_series_registry.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an initial, reviewable FRED-MD series registry from "
            "the vintage-audit artifacts. Existing registries are protected "
            "unless --overwrite is supplied."
        )
    )
    parser.add_argument(
        "--audit-dir",
        type=Path,
        required=True,
        help=(
            "Directory containing series_tcode_history.csv, "
            "series_presence.csv, series_changes.csv and series_audit.csv."
        ),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Registry output path (default: {DEFAULT_OUT}).",
    )
    parser.add_argument(
        "--stable-start",
        required=True,
        help=(
            "First vintage used for the strict raw-stable candidate flag "
            "(YYYY-MM)."
        ),
    )
    parser.add_argument(
        "--stable-end",
        required=True,
        help=(
            "Last vintage used for the strict raw-stable candidate flag "
            "(YYYY-MM)."
        ),
    )
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help=(
            "Optional generated QC summary path. Default: "
            "<registry stem>__summary.csv."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Allow replacement of an existing registry. Use only before "
            "manual documentary review begins."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    out = args.out
    summary_out = args.summary_out or out.with_name(
        f"{out.stem}__summary.csv"
    )

    protected = [p for p in (out, summary_out) if p.exists()]
    if protected and not args.overwrite:
        paths = ", ".join(str(p) for p in protected)
        raise SystemExit(
            "Refusing to overwrite existing registry artifacts: "
            f"{paths}. Use --overwrite only if replacement is intentional."
        )

    registry = build_registry_seed_from_audit_dir(
        args.audit_dir,
        stable_start=args.stable_start,
        stable_end=args.stable_end,
    )
    summary = build_registry_summary(registry)

    out.parent.mkdir(parents=True, exist_ok=True)
    registry.to_csv(out, index=False)
    summary.to_csv(summary_out, index=False)

    print("FRED-MD registry seed created")
    print(f"registry              : {out}")
    print(f"summary               : {summary_out}")
    print(f"raw_series            : {len(registry)}")
    print(
        "rt_stable_candidates  : "
        f"{int(registry['candidate_rt_stable'].sum())}"
    )
    print(
        "transition_review      : "
        f"{int(registry['needs_stable_window_transition_review'].sum())}"
    )
    print(
        "tcode_changes          : "
        f"{int((registry['n_tcode_changes'] > 0).sum())}"
    )
    print()
    print(
        "Semantic/documentary fields remain explicitly unreviewed. "
        "Do not overwrite this file after manual review begins."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
