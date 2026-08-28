#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical import (
    audit_rt_canonical_transitions,
    build_rt_canonical_specification,
    build_rt_canonical_summary,
)

ENV_RAW_DIR = "FRED_MD_VINTAGE_RAW_DIR"
DEFAULT_REGISTRY = Path("data/metadata/series_maps/fred_md_series_registry.csv")
DEFAULT_RT_STABLE = Path("data/metadata/series_maps/fred_md_panel_rt_stable_2010_2026.csv")
DEFAULT_OUT = Path("data/metadata/series_maps/fred_md_panel_rt_canonical_2010_2026__candidate.csv")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build the candidate FRED-MD RT_CANONICAL concept specification and source map. "
            "Optionally run an empirical transition audit against historical vintages. "
            "No forecast-performance information is used."
        )
    )
    p.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    p.add_argument("--rt-stable", type=Path, default=DEFAULT_RT_STABLE)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--source-map-out", type=Path, default=None)
    p.add_argument("--summary-out", type=Path, default=None)
    p.add_argument("--raw-dir", type=Path, default=None, help="If supplied, also run the empirical transition audit.")
    p.add_argument("--start", default="2010-01")
    p.add_argument("--end", default="2026-06")
    p.add_argument("--audit-out", type=Path, default=None)
    p.add_argument("--recursive", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--fail-on-audit-hard", action="store_true")
    return p.parse_args()


def _read(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"{label} not found: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _resolve_raw_dir(args: argparse.Namespace) -> Path | None:
    if args.raw_dir is not None:
        return args.raw_dir.expanduser().resolve()
    env = os.environ.get(ENV_RAW_DIR)
    return Path(env).expanduser().resolve() if env else None


def main() -> int:
    args = parse_args()
    source_map_out = args.source_map_out or args.out.with_name(f"{args.out.stem}__source_map.csv")
    summary_out = args.summary_out or args.out.with_name(f"{args.out.stem}__summary.csv")

    raw_dir = _resolve_raw_dir(args)
    audit_out = args.audit_out or args.out.with_name("fred_md_rt_canonical_transition_audit_2010_2026.csv")
    audit_by_vintage = audit_out.with_name(f"{audit_out.stem}__by_vintage.csv")
    audit_boundaries = audit_out.with_name(f"{audit_out.stem}__boundaries.csv")
    audit_summary = audit_out.with_name(f"{audit_out.stem}__summary.csv")

    outputs = [args.out, source_map_out, summary_out]
    if raw_dir is not None:
        outputs += [audit_out, audit_by_vintage, audit_boundaries, audit_summary]
    existing = [x for x in outputs if x.exists()]
    if existing and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing RT_CANONICAL artifacts: "
            + ", ".join(str(x) for x in existing)
            + ". Use --overwrite to regenerate."
        )

    registry = _read(args.registry, "Registry")
    rt_stable = _read(args.rt_stable, "RT_STABLE")
    panel, source_map = build_rt_canonical_specification(registry, rt_stable)
    summary = build_rt_canonical_summary(panel, source_map)

    for path in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(args.out, index=False)
    source_map.to_csv(source_map_out, index=False)
    summary.to_csv(summary_out, index=False)

    print("FRED-MD RT_CANONICAL candidate specification built")
    print(f"registry              : {args.registry}")
    print(f"rt_stable             : {args.rt_stable}")
    print(f"panel                 : {args.out}")
    print(f"source_map            : {source_map_out}")
    print(f"summary               : {summary_out}")
    print()
    print(summary.to_string(index=False))

    switched = panel.loc[panel["mapping_type"].eq("switch_by_vintage")]
    print("\nSwitch-by-vintage concepts\n--------------------------")
    print("None" if switched.empty else switched[["panel_position", "canonical_id", "n_source_segments"]].to_string(index=False))

    if raw_dir is None:
        print("\nNOTE: candidate specification only. Run again with --raw-dir to perform the empirical transition audit before freezing RT_CANONICAL.")
        return 0

    by_vintage, by_concept, boundaries, a_summary = audit_rt_canonical_transitions(
        raw_dir,
        panel,
        source_map,
        start=args.start,
        end=args.end,
        recursive=args.recursive,
    )
    by_concept.to_csv(audit_out, index=False)
    by_vintage.to_csv(audit_by_vintage, index=False)
    boundaries.to_csv(audit_boundaries, index=False)
    a_summary.to_csv(audit_summary, index=False)

    print("\nFRED-MD RT_CANONICAL empirical transition audit completed")
    print(f"raw_dir               : {raw_dir}")
    print(f"concept_report        : {audit_out}")
    print(f"by_vintage_report     : {audit_by_vintage}")
    print(f"boundaries            : {audit_boundaries}")
    print(f"audit_summary         : {audit_summary}")
    print()
    print(a_summary.to_string(index=False))

    hard = by_concept.loc[by_concept["canonical_audit_status"].eq("hard_fail")]
    print("\nCanonical transition hard failures\n----------------------------------")
    print("None" if hard.empty else hard[["canonical_id", "n_vintages_hard_fail", "hard_failure_reasons"]].to_string(index=False))

    if args.fail_on_audit_hard and int(a_summary["n_concepts_hard_fail"].iloc[0]) > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
