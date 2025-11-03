#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from dfm_pipeline.preselection.analysis.compare import (
    selections_from_json_files,
    compare_selected_sets,
    save_comparison_outputs,
)


def _default_json_path(panel: str, tag: str, method: str) -> Path:
    # Matches the JSON layout produced by your preselection runner
    return Path(f"data/metadata/variants/{panel}__{tag}__preselect_{method}.json")


def _default_out_prefix(panel: str, tag: str, methods: list[str], label: str | None) -> Path:
    """
    More specific, human-friendly default location:
      data/metadata/variants/comparisons/{panel}/{tag}/{methods_joined}/compare__{panel}__{tag}
    Optionally append a user label to the methods folder name.
    """
    methods_joined = "__".join(methods)
    if label:
        methods_joined = f"{methods_joined}__{label}"
    out_dir = Path("data/metadata/variants/comparisons") / panel / tag / methods_joined
    return out_dir / f"compare__{panel}__{tag}"


def main():
    ap = argparse.ArgumentParser(description="Compare variable selections across methods.")
    ap.add_argument("--panel", required=True, help="e.g., 1965_withVIX")
    ap.add_argument("--tag", required=True, help="e.g., train1990_2019")
    ap.add_argument("--methods", nargs="+", required=True, help="e.g., sis tstat lars")
    ap.add_argument(
        "--json",
        nargs="*",
        default=None,
        help=(
            "Optional explicit JSON paths, same length/order as --methods. "
            "If omitted, standard paths are used based on --panel/--tag."
        ),
    )
    ap.add_argument(
        "--out-prefix",
        default=None,
        help="Custom prefix for output CSVs. If omitted, a structured default is used.",
    )
    ap.add_argument(
        "--label",
        default=None,
        help="Optional label appended to the methods folder (e.g., 'alpha005' or 'dedup098').",
    )
    args = ap.parse_args()

    panel, tag = args.panel, args.tag
    methods = args.methods

    # Resolve JSON artifact paths
    if args.json:
        if len(args.json) != len(methods):
            raise SystemExit("ERROR: If --json is provided, it must have the same number of items as --methods.")
        named_paths: Dict[str, str] = {m: p for (m, p) in zip(methods, args.json)}
    else:
        named_paths = {m: str(_default_json_path(panel, tag, m)) for m in methods}

    # ----- Guard 1: verify that all files exist -----
    missing = [str(p) for p in named_paths.values() if not Path(p).is_file()]
    if missing:
        raise SystemExit("ERROR: Missing JSON artifact(s):\n  " + "\n  ".join(missing))

    # Load selections
    selections = selections_from_json_files(named_paths)

    # ----- Guard 2: ensure each selection is non-empty -----
    empty = [name for name, s in selections.items() if not s]
    if empty:
        raise SystemExit(
            "ERROR: Empty 'selected' list in the following method(s): "
            + ", ".join(empty)
            + "\nMake sure those preselection runs completed and wrote non-empty selections."
        )

    # Compare
    comp = compare_selected_sets(selections)

    # Output prefix: custom or structured default
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = _default_out_prefix(panel, tag, methods, args.label)

    written = save_comparison_outputs(comp, out_prefix)

    # Console summary
    print("=== Comparison summary ===")
    for m, k in comp.sizes.items():
        print(f"{m:>12}: {k}")
    print(f"{'union size':>12}: {comp.union_size}")
    print(f"{'intersection':>12}: {comp.intersection_size}")
    print("written files:")
    for k, p in written.items():
        print(f"  {k:>12}: {p}")


if __name__ == "__main__":
    main()
