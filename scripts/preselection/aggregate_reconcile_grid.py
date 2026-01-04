#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd


def aggregate_reconcile_tables(csv_paths: List[str]) -> pd.DataFrame:
    """
    Aggregate multiple *_reconciled_table.csv files produced by
    run_reconcile_methods.py.

    For each variable:
      - count in how many files it appears in_A, in_B, in_C
      - compute fractions (counts / n_files where variable appears)
    """
    stats: Dict[str, Dict[str, Any]] = {}

    for path in csv_paths:
        df = pd.read_csv(path)
        if "variable" not in df.columns:
            raise ValueError(f"{path} missing 'variable' column.")
        # ensure flags exist
        for col in ["in_A", "in_B", "in_C"]:
            if col not in df.columns:
                df[col] = False

        for _, row in df.iterrows():
            var = str(row["variable"])
            group = row.get("group", "unknown")

            if var not in stats:
                stats[var] = {
                    "variable": var,
                    "group": group,
                    "n_files": 0,
                    "count_in_A": 0,
                    "count_in_B": 0,
                    "count_in_C": 0,
                }

            s = stats[var]
            s["n_files"] += 1
            if bool(row["in_A"]):
                s["count_in_A"] += 1
            if bool(row["in_B"]):
                s["count_in_B"] += 1
            if bool(row["in_C"]):
                s["count_in_C"] += 1

    if not stats:
        raise ValueError("No variables aggregated from provided CSVs.")

    # Build DataFrame
    rows = []
    for _, s in stats.items():
        n = float(s["n_files"])
        rows.append(
            {
                "variable": s["variable"],
                "group": s["group"],
                "n_files": int(s["n_files"]),
                "count_in_A": int(s["count_in_A"]),
                "count_in_B": int(s["count_in_B"]),
                "count_in_C": int(s["count_in_C"]),
                "frac_in_A": s["count_in_A"] / n if n > 0 else 0.0,
                "frac_in_B": s["count_in_B"] / n if n > 0 else 0.0,
                "frac_in_C": s["count_in_C"] / n if n > 0 else 0.0,
            }
        )

    out_df = pd.DataFrame(rows)
    # sort by frac_in_C descending, then by frac_in_B/A
    out_df = out_df.sort_values(
        ["frac_in_C", "frac_in_B", "frac_in_A", "variable"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    return out_df


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Aggregate multiple reconciled_*_reconciled_table.csv files and "
            "compute selection frequencies for A/B/C across the grid."
        )
    )
    ap.add_argument(
        "--glob-pattern",
        required=True,
        help=(
            "Glob for reconciled tables, e.g. "
            "'data/metadata/variants_TEST_v3/reconciled_1960_noVIX_TEST_1990_2019*_reconciled_table.csv'"
        ),
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output CSV with aggregated counts and fractions.",
    )

    args = ap.parse_args()

    paths = sorted(glob.glob(args.glob_pattern))
    if not paths:
        raise SystemExit(f"No files match glob pattern: {args.glob_pattern!r}")

    df = aggregate_reconcile_tables(paths)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[aggregate_reconcile] aggregated {len(paths)} files -> {out_path}")


if __name__ == "__main__":
    main()
