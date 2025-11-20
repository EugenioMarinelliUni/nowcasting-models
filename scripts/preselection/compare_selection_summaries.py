#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


def load_summary_strict(
    path: Path,
    label: str,
    top_k: int,
) -> pd.DataFrame:
    """
    Load one selection_summary.csv and return:

        index:  variable
        column: {label}_frac  (taken directly from 'frac_selected').

    Assumes the CSV has columns: 'variable', 'frac_selected'.
    If not, this raises loudly.
    """
    df = pd.read_csv(path)

    if "variable" not in df.columns:
        raise ValueError(f"{label}: {path} has no 'variable' column.")
    if "frac_selected" not in df.columns:
        raise ValueError(f"{label}: {path} has no 'frac_selected' column.")

    df = df[["variable", "frac_selected"]].copy()

    # Force to float; if it fails, raise (do not silently coerce to zero)
    try:
        frac = df["frac_selected"].astype(float)
    except Exception as e:
        raise ValueError(f"{label}: cannot convert 'frac_selected' to float in {path}: {e}")

    df["_frac"] = frac

    # Optional: keep only top_k by frac_selected
    df = df.sort_values("_frac", ascending=False)
    if top_k > 0:
        df = df.head(top_k)

    out = pd.DataFrame(
        {f"{label}_frac": df["_frac"].values},
        index=df["variable"],
    )
    out.index.name = "variable"
    return out


def load_group_map(path: Path) -> Dict[str, str]:
    """
    Load variable_group_map.json:
      { "RPI": "Output & Income", ... }
    """
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Group map at {path} is not a JSON object.")
    group_map: Dict[str, str] = {}
    for k, v in obj.items():
        # Ignore metadata keys, if any
        if isinstance(k, str) and k.startswith("_"):
            continue
        group_map[str(k)] = str(v)
    return group_map


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Combine SIS / tstat / LARS selection_summary.csv into one table:\n"
            "variable, group, sis_frac, tstat_frac, lars_frac"
        )
    )
    ap.add_argument(
        "--sis-summary",
        default="",
        help="Path to SIS selection_summary.csv (must have 'variable' and 'frac_selected').",
    )
    ap.add_argument(
        "--tstat-summary",
        default="",
        help="Path to tstat selection_summary.csv (must have 'variable' and 'frac_selected').",
    )
    ap.add_argument(
        "--lars-summary",
        default="",
        help="Path to LARS selection_summary.csv (must have 'variable' and 'frac_selected').",
    )
    ap.add_argument(
        "--top-k",
        type=int,
        default=0,
        help="Top variables per method to keep before merging (0 = all from that method).",
    )
    ap.add_argument(
        "--group-map-path",
        default="data/metadata/variable_group_map.json",
        help="JSON with variable -> group mapping.",
    )
    ap.add_argument(
        "--out-csv",
        default="data/metadata/variants_TEST_v2/selection_comparison_table.csv",
        help="Output CSV path.",
    )

    args = ap.parse_args()

    pieces: List[pd.DataFrame] = []

    if args.sis_summary:
        p = Path(args.sis_summary)
        if not p.exists():
            raise FileNotFoundError(f"SIS summary not found: {p}")
        pieces.append(load_summary_strict(p, "sis", args.top_k))

    if args.tstat_summary:
        p = Path(args.tstat_summary)
        if not p.exists():
            raise FileNotFoundError(f"tstat summary not found: {p}")
        pieces.append(load_summary_strict(p, "tstat", args.top_k))

    if args.lars_summary:
        p = Path(args.lars_summary)
        if not p.exists():
            raise FileNotFoundError(f"LARS summary not found: {p}")
        pieces.append(load_summary_strict(p, "lars", args.top_k))

    if not pieces:
        raise ValueError(
            "No summaries provided; pass at least one of "
            "--sis-summary / --tstat-summary / --lars-summary."
        )

    # Outer-join to keep variables that appear in ANY method
    comp = pieces[0]
    for df in pieces[1:]:
        comp = comp.join(df, how="outer")

    # Fill missing fractions with 0.0 (never selected by that method)
    for c in comp.columns:
        if c.endswith("_frac"):
            comp[c] = comp[c].fillna(0.0)

    # Attach group info
    group_map_path = Path(args.group_map_path)
    if group_map_path.exists():
        group_map = load_group_map(group_map_path)
        comp["group"] = [group_map.get(var, "UNKNOWN") for var in comp.index]
    else:
        comp["group"] = "UNKNOWN"

    # Average across available methods for sorting
    frac_cols = [c for c in comp.columns if c.endswith("_frac")]
    comp["avg_frac"] = comp[frac_cols].mean(axis=1, skipna=True)

    # Sort by avg_frac descending
    comp = comp.sort_values("avg_frac", ascending=False)

    # Final ordering: variable, group, sis_frac, tstat_frac, lars_frac (if present)
    cols_out: List[str] = ["group"]
    cols_out.extend(frac_cols)
    comp_out = comp[cols_out].copy()
    comp_out.index.name = "variable"

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    comp_out.to_csv(out_path, index_label="variable")

    print(f"[compare] Wrote comparison table to: {out_path}")
    print("[compare] Columns:", list(comp_out.columns))
    print("[compare] Number of rows:", len(comp_out))
    print("[compare] Head:")
    print(comp_out.head(20))


if __name__ == "__main__":
    main()
