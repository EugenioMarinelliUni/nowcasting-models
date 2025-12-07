#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

# Ensure src/ is on path if needed
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Subset a panel CSV using a JSON list of selected variables, "
            "preserving the date column. Intended for FULL standardized panels."
        )
    )
    ap.add_argument(
        "--panel-csv",
        required=True,
        help="Input panel CSV with a date column (e.g. sasdate/date).",
    )
    ap.add_argument(
        "--selected-json",
        required=True,
        help="JSON file with list of selected variables (e.g. direct_*__selected.json).",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output path for reduced panel CSV.",
    )
    ap.add_argument(
        "--keep-extra-cols",
        nargs="*",
        default=[],
        help="Optional extra columns to keep (e.g. masks), in addition to date + selected vars.",
    )
    return ap.parse_args()


def load_selected_vars(path: Path) -> list[str]:
    with path.open("r") as f:
        obj = json.load(f)

    # Common patterns for direct_*__selected.json
    if isinstance(obj, list):
        return [str(x) for x in obj]

    if isinstance(obj, dict):
        for key in ("selected_vars", "selected", "variables"):
            if key in obj and isinstance(obj[key], list):
                return [str(x) for x in obj[key]]

    raise ValueError(
        f"Cannot infer selected variable list from JSON {path} "
        f"(type={type(obj)}, keys={list(obj) if isinstance(obj, dict) else 'n/a'})"
    )


def main() -> None:
    args = parse_args()

    panel_path = Path(args.panel_csv)
    sel_path = Path(args.selected_json)
    out_path = Path(args.out_csv)

    if not panel_path.exists():
        raise FileNotFoundError(f"Panel CSV not found: {panel_path}")
    if not sel_path.exists():
        raise FileNotFoundError(f"Selected-vars JSON not found: {sel_path}")

    df = pd.read_csv(panel_path)

    # Detect date column
    candidate_date_cols = ["sasdate", "DATE", "Date", "date"]
    date_col = None
    for c in candidate_date_cols:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError(
            f"No date column found in {panel_path}; "
            f"looked for {candidate_date_cols}. "
            f"Columns={df.columns.tolist()[:20]}"
        )

    selected_vars = load_selected_vars(sel_path)

    # Filter selected vars to those actually present
    keep_vars = [v for v in selected_vars if v in df.columns]
    if not keep_vars:
        raise ValueError(
            "No overlap between selected vars and panel columns.\n"
            f"Example panel columns: {df.columns[:10].tolist()}\n"
            f"Selected vars (first 10): {selected_vars[:10]}"
        )

    extra_cols = [c for c in args.keep_extra_cols if c in df.columns]

    # Final column order: date, extra, selected
    cols: list[str] = [date_col] + extra_cols
    for v in keep_vars:
        if v not in cols:
            cols.append(v)

    df_reduced = df[cols].copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_reduced.to_csv(out_path, index=False)

    print(f"[OK] wrote reduced panel: {out_path}")
    print(f"  shape={df_reduced.shape}")
    print(f"  date range={df_reduced[date_col].min()} -> {df_reduced[date_col].max()}")


if __name__ == "__main__":
    main()
