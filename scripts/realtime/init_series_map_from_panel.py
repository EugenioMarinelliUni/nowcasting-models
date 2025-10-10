#!/usr/bin/env python3
from pathlib import Path
import argparse, yaml
import pandas as pd

def main():
    ap = argparse.ArgumentParser(
        description="Create series_map.yaml (MNEMONIC -> FRED ID) from a panel CSV by assuming identity mapping."
    )
    ap.add_argument("panel_csv", type=Path, help="Balanced panel CSV (monthly); must have 'sasdate' date column or index.")
    ap.add_argument("--out", type=Path, default=Path("data/metadata/series_map.yaml"))
    ap.add_argument("--date-col", default="sasdate", help="Name of date column if present in the CSV header.")
    ap.add_argument("--exclude", nargs="*", default=[], help="Optional list of columns to exclude.")
    args = ap.parse_args()

    # Load header to get columns; try both 'sasdate as column' and 'sasdate as index'
    try:
        df = pd.read_csv(args.panel_csv, nrows=1)
        cols = list(df.columns)
        if args.date_col in cols:
            cols.remove(args.date_col)
    except Exception:
        # If index-only CSV, try reading index_col
        df = pd.read_csv(args.panel_csv, nrows=1, parse_dates=[args.date_col], index_col=args.date_col)
        cols = list(df.columns)

    # Exclude any requested columns
    cols = [c for c in cols if c not in set(args.exclude)]

    # Identity mapping MNEMONIC -> FRED ID (edit later if needed)
    mapping = {c: c for c in cols}

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump(mapping, sort_keys=True), encoding="utf-8")
    print(f"Wrote {args.out} with {len(mapping)} entries.")

if __name__ == "__main__":
    main()