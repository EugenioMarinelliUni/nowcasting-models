#!/usr/bin/env python3
from pathlib import Path
import argparse
from dfm_pipeline.eda.zscore import zscore_panel_from_csv

def main():
    ap = argparse.ArgumentParser(description="Z-score a panel using training-window mean/std (no leakage).")
    ap.add_argument("in_csv", type=Path)
    ap.add_argument("--out-csv", type=Path, help="Output path (default: <in>_z.csv)")
    ap.add_argument("--stats-csv", type=Path, help="Optional CSV to save mean/std used")
    ap.add_argument("--date-col", default="sasdate")
    ap.add_argument("--train-start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--train-end",   required=True, help="YYYY-MM-DD")
    ap.add_argument("--date-fmt", default="%m/%d/%Y")
    ap.add_argument("--float-fmt", default="%.10g")
    ap.add_argument("--ddof", type=int, default=0, help="Std degrees of freedom (0 or 1).")
    args = ap.parse_args()

    out_csv, stats_csv = zscore_panel_from_csv(
        args.in_csv,
        date_col=args.date_col,
        train_start=args.train_start,
        train_end=args.train_end,
        out_csv=args.out_csv,
        stats_csv=args.stats_csv,
        date_fmt=args.date_fmt,
        float_fmt=args.float_fmt,
        ddof=args.ddof,
    )

    print(f"rows/cols preserved. Wrote panel → {out_csv}")
    if stats_csv:
        print(f"params (mean/std) → {stats_csv}")

if __name__ == "__main__":
    main()
