from __future__ import annotations

import argparse

from dfm_pipeline.preprocessing.bm_inputs import build_bm_inputs_from_raw


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build BM-DFM inputs in BOTH modes: raw (unstandardized) and frozen-window standardized."
    )
    ap.add_argument("--panel-csv", required=True, help="Transformed (tcode) panel CSV, not standardized.")
    ap.add_argument("--quarterly-target-csv", required=True, help="Quarterly target CSV (e.g., GDP).")
    ap.add_argument("--date-col-panel", default="sasdate", help="Date column name in panel CSV.")
    ap.add_argument("--date-col-target", default="sasdate", help="Date column name in target CSV.")
    ap.add_argument("--target-col", required=True, help="Target column name in quarterly target CSV.")
    ap.add_argument("--outdir", required=True, help="Output directory for BM files.")
    ap.add_argument("--train-start", required=True, help="Start date for frozen scalers (YYYY-MM-DD).")
    ap.add_argument("--train-end", required=True, help="End date for frozen scalers (YYYY-MM-DD).")
    ap.add_argument(
        "--date-col-out",
        default="sasdate",
        help="Date column name to write in outputs (default: sasdate).",
    )
    ap.add_argument(
        "--target-anchor",
        default="quarter_end_month",
        choices=["quarter_end_month", "quarter_start_month"],
        help="Where to place quarterly values on the monthly grid (default: quarter_end_month).",
    )

    args = ap.parse_args()

    paths = build_bm_inputs_from_raw(
        panel_csv=args.panel_csv,
        quarterly_target_csv=args.quarterly_target_csv,
        date_col_panel=args.date_col_panel,
        date_col_target=args.date_col_target,
        target_col=args.target_col,
        outdir=args.outdir,
        train_start=args.train_start,
        train_end=args.train_end,
        date_col_out=args.date_col_out,
        target_anchor=args.target_anchor,
    )

    print(paths.x_raw_csv)
    print(paths.y_raw_monthly_csv)
    print(paths.x_z_csv)
    print(paths.y_z_monthly_csv)
    print(paths.scalers_json)


if __name__ == "__main__":
    main()