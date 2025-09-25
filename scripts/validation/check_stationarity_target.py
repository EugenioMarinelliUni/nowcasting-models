#!/usr/bin/env python3
from pathlib import Path
import argparse
import sys
import pandas as pd

# uses your existing utilities
from dfm_pipeline.validation.stationarity import run_stationarity_tests_on_series


def main():
    ap = argparse.ArgumentParser(
        description="Run stationarity tests on a single target series CSV."
    )
    ap.add_argument("csv", type=Path, help="Path to target CSV (with date and value columns).")
    ap.add_argument("--date-col", default="sasdate", help="Date column name (default: sasdate).")
    ap.add_argument("--value-col", default="gdp_qoq_saar", help="Value column name to test.")
    ap.add_argument("--out-dir", type=Path, default=Path("data/quality_checks/stationarity_target"),
                    help="Directory to write the summary CSV.")
    ap.add_argument("--outfile", type=Path, default=None,
                    help="Optional explicit output file path.")
    ap.add_argument("--alpha", type=float, default=0.05, help="Significance level (default: 0.05).")
    ap.add_argument("--kpss-reg", choices=["c", "ct"], default="c",
                    help="KPSS regression: c (level) or ct (trend). Default: c")
    ap.add_argument("--adf-autolag", default="AIC",
                    help="ADF autolag criterion (default: AIC).")
    ap.add_argument("--kpss-nlags", default="auto",
                    help='KPSS lag selection (default: "auto").')

    # Optional extra tests
    ap.add_argument("--pp", action="store_true", help="Also run Phillips–Perron test.")
    ap.add_argument("--dfgls", action="store_true", help="Also run DF-GLS test.")
    ap.add_argument("--za", action="store_true", help="Also run Zivot–Andrews test.")

    args = ap.parse_args()

    if not args.csv.exists():
        sys.exit(f"CSV not found: {args.csv}")

    # Load the target series
    try:
        df = pd.read_csv(args.csv, parse_dates=[args.date_col])
    except Exception as e:
        sys.exit(f"Failed to read {args.csv}: {e}")

    if args.value_col not in df.columns:
        sys.exit(f"Column '{args.value_col}' not found in {args.csv}. "
                 f"Available: {df.columns.tolist()}")

    s = (df.set_index(args.date_col)[args.value_col]).dropna()
    if s.empty:
        sys.exit("Series is empty after parsing and dropping NaNs.")

    # Run tests
    res = run_stationarity_tests_on_series(
        s,
        adf_autolag=args.adf_autolag,
        kpss_reg=args.kpss_reg,
        kpss_nlags=args.kpss_nlags,
        run_pp=args.pp,
        run_dfgls=args.dfgls,
        run_za=args.za,
        alpha=args.alpha,
    )

    # Persist summary
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.outfile or (args.out_dir / f"{args.csv.stem}_stationarity.csv")
    try:
        # res is a one-row Series; make it a DataFrame so .to_csv writes headers
        res.to_frame().T.to_csv(out, index=False, float_format="%.10g")
    except Exception as e:
        sys.exit(f"Failed to write {out}: {e}")

    # Console summary (ASCII arrow for Windows)
    decision = res.get("decision", "n/a")
    print(f"[{args.csv.name}] decision={decision} -> {out}")


if __name__ == "__main__":
    main()
