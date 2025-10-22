#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse

from dfm_pipeline.utils import load_panel_csv, ensure_monthly_index, clean_columns
from dfm_pipeline.validation.stationarity import (
    run_stationarity_tests_on_panel,
    HAVE_ARCH,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="ADF+KPSS stationarity check for monthly panel(s) with optional extras"
    )
    ap.add_argument("csvs", nargs="+", type=Path, help="Panel CSV paths")
    ap.add_argument("--date-fmt", default="%m/%d/%Y",
                    help="Date format for load_panel_csv (ignored if loader auto-detects).")
    ap.add_argument("--out-dir", type=Path, default=Path("data/quality_checks/stationarity"))
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--kpss-reg", default="c", choices=["c", "ct"],
                    help="KPSS regression: level (c) or level+trend (ct). Default: c")

    # Optional extras (need 'arch')
    ap.add_argument("--pp", action="store_true", help="Add Phillips–Perron test (requires 'arch')")
    ap.add_argument("--dfgls", action="store_true", help="Add DF-GLS (ERS) test (requires 'arch')")
    ap.add_argument("--za", action="store_true", help="Add Zivot–Andrews break test (requires 'arch')")
    ap.add_argument("--pp-trend", default="c", choices=["n", "c", "ct"],
                    help="Trend for PP test (if enabled). Default: c")
    ap.add_argument("--dfgls-trend", default="c", choices=["c", "ct"],
                    help="Trend for DF-GLS test (if enabled). Default: c")
    ap.add_argument("--za-reg", default="c", choices=["c", "t", "ct"],
                    help="Regression for Zivot–Andrews test (if enabled). Default: c")
    args = ap.parse_args()

    # Gentle warning if extras requested but arch is missing
    if (args.pp or args.dfgls or args.za) and not HAVE_ARCH:
        print("[warn] 'arch' package not available; --pp/--dfgls/--za will be skipped. "
              "Install with: pip install arch")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for p in args.csvs:
        # Your utils should: parse the date col (sasdate/Date), set monthly DatetimeIndex,
        # keep only numeric columns in the frame or leave that to the stationarity module.
        df = load_panel_csv(p, date_fmt=args.date_fmt)
        df = ensure_monthly_index(df)
        df = clean_columns(df)

        res = run_stationarity_tests_on_panel(
            df,
            kpss_reg=args.kpss_reg,
            alpha=args.alpha,
            run_pp=args.pp,
            run_dfgls=args.dfgls,
            run_za=args.za,
        )

        out = args.out_dir / f"{p.stem}_stationarity.csv"
        res.to_csv(out, float_format="%.6g")

        counts = res["decision"].value_counts(dropna=False).to_dict()
        print(f"[{p.name}] rows={len(df):,} cols={df.shape[1]}  decisions={counts}  -> {out}")


if __name__ == "__main__":
    main()

