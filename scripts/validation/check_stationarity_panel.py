#!/usr/bin/env python3
from pathlib import Path
import argparse

from dfm_pipeline.utils import load_panel_csv, ensure_monthly_index, clean_columns
from dfm_pipeline.validation.stationarity import panel_stationarity


def main():
    ap = argparse.ArgumentParser(description="ADF+KPSS stationarity check for monthly panel(s) with optional extras")
    ap.add_argument("csvs", nargs="+", type=Path, help="Panel CSV paths")
    ap.add_argument("--date-fmt", default="%m/%d/%Y")
    ap.add_argument("--out-dir", type=Path, default=Path("data/quality_checks/stationarity"))
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--kpss-reg", default="c", choices=["c", "ct"], help="KPSS regression: level (c) or trend (ct)")
    # Extras
    ap.add_argument("--pp", action="store_true", help="Add Phillips–Perron test (requires 'arch')")
    ap.add_argument("--dfgls", action="store_true", help="Add DF-GLS (ERS) test (requires 'arch')")
    ap.add_argument("--za", action="store_true", help="Add Zivot–Andrews break test")
    ap.add_argument("--pp-trend", default="c", choices=["n","c","ct"])
    ap.add_argument("--dfgls-trend", default="c", choices=["c","ct"])
    ap.add_argument("--za-reg", default="c", choices=["c","t","ct"])
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for p in args.csvs:
        df = load_panel_csv(p, date_fmt=args.date_fmt)
        df = ensure_monthly_index(df)
        df = clean_columns(df)

        res = panel_stationarity(
            df,
            kpss_reg=args.kpss_reg,
            alpha=args.alpha,
            run_pp=args.pp,
            run_dfgls=args.dfgls,
            run_za=args.za,
            pp_trend=args.pp_trend,
            dfgls_trend=args.dfgls_trend,
            za_reg=args.za_reg,
        )
        out = args.out_dir / f"{p.stem}_stationarity.csv"
        res.to_csv(out, float_format="%.6g")

        counts = res["decision"].value_counts(dropna=False).to_dict()
        print(f"[{p.name}] rows={len(df):,} cols={df.shape[1]}  decisions={counts}  -> {out}")


if __name__ == "__main__":
    main()
