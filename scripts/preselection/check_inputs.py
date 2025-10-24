#!/usr/bin/env python3
from __future__ import annotations
import argparse, glob, os, sys, re
import pandas as pd
import numpy as np
from pathlib import Path

def read_panel_csv(p: str | Path) -> pd.DataFrame:
    df = pd.read_csv(p, parse_dates=["Date"])
    if "Date" not in df.columns:
        raise ValueError(f"'Date' column missing in {p}")
    return df.set_index("Date").sort_index()

def infer_years_from_tag(tag: str) -> tuple[int,int] | None:
    m = re.match(r"^train(\d{4})_(\d{4})$", tag)
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def main():
    ap = argparse.ArgumentParser("Verify X (training_sets) and y (baseline) inputs for all tags.")
    ap.add_argument("--panels", nargs="*", default=["1960_noVIX","1965_withVIX"])
    ap.add_argument("--strict-qmonths", action="store_true",
                    help="Fail if y non-NaN months are not exactly {1,4,7,10}")
    ap.add_argument("--check-x-zscore", action="store_true",
                    help="Warn (or fail if --fail-on-zscore) if X not ~N(0,1) on the train index")
    ap.add_argument("--z-mean-tol", type=float, default=0.15, help="|mean| tolerance (default 0.15)")
    ap.add_argument("--z-std-tol", type=float, default=0.15, help="|std-1| tolerance (default 0.15)")
    ap.add_argument("--fail-on-zscore", action="store_true", help="Treat z-score sanity warnings as failures")
    args = ap.parse_args()

    problems = 0
    for panel in args.panels:
        print(f"=== {panel} ===")
        for d in sorted(glob.glob(f"dataset/{panel}/training_sets/*")):
            if not os.path.isdir(d):
                continue
            tag = os.path.basename(d)  # e.g., train1965_2015
            x_path = f"{d}/standardized_train__{panel}__{tag}.csv"
            y_path = f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv"

            # 1) Existence
            missing = []
            if not os.path.exists(x_path): missing.append(f"X:{x_path}")
            if not os.path.exists(y_path): missing.append(f"y:{y_path}")
            if missing:
                print(f"[MISS] {panel} {tag} -> {'; '.join(missing)}")
                problems += 1
                continue

            # 2) Read
            try:
                X = read_panel_csv(x_path)
                y = read_panel_csv(y_path).iloc[:,0].astype(float)
            except Exception as e:
                print(f"[ERR ] {panel} {tag}: read/parsing failed: {e}")
                problems += 1
                continue

            # 3) Index alignment
            if not X.index.equals(y.index):
                print(f"[FAIL] {panel} {tag}: index mismatch.\n"
                      f"       X: {X.index.min()} → {X.index.max()} (n={len(X)})\n"
                      f"       y: {y.index.min()} → {y.index.max()} (n={len(y)})")
                problems += 1
                continue

            # 4) y availability & quarter months
            y_non_na = int(y.notna().sum())
            qm = sorted(set(y.dropna().index.month))
            if y_non_na == 0:
                print(f"[FAIL] {panel} {tag}: y has no non-NaN values in the train index.")
                problems += 1
                continue
            if args.strict_qmonths and qm != [1,4,7,10]:
                print(f"[FAIL] {panel} {tag}: y quarter months {qm} != [1,4,7,10] (strict)")
                problems += 1

            # 5) Tag vs date sanity (optional but useful)
            yrs = infer_years_from_tag(tag)
            if yrs:
                y0, y1 = yrs
                # Expect index from Feb(y0) to Dec(y1) if MS dating; we just check years
                ix_years = (X.index.min().year, X.index.max().year)
                if not (ix_years[0] == y0 and ix_years[1] == y1):
                    print(f"[WARN] {panel} {tag}: index years {ix_years} differ from tag ({y0},{y1})")

            # 6) Optional: z-score sanity on X across the full train index
            if args.check_x_zscore:
                mu = X.mean(numeric_only=True)
                sd = X.std(ddof=0, numeric_only=True)
                bad_mu = int((mu.abs() > args.z_mean_tol).sum())
                bad_sd = int(((sd - 1).abs() > args.z_std_tol).sum())
                if bad_mu or bad_sd:
                    msg = (f"[WARN] {panel} {tag}: X not ~N(0,1). "
                           f"bad_means={bad_mu} (>{args.z_mean_tol}), "
                           f"bad_stds={bad_sd} (>{args.z_std_tol})")
                    if args.fail_on_zscore:
                        print(msg.replace("[WARN]","[FAIL]"))
                        problems += 1
                    else:
                        print(msg)

            print(f"[OK  ] {panel} {tag}: X{X.shape}  y_nonNA={y_non_na}  qmonths={qm}")

    if problems:
        print(f"\nChecks completed with {problems} problem(s).")
        sys.exit(1)
    print("\nAll checks passed.")
    sys.exit(0)

if __name__ == "__main__":
    main()
