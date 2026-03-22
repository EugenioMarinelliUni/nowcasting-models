from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from dfm_pipeline.eval_pseudort.baselines import evaluate_miq_csv


def _find_miq_csv(run_dir: str) -> str:
    pats = [
        str(Path(run_dir) / "bm_pseudort_fast_miq__*.csv"),
        str(Path(run_dir) / "**" / "bm_pseudort_fast_miq__*.csv"),
    ]
    matches = []
    for pat in pats:
        matches.extend(glob.glob(pat, recursive=True))
    matches = sorted(matches)
    if not matches:
        raise SystemExit(f"No MIQ CSV found under {run_dir}")
    return matches[-1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_dir", required=True, help="Directory containing bm_pseudort_fast_miq__*.csv")
    ap.add_argument("--min_ar1_obs", type=int, default=8)
    ap.add_argument("--write_csv", default="", help="Optional output CSV path for a 1-row summary.")
    args = ap.parse_args()

    miq_csv = _find_miq_csv(args.run_dir)
    rep = evaluate_miq_csv(miq_csv, min_ar1_obs=int(args.min_ar1_obs))

    rows = [{
        "miq_csv": rep.miq_csv,
        "n_quarters": rep.n_quarters,
        "dfm_rmse_m1": rep.dfm_rmse_m1,
        "dfm_rmse_m2": rep.dfm_rmse_m2,
        "dfm_rmse_m3": rep.dfm_rmse_m3,
        "dfm_da_m3": rep.dfm_da_m3,
        "nc_rmse": rep.nc_rmse,
        "nc_da": rep.nc_da,
        "ar1_rmse": rep.ar1_rmse,
        "ar1_da": rep.ar1_da,
    }]
    out = pd.DataFrame(rows)

    # stdout
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(out.to_string(index=False))

    if args.write_csv:
        Path(args.write_csv).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.write_csv, index=False)


if __name__ == "__main__":
    main()