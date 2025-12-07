#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

# Ensure src/ is on path when called from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _load_forecasts_long(path: str, panel_type: str) -> pd.DataFrame:
    """
    Load a long-format forecasts CSV and tag it with a 'panel_type' flag.

    Expected columns:
      - Date
      - year
      - quarter
      - month_in_quarter
      - y_real_Q
      - y_hat
      - spec
      - q, r, p
    """
    df = pd.read_csv(path, parse_dates=["Date"])
    df["panel_type"] = panel_type
    required = {
        "Date",
        "year",
        "quarter",
        "month_in_quarter",
        "y_real_Q",
        "y_hat",
        "spec",
        "q",
        "r",
        "p",
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing columns: {sorted(missing)}")
    return df


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build a panel that, for each evaluation month, compares accuracy "
            "of full vs reduced-stability vs reduced-vote panels across all "
            "specs and identifies the best panel + (q,p,r)."
        )
    )

    ap.add_argument(
        "--full-forecasts-long-csv",
        required=True,
        help="Long forecasts CSV for the full panel.",
    )
    ap.add_argument(
        "--stab-forecasts-long-csv",
        required=True,
        help="Long forecasts CSV for the reduced stability panel.",
    )
    ap.add_argument(
        "--vote-forecasts-long-csv",
        required=True,
        help="Long forecasts CSV for the reduced vote panel.",
    )

    ap.add_argument(
        "--eval-start",
        required=True,
        help="Evaluation start date (YYYY-MM-DD), e.g. 2000-01-01.",
    )
    ap.add_argument(
        "--eval-end",
        required=True,
        help="Evaluation end date (YYYY-MM-DD), e.g. 2019-12-01.",
    )

    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output CSV path for the per-month best-panel comparison.",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    # Load the three long forecast tables
    df_full = _load_forecasts_long(args.full_forecasts_long_csv, panel_type="full")
    df_stab = _load_forecasts_long(args.stab_forecasts_long_csv, panel_type="stab")
    df_vote = _load_forecasts_long(args.vote_forecasts_long_csv, panel_type="vote")

    df_all = pd.concat([df_full, df_stab, df_vote], ignore_index=True)

    # Restrict to eval window
    eval_start = pd.to_datetime(args.eval_start)
    eval_end = pd.to_datetime(args.eval_end)
    mask = (df_all["Date"] >= eval_start) & (df_all["Date"] <= eval_end)
    df_all = df_all.loc[mask].copy()

    if df_all.empty:
        raise RuntimeError("No rows in eval window; check dates and inputs.")

    # Absolute error
    df_all["abs_err"] = (df_all["y_hat"] - df_all["y_real_Q"]).abs()

    # Global best (panel+spec) per month
    # one row per Date with minimal abs_err over all panels and specs
    idx_best = df_all.groupby("Date")["abs_err"].idxmin()
    df_best = df_all.loc[idx_best].copy()

    df_best = df_best.sort_values("Date").reset_index(drop=True)

    # Rename for clarity
    df_best = df_best.rename(
        columns={
            "panel_type": "best_panel",
            "spec": "best_spec",
            "q": "best_q",
            "r": "best_r",
            "p": "best_p",
            "abs_err": "best_abs_err",
        }
    )

    # Keep core columns
    cols_keep = [
        "Date",
        "year",
        "quarter",
        "month_in_quarter",
        "y_real_Q",
        "best_panel",
        "best_spec",
        "best_q",
        "best_r",
        "best_p",
        "best_abs_err",
    ]
    df_best = df_best[cols_keep]

    # Optional: also attach panel-wise best errors (full/stab/vote) per month
    # For each (Date, panel_type) pick min abs_err; pivot to columns
    panel_best = (
        df_all.groupby(["Date", "panel_type"])["abs_err"]
        .min()
        .reset_index()
    )
    panel_best_pivot = panel_best.pivot(
        index="Date",
        columns="panel_type",
        values="abs_err",
    )
    # Normalize column names
    panel_best_pivot = panel_best_pivot.rename(
        columns={
            "full": "best_abs_err_full_panel",
            "stab": "best_abs_err_stab_panel",
            "vote": "best_abs_err_vote_panel",
        }
    )

    # Merge into df_best
    df_best = df_best.merge(
        panel_best_pivot,
        left_on="Date",
        right_index=True,
        how="left",
    )

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_best.to_csv(out_path, index=False)

    print(
        f"[OK] wrote best-panel-per-month comparison: {out_path} "
        f"shape={df_best.shape}"
    )


if __name__ == "__main__":
    main()
