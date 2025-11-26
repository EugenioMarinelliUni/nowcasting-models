#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

# add src to path when running from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.preprocessing.fixed_window_standardize import (  # noqa: E402
    standardize_full_panel_on_window,
)


def load_transformed_panel(csv_path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    """
    Load a stationarized/transformed monthly panel; set DatetimeIndex;
    keep numeric columns only.
    """
    df = pd.read_csv(csv_path, low_memory=False)

    # Prefer the provided date_col if present; otherwise heuristically
    # detect whether the first column is date-like.
    if date_col not in df.columns:
        first = df.columns[0]
        s_dates = pd.to_datetime(df[first], errors="coerce")
        valid_count: int = int(s_dates.notna().sum())
        total_count: int = int(len(s_dates))
        frac_valid: float = valid_count / total_count if total_count > 0 else 0.0

        if frac_valid > 0.9:
            date_col = first
        else:
            raise ValueError(
                f"Date column '{date_col}' not found in {csv_path}. "
                f"Available columns: {list(df.columns)[:8]}"
            )

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(df[date_col].name).sort_index()
    return df.select_dtypes(include="number")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Standardize a full transformed panel (stationarized, NOT standardized) "
            "using μ,σ computed on a training window, and write a full standardized "
            "panel (from train-start onward) plus optional train/OOS subpanels."
        )
    )
    ap.add_argument(
        "--csv",
        required=True,
        help="Full transformed (tcodes-applied) panel CSV, e.g. data/processed_data/stationarity/panel_....csv",
    )
    ap.add_argument(
        "--date-col",
        default="sasdate",
        help="Date column name in CSV (default: sasdate).",
    )
    ap.add_argument(
        "--train-start",
        required=True,
        help="Training window start YYYY-MM-DD (e.g. 1990-01-01).",
    )
    ap.add_argument(
        "--train-end",
        required=True,
        help="Training window end YYYY-MM-DD (e.g. 2019-12-01).",
    )
    ap.add_argument(
        "--oos-start",
        required=True,
        help="OOS window start YYYY-MM-DD (e.g. 2020-01-01).",
    )
    ap.add_argument(
        "--oos-end",
        default=None,
        help="Optional OOS window end YYYY-MM-DD. If omitted, uses max date in panel.",
    )
    ap.add_argument(
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Normalize index to month-start (MS) or month-end (ME). Default: MS.",
    )
    ap.add_argument(
        "--min-obs-per-col",
        type=int,
        default=1,
        help="Drop columns with fewer than this many non-missing obs in the training window.",
    )
    ap.add_argument(
        "--out-full",
        required=True,
        help="Output CSV for full standardized panel (from train-start to end).",
    )
    ap.add_argument(
        "--out-train",
        required=False,
        help="Optional output CSV for standardized training subpanel.",
    )
    ap.add_argument(
        "--out-oos",
        required=False,
        help="Optional output CSV for standardized OOS subpanel.",
    )
    ap.add_argument(
        "--save-stats",
        action="store_true",
        help="If set, write μ,σ,nobs CSV next to --out-full.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    csv_path = Path(args.csv)
    X_raw = load_transformed_panel(csv_path, date_col=args.date_col)

    # 1) Full standardization with frozen train scalers
    #    This computes μ,σ on [train-start, train-end] and applies them to all dates in X_raw.
    Z_full_all, stats = standardize_full_panel_on_window(
        X_raw,
        start=args.train_start,
        end=args.train_end,
        monthly_freq=args.monthly_freq,
        min_obs_per_col=int(args.min_obs_per_col),
    )

    # 2) Restrict the standardized panel to [train-start, end-of-sample]
    #    We explicitly drop any pre-1990 rows as requested.
    Z_full = Z_full_all.loc[args.train_start:].copy()

    # 3) Write full standardized panel (train + OOS, starting at train-start)
    out_full = Path(args.out_full)
    out_full.parent.mkdir(parents=True, exist_ok=True)
    df_full = Z_full.copy()
    df_full.insert(0, "date", df_full.index)
    df_full.to_csv(out_full, index=False)
    print(f"[OK] wrote full standardized panel: {out_full}  shape={df_full.shape}")

    # 4) Optional train/OOS splits based on index (within the trimmed Z_full)
    oos_end_str: str = args.oos_end or str(Z_full.index.max().date())
    train_slice = slice(args.train_start, args.train_end)
    oos_slice = slice(args.oos_start, oos_end_str)

    if args.out_train:
        Z_train = Z_full.loc[train_slice].copy()
        out_train = Path(args.out_train)
        out_train.parent.mkdir(parents=True, exist_ok=True)
        df_train = Z_train.copy()
        df_train.insert(0, "date", df_train.index)
        df_train.to_csv(out_train, index=False)
        print(f"[OK] wrote standardized training panel: {out_train}  shape={df_train.shape}")

    if args.out_oos:
        Z_oos = Z_full.loc[oos_slice].copy()
        out_oos = Path(args.out_oos)
        out_oos.parent.mkdir(parents=True, exist_ok=True)
        df_oos = Z_oos.copy()
        df_oos.insert(0, "date", df_oos.index)
        df_oos.to_csv(out_oos, index=False)
        print(f"[OK] wrote standardized OOS panel: {out_oos}  shape={df_oos.shape}")

    # 5) Optional stats (μ,σ,nobs for the training window)
    if args.save_stats:
        st_path = out_full.with_suffix("").with_name(out_full.stem + "__train_stats.csv")
        pd.DataFrame(
            {"mean": stats.mean, "std": stats.std, "nobs": stats.nobs}
        ).to_csv(st_path, index_label="series")
        print(f"[OK] wrote train stats: {st_path}")


if __name__ == "__main__":
    main()
