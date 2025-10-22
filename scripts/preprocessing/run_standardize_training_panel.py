#!/usr/bin/env python3
from __future__ import annotations

import argparse, sys
from pathlib import Path
import pandas as pd

# add src to path when running from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.preprocessing.fixed_window_standardize import standardize_panel_on_window  # noqa: E402

def load_stationary(csv_path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    """Load a stationarized monthly panel; set DatetimeIndex; keep numeric columns only."""
    df = pd.read_csv(csv_path, low_memory=False)
    if date_col not in df.columns:
        # fallback: try first column as dates
        first = df.columns[0]
        if pd.to_datetime(df[first], errors="coerce").notna().mean() > 0.9:
            date_col = first
        else:
            raise ValueError(f"Date column '{date_col}' not found in {csv_path}. "
                             f"Available: {list(df.columns)[:8]}")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    return df.select_dtypes(include="number")

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Standardize (z-score) a stationarized panel on a chosen training window."
    )
    ap.add_argument("--csv", required=True,
                    help="Stationarized panel CSV (NOT standardized), e.g., panel_start_1965_..._delta.csv")
    ap.add_argument("--date-col", default="sasdate", help="Date column name (default: sasdate)")
    ap.add_argument("--start", required=True, help="Training start YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="Training end YYYY-MM-DD")
    ap.add_argument("--monthly-freq", default="MS", choices=["MS","ME"],
                    help="Normalize index to month-start (MS) or month-end (ME).")
    ap.add_argument("--min-obs-per-col", type=int, default=1,
                    help="Drop columns with fewer than this many non-missing obs in the window.")
    ap.add_argument("--panel", required=True, choices=["1960_noVIX","1965_withVIX"],
                    help="Panel ID for output placement under dataset/{panel}/training_sets/{tag}/")
    ap.add_argument("--tag", required=True,
                    help="Training tag, e.g., train1965_2015 or train1990_2019")
    ap.add_argument("--out", default=None,
                    help="Optional explicit output CSV. If omitted, auto path is used.")
    ap.add_argument("--save-stats", action="store_true",
                    help="Also save μ,σ,nobs CSV next to the output.")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    X = load_stationary(Path(args.csv), date_col=args.date_col)

    Z, stats = standardize_panel_on_window(
        X,
        start=args.start,
        end=args.end,
        monthly_freq=args.monthly_freq,
        min_obs_per_col=int(args.min_obs_per_col),
    )

    # Decide output path
    if args.out:
        outp = Path(args.out)
    else:
        out_dir = Path("dataset") / args.panel / "training_sets" / args.tag
        out_dir.mkdir(parents=True, exist_ok=True)
        outp = out_dir / f"standardized_train__{args.panel}__{args.tag}.csv"

    outp.parent.mkdir(parents=True, exist_ok=True)
    Z.to_csv(outp, index_label="Date")
    print(f"[OK] wrote standardized training panel: {outp}  shape={Z.shape}")

    if args.save_stats:
        st_path = outp.with_suffix("").with_name(outp.stem + "__train_stats.csv")
        pd.DataFrame({"mean": stats.mean, "std": stats.std, "nobs": stats.nobs}).to_csv(st_path, index_label="series")
        print(f"[OK] wrote train stats: {st_path}")

if __name__ == "__main__":
    main()
