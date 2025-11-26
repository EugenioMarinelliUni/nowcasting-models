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

from dfm_pipeline.preprocessing.target_full_standardize import (  # noqa: E402
    build_full_standardized_target,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build a full standardized monthly target series aligned to a full standardized X panel, "
            "using μ,σ computed on a training window."
        )
    )
    ap.add_argument(
        "--raw-target",
        required=True,
        help="Raw quarterly target CSV, e.g. data/raw_data/targets/gdp/A191RL1Q225SBEA_latest.csv",
    )
    ap.add_argument(
        "--x-full-panel",
        required=True,
        help="Full standardized X panel CSV with 'date' column (e.g. dataset/...__full_1990_01_2025_04.csv).",
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
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Monthly frequency for the target (MS or ME). Default: MS.",
    )
    ap.add_argument(
        "--place",
        default="start",
        choices=["start", "end"],
        help="Map quarterly observations to month at 'start' (Jan/Apr/Jul/Oct) or 'end' (Mar/Jun/Sep/Dec). Default: start.",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output CSV for full standardized monthly target aligned to X (1990..end).",
    )
    ap.add_argument(
        "--save-stats",
        action="store_true",
        help="If set, write μ,σ,nobs CSV next to --out.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    raw_target = Path(args.raw_target)
    x_full = Path(args.x_full_panel)

    yz, stats = build_full_standardized_target(
        raw_quarterly_csv=raw_target,
        x_full_panel_csv=x_full,
        train_start=args.train_start,
        train_end=args.train_end,
        monthly_freq=args.monthly_freq,
        place=args.place,
    )

    # Write standardized target: index = date, col = 'y'
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    df_y = yz.to_frame("y").copy()
    df_y.insert(0, "date", df_y.index)
    df_y.to_csv(out, index=False)
    print(f"[OK] wrote standardized target panel: {out}  shape={df_y.shape}")

    if args.save_stats:
        st_path = out.with_suffix("").with_name(out.stem + "__train_stats.csv")
        pd.DataFrame(
            {"mean": [stats.mean], "std": [stats.std], "nobs": [stats.nobs]},
            index=[f"{stats.start.date()}..{stats.end.date()}"],
        ).to_csv(st_path, index_label="train_window")
        print(f"[OK] wrote target train stats: {st_path}")


if __name__ == "__main__":
    main()
