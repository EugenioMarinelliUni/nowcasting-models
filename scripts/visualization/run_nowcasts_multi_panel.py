#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd

# Ensure src/ is on path when called from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.visualization.nowcast_quarter_plots import (  # noqa: E402
    plot_quarter_nowcasts_multi_panel,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Plot, for each spec, 3x3 nowcasts (3 models x 3 horizons) "
            "plus the actual value, one PNG per spec."
        )
    )

    ap.add_argument(
        "--full-long-csv",
        required=True,
        help="Long forecasts CSV for FULL panel (all specs).",
    )
    ap.add_argument(
        "--stab-long-csv",
        required=True,
        help="Long forecasts CSV for reduced STABILITY panel (all specs).",
    )
    ap.add_argument(
        "--vote-long-csv",
        required=True,
        help="Long forecasts CSV for reduced VOTE panel (all specs).",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for PNGs (one per spec).",
    )
    ap.add_argument(
        "--specs",
        nargs="*",
        default=None,
        help=(
            "Optional list of specs to plot. If omitted, use intersection "
            "of specs present in all three CSVs."
        ),
    )
    ap.add_argument(
        "--full-label",
        default="full",
        help="Model label for FULL panel (used in legend). Default: 'full'.",
    )
    ap.add_argument(
        "--stab-label",
        default="stab",
        help="Model label for stability-reduced panel. Default: 'stab'.",
    )
    ap.add_argument(
        "--vote-label",
        default="vote",
        help="Model label for vote-reduced panel. Default: 'vote'.",
    )
    return ap.parse_args()


def _load_and_tag(path: Path, model_label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Forecast long CSV not found: {path}")
    df: pd.DataFrame = pd.read_csv(path)
    df["model"] = model_label

    # infer month_in_quarter if missing
    if "month_in_quarter" not in df.columns:
        if "Date" not in df.columns:
            raise ValueError(
                f"{path} missing both 'month_in_quarter' and 'Date'; "
                "cannot infer horizon."
            )
        df["Date"] = pd.to_datetime(df["Date"])
        df["month_in_quarter"] = ((df["Date"].dt.month - 1) % 3) + 1

    required = {"spec", "year", "quarter", "month_in_quarter", "y_real_Q", "y_hat"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")

    return df


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_long_csv)
    stab_path = Path(args.stab_long_csv)
    vote_path = Path(args.vote_long_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df_full: pd.DataFrame = _load_and_tag(full_path, args.full_label)
    df_stab: pd.DataFrame = _load_and_tag(stab_path, args.stab_label)
    df_vote: pd.DataFrame = _load_and_tag(vote_path, args.vote_label)

    specs_full = set(df_full["spec"].unique())
    specs_stab = set(df_stab["spec"].unique())
    specs_vote = set(df_vote["spec"].unique())

    if args.specs:
        specs_requested = set(args.specs)
        specs = sorted(specs_requested & specs_full & specs_stab & specs_vote)
    else:
        specs = sorted(specs_full & specs_stab & specs_vote)

    if not specs:
        raise SystemExit("No common specs across full/stab/vote (after filtering).")

    print(f"[INFO] Found {len(specs)} specs to plot.")

    for spec in specs:
        print(f"[INFO] Plotting spec {spec}...")

        df_full_spec: pd.DataFrame = df_full[df_full["spec"] == spec].copy()
        df_stab_spec: pd.DataFrame = df_stab[df_stab["spec"] == spec].copy()
        df_vote_spec: pd.DataFrame = df_vote[df_vote["spec"] == spec].copy()

        if df_full_spec.empty or df_stab_spec.empty or df_vote_spec.empty:
            print(f"  [WARN] skipping {spec}: missing in at least one model")
            continue

        df_all: pd.DataFrame = pd.concat(
            [df_full_spec, df_stab_spec, df_vote_spec],
            ignore_index=True,
        )

        fig, ax = plt.subplots(figsize=(12.0, 5.0))
        plot_quarter_nowcasts_multi_panel(
            df_all,
            model_col="model",
            horizon_col="month_in_quarter",
            y_real_col="y_real_Q",
            y_hat_col="y_hat",
            ax=ax,
        )
        fig.tight_layout()
        out_path = out_dir / f"nowcasts_multi_panel_{spec}.png"
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        print(f"  [OK] saved {out_path}")

    print("[DONE] Multi-panel nowcast plots created.")


if __name__ == "__main__":
    main()
