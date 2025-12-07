#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

# Ensure src/ is on path when called from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.visualization.nowcast_quarter_plots import (  # noqa: E402
    NowcastPlotConfig,
    plot_four_dots_per_quarter,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Compare three panels (full, reduced-stability, reduced-vote) for a given "
            "DFM spec by plotting, for each quarter, the three within-quarter nowcasts "
            "and the realized value, in a 3-panel figure."
        )
    )

    ap.add_argument(
        "--full-forecasts-long-csv",
        required=True,
        help="Forecasts-long CSV for FULL panel (dfm_simple_all_specs_forecasts_long.csv).",
    )
    ap.add_argument(
        "--stab-forecasts-long-csv",
        required=True,
        help="Forecasts-long CSV for reduced STABILITY panel.",
    )
    ap.add_argument(
        "--vote-forecasts-long-csv",
        required=True,
        help="Forecasts-long CSV for reduced VOTE panel.",
    )

    ap.add_argument(
        "--spec",
        required=True,
        help="Specification label, e.g. q2_r1_p2.",
    )

    ap.add_argument(
        "--eval-start",
        required=True,
        help="Evaluation start date (YYYY-MM-DD) for filtering the monthly vintages.",
    )
    ap.add_argument(
        "--eval-end",
        required=True,
        help="Evaluation end date (YYYY-MM-DD) for filtering the monthly vintages.",
    )

    ap.add_argument(
        "--output",
        required=True,
        help="Output PNG path.",
    )

    ap.add_argument(
        "--figsize",
        type=float,
        nargs=2,
        default=(12.0, 8.0),
        metavar=("WIDTH", "HEIGHT"),
        help="Figure size in inches (default: 12 8).",
    )

    return ap.parse_args()


def _load_and_filter(path: Path, eval_start: str, eval_end: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    if "Date" not in df.columns:
        raise ValueError(f"{path} must contain a 'Date' column.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    start = pd.to_datetime(eval_start)
    end = pd.to_datetime(eval_end)
    df = df[(df["Date"] >= start) & (df["Date"] <= end)]

    return df


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_forecasts_long_csv)
    stab_path = Path(args.stab_forecasts_long_csv)
    vote_path = Path(args.vote_forecasts_long_csv)
    out_path = Path(args.output)

    for p in [full_path, stab_path, vote_path]:
        if not p.exists():
            raise FileNotFoundError(f"Input forecasts-long CSV not found: {p}")

    # Load and filter
    df_full = _load_and_filter(full_path, args.eval_start, args.eval_end)
    df_stab = _load_and_filter(stab_path, args.eval_start, args.eval_end)
    df_vote = _load_and_filter(vote_path, args.eval_start, args.eval_end)

    fig, axes = plt.subplots(
        nrows=3,
        ncols=1,
        figsize=tuple(args.figsize),
        sharex=True,
        sharey=True,
    )

    panels = [
        ("FULL panel", df_full, axes[0]),
        ("Reduced STABILITY panel", df_stab, axes[1]),
        ("Reduced VOTE panel", df_vote, axes[2]),
    ]

    any_plotted = False

    for title_prefix, df_panel, ax in panels:
        cfg = NowcastPlotConfig(
            spec=args.spec,
            title=f"{title_prefix} – {args.spec}",
            figsize=tuple(args.figsize),
        )
        try:
            plot_four_dots_per_quarter(df_panel, cfg=cfg, ax=ax)
            any_plotted = True
        except ValueError as e:
            ax.text(
                0.5,
                0.5,
                f"No data for {title_prefix}\n({e})",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title(f"{title_prefix} – {args.spec}")

    if not any_plotted:
        raise RuntimeError(
            f"No forecasts found for spec={args.spec} in any of the three panels."
        )

    axes[-1].set_xlabel("Quarter")
    fig.suptitle(f"DFM nowcasts comparison – spec {args.spec}", y=0.98)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[OK] wrote multi-panel quarter-dots plot: {out_path}")


if __name__ == "__main__":
    main()
