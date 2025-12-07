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

from dfm_pipeline.visualization.nowcast_horizon_errors import (  # noqa: E402
    HorizonErrorConfig,
    plot_horizon_error_distribution,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Compare horizon-specific error distributions (box/violin) "
            "across THREE panels (full / reduced A / reduced B) "
            "for a single DFM specification."
        )
    )

    # Three separate long CSVs, one per panel
    ap.add_argument(
        "--full-forecasts-long-csv",
        required=True,
        help="Forecast long CSV for FULL panel.",
    )
    ap.add_argument(
        "--stab-forecasts-long-csv",
        required=True,
        help="Forecast long CSV for reduced STABILITY panel.",
    )
    ap.add_argument(
        "--vote-forecasts-long-csv",
        required=True,
        help="Forecast long CSV for reduced VOTE panel.",
    )

    # Panel labels (used both as filters and titles)
    ap.add_argument(
        "--full-panel-id",
        required=True,
        help="Panel id label for FULL panel (must match or be assigned to its CSV).",
    )
    ap.add_argument(
        "--stab-panel-id",
        required=True,
        help="Panel id label for STABILITY panel.",
    )
    ap.add_argument(
        "--vote-panel-id",
        required=True,
        help="Panel id label for VOTE panel.",
    )

    ap.add_argument(
        "--spec",
        required=True,
        help="DFM specification label, e.g. 'q4_r1_p2'.",
    )
    ap.add_argument(
        "--metric",
        choices=["error", "abs_error"],
        default="abs_error",
        help="Metric to plot: 'error' or 'abs_error'. Default: abs_error.",
    )
    ap.add_argument(
        "--kind",
        choices=["box", "violin"],
        default="box",
        help="Plot type: 'box' or 'violin'. Default: box.",
    )
    ap.add_argument(
        "--eval-start",
        default=None,
        help="Optional lower bound for Date (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--eval-end",
        default=None,
        help="Optional upper bound for Date (YYYY-MM-DD).",
    )

    ap.add_argument(
        "--output",
        required=True,
        help="Output PNG path for the 1×3 comparison plot.",
    )

    return ap.parse_args()


def _load_and_tag_panel(csv_path: Path, panel_id: str) -> pd.DataFrame:
    """
    Load a single-panel long forecast CSV and ensure it has a 'panel_id' column
    with the given value.
    """
    df = pd.read_csv(csv_path, low_memory=False)

    # If panel_id col exists but empty or different, overwrite; if missing, create.
    df["panel_id"] = panel_id

    return df


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_forecasts_long_csv)
    stab_path = Path(args.stab_forecasts_long_csv)
    vote_path = Path(args.vote_forecasts_long_csv)

    for p in [full_path, stab_path, vote_path]:
        if not p.exists():
            raise FileNotFoundError(f"Forecast long CSV not found: {p}")

    # Load and tag each panel
    df_full = _load_and_tag_panel(full_path, args.full_panel_id)
    df_stab = _load_and_tag_panel(stab_path, args.stab_panel_id)
    df_vote = _load_and_tag_panel(vote_path, args.vote_panel_id)

    # Concatenate into one long DataFrame with panel_id labels
    df_long = pd.concat([df_full, df_stab, df_vote], ignore_index=True)

    # Create 1×3 subplots
    panel_ids = [args.full_panel_id, args.stab_panel_id, args.vote_panel_id]
    fig, axes = plt.subplots(
        nrows=1,
        ncols=3,
        figsize=(12.0, 4.0),
        sharey=True,
    )

    # Plot each panel
    for ax, pid in zip(axes, panel_ids):
        cfg = HorizonErrorConfig(
            panel_id=pid,
            spec=args.spec,
            metric=args.metric,  # type: ignore[arg-type]
            kind=args.kind,      # type: ignore[arg-type]
            eval_start=args.eval_start,
            eval_end=args.eval_end,
            figsize=(4.0, 4.0),
        )
        plot_horizon_error_distribution(df_long, cfg=cfg, ax=ax)
        # Compact title: just panel label
        ax.set_title(pid, fontsize=9)

    fig.suptitle(
        f"Horizon-specific {args.metric} distributions – spec {args.spec}",
        fontsize=11,
    )
    fig.tight_layout(rect=[0, 0.0, 1, 0.94])

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[OK] wrote 3-panel horizon-{args.metric} {args.kind} plot to: {out_path}")


if __name__ == "__main__":
    main()
