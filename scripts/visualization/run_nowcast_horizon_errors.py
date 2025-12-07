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
            "Plot horizon-specific error distributions (box/violin) "
            "for a single panel/spec from a per-vintage forecast long CSV."
        )
    )

    ap.add_argument(
        "--forecasts-long-csv",
        required=True,
        help="Path to dfm_simple_all_specs_forecasts_long.csv (or similar).",
    )
    ap.add_argument(
        "--panel-id",
        required=True,
        help="Panel identifier, e.g. 1960_noVIX_full_mu1990_1999.",
    )
    ap.add_argument(
        "--spec",
        required=True,
        help="DFM specification label, e.g. q4_r1_p2.",
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
        help="Output PNG path.",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    csv_path = Path(args.forecasts_long_csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"Forecast long CSV not found: {csv_path}")

    df_long = pd.read_csv(csv_path, low_memory=False)

    cfg = HorizonErrorConfig(
        panel_id=args.panel_id,
        spec=args.spec,
        metric=args.metric,  # type: ignore[arg-type]
        kind=args.kind,      # type: ignore[arg-type]
        eval_start=args.eval_start,
        eval_end=args.eval_end,
    )

    fig, ax = plt.subplots(figsize=cfg.figsize)
    plot_horizon_error_distribution(df_long, cfg=cfg, ax=ax)
    fig.tight_layout()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[OK] wrote horizon-{args.metric} {args.kind} plot to: {out_path}")


if __name__ == "__main__":
    main()
