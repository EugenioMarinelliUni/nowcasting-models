#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

# Ensure src/ is on path
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.dfm_simple.backtest import (  # noqa: E402
    DFMBacktestConfigSimple,
    load_X_y_simple,
    load_mask_simple,
    build_quarterly_target_from_monthly,
    collect_forecast_panel_single_spec_simple,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Collect per-vintage nowcasts for a single (q,r,p) spec of the simple DFM.\n"
            "Outputs a panel with one row per evaluation month T:\n"
            "  Date, year, quarter, month_in_quarter, y_real_Q, y_hat, err, sq_err."
        )
    )
    ap.add_argument(
        "--X",
        required=True,
        help="Full standardized X panel CSV (1990–2025), with a date column.",
    )
    ap.add_argument(
        "--y",
        required=True,
        help="Full standardized target y CSV (1990–2025), with a date column and one numeric 'y'.",
    )
    ap.add_argument(
        "--panel-id",
        default="1960_noVIX_TEST_full_baseline_simple_single_spec",
        help="Identifier stored in metadata columns if needed.",
    )
    ap.add_argument(
        "--train-start",
        default="1990-01-01",
        help="First date allowed in estimation sample (default: 1990-01-01).",
    )
    ap.add_argument(
        "--eval-start",
        required=True,
        help="First evaluation month (YYYY-MM-DD), e.g. 2000-01-01.",
    )
    ap.add_argument(
        "--eval-end",
        required=True,
        help="Last evaluation month (YYYY-MM-DD), e.g. 2019-12-01.",
    )
    ap.add_argument(
        "--ragged-mode",
        default="none",
        choices=["none", "mask"],
        help="How to handle the ragged edge: 'none' or 'mask'.",
    )
    ap.add_argument(
        "--mask",
        default=None,
        help=(
            "Optional ragged-edge mask CSV (same layout as X, with a date column). "
            "Used only if --ragged-mode=mask."
        ),
    )
    ap.add_argument(
        "--q",
        type=int,
        required=True,
        help="Number of static factors (EM-PCA).",
    )
    ap.add_argument(
        "--r",
        type=int,
        default=1,
        help="Dynamic factor dimension (placeholder in simple version).",
    )
    ap.add_argument(
        "--p",
        type=int,
        required=True,
        help="VAR order for factor dynamics.",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Where to write the per-vintage forecast panel.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    X_path = Path(args.X)
    y_path = Path(args.y)
    mask_path = Path(args.mask) if args.mask is not None else None

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = DFMBacktestConfigSimple(
        panel_id=args.panel_id,
        X_path=X_path,
        y_path=y_path,
        train_start=args.train_start,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        monthly_freq="MS",
        grid=[],
        mask_path=mask_path,
        ragged_mode=args.ragged_mode,
    )

    # Load X, y, mask, quarterly target
    X, y = load_X_y_simple(cfg.X_path, cfg.y_path, monthly_freq=cfg.monthly_freq)

    mask = None
    if cfg.ragged_mode == "mask" and cfg.mask_path is not None:
        mask = load_mask_simple(cfg.mask_path, monthly_freq=cfg.monthly_freq)

    y_q = build_quarterly_target_from_monthly(y)

    df_panel = collect_forecast_panel_single_spec_simple(
        cfg,
        X,
        y,
        y_q,
        mask,
        q=args.q,
        r=args.r,
        p=args.p,
    )

    df_panel.to_csv(out_path, index=False)
    print(
        f"[OK] wrote per-vintage forecast panel for q={args.q}, r={args.r}, p={args.p}: "
        f"{out_path}  shape={df_panel.shape}"
    )


if __name__ == "__main__":
    main()
