#!/usr/bin/env python3
from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path
import sys

# Ensure src/ is on path when called from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.dfm_simple.backtest import (  # noqa: E402
    DFMBacktestConfigSimple,
    run_dfm_grid_simple,
)


def _range_to_list(range_vals: list[int], name: str) -> list[int]:
    """
    Convert [start, end, step] to an inclusive Python range list.

    Example:
      range_vals = [1, 5, 2] -> [1, 3, 5]

    Raises if step <= 0 or the resulting list is empty.
    """
    if len(range_vals) != 3:
        raise ValueError(f"--{name}-range must have exactly 3 integers: start end step.")
    start, end, step = range_vals
    if step <= 0:
        raise ValueError(f"--{name}-range step must be > 0 (got {step}).")

    vals = list(range(start, end + 1, step))  # inclusive end
    if not vals:
        raise ValueError(f"--{name}-range [{start}, {end}, {step}] produced an empty list.")
    return vals


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Run simple DFM grid backtest on a full standardized panel X and "
            "target y, over a grid of (q, r, p) specified as ranges "
            "(start, end, step) for each dimension."
        )
    )

    ap.add_argument(
        "--X",
        required=True,
        help=(
            "Full standardized predictor panel CSV, e.g. "
            "dataset/1960_noVIX_TEST/full_panels/"
            "1960_noVIX_TEST__full_1990_01_2025_04.csv"
        ),
    )
    ap.add_argument(
        "--y",
        required=True,
        help=(
            "Full standardized target CSV aligned to X, e.g. "
            "data/targets/gdp_A191RL1Q225SBEA_TEST/"
            "gdp_A191RL1Q225SBEA_TEST__full_1990_01_2025_04.csv"
        ),
    )
    ap.add_argument(
        "--panel-id",
        required=True,
        help="Identifier for this panel/specification (stored in the output).",
    )
    ap.add_argument(
        "--train-start",
        required=True,
        help="Training start date (YYYY-MM-DD), e.g. 1990-01-01.",
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
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Normalize index to month-start (MS) or month-end (ME). Default: MS.",
    )

    ap.add_argument(
        "--ragged-mode",
        default="none",
        choices=["none", "mask"],
        help="Ragged-edge handling: 'none' (full info) or 'mask' (apply release mask).",
    )
    ap.add_argument(
        "--mask-path",
        default=None,
        help=(
            "Optional mask CSV (with Date + same series as X) when --ragged-mode=mask. "
            "Entries should be 0/1 or True/False."
        ),
    )

    # Range-style hyperparameter specs (all required)
    ap.add_argument(
        "--q-range",
        type=int,
        nargs=3,
        metavar=("Q_MIN", "Q_MAX", "Q_STEP"),
        required=True,
        help=(
            "Range (start end step) of static factor dimensions q, inclusive. "
            "Example: --q-range 1 5 1  -> q = 1,2,3,4,5."
        ),
    )
    ap.add_argument(
        "--r-range",
        type=int,
        nargs=3,
        metavar=("R_MIN", "R_MAX", "R_STEP"),
        required=True,
        help=(
            "Range (start end step) of dynamic factor dimensions r, inclusive. "
            "Example: --r-range 1 2 1 -> r = 1,2. "
            "Note: in the current simple implementation r is not used, but kept for compatibility."
        ),
    )
    ap.add_argument(
        "--p-range",
        type=int,
        nargs=3,
        metavar=("P_MIN", "P_MAX", "P_STEP"),
        required=True,
        help=(
            "Range (start end step) of VAR orders p for factor dynamics, inclusive. "
            "Example: --p-range 1 3 1 -> p = 1,2,3."
        ),
    )

    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output CSV path for the grid summary (RMSE/FDA per (q,r,p)).",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    X_path = Path(args.X)
    y_path = Path(args.y)
    out_path = Path(args.out_csv)

    if not X_path.exists():
        raise FileNotFoundError(f"X panel not found: {X_path}")
    if not y_path.exists():
        raise FileNotFoundError(f"y target not found: {y_path}")

    mask_path: Path | None
    if args.ragged_mode == "mask":
        if args.mask_path is None:
            raise ValueError("ragged-mode='mask' requires --mask-path.")
        mask_path = Path(args.mask_path)
        if not mask_path.exists():
            raise FileNotFoundError(f"Mask CSV not found: {mask_path}")
    else:
        mask_path = None

    # Build hyperparameter lists from ranges
    q_list = _range_to_list(args.q_range, "q")
    r_list = _range_to_list(args.r_range, "r")
    p_list = _range_to_list(args.p_range, "p")

    # Cartesian product grid
    grid = [(q, r, p) for q, r, p in product(q_list, r_list, p_list)]

    cfg = DFMBacktestConfigSimple(
        panel_id=args.panel_id,
        X_path=X_path,
        y_path=y_path,
        train_start=args.train_start,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        monthly_freq=args.monthly_freq,
        grid=grid,
        mask_path=mask_path,
        ragged_mode=args.ragged_mode,
    )

    df_res = run_dfm_grid_simple(cfg)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_res.to_csv(out_path, index=False)
    print(
        f"[OK] wrote SIMPLE DFM grid results: {out_path}  "
        f"shape={df_res.shape}  grid_size={len(grid)}"
    )


if __name__ == "__main__":
    main()
