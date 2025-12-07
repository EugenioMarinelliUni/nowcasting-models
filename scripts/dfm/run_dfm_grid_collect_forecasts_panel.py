#!/usr/bin/env python3
from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path
import sys
from typing import List

import pandas as pd

# Ensure src/ is on path when called from repo root
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


def _range_to_list(range_vals: List[int], name: str) -> List[int]:
    if len(range_vals) != 3:
        raise ValueError(f"--{name}-range must have exactly 3 integers: start end step.")
    start, end, step = range_vals
    if step <= 0:
        raise ValueError(f"--{name}-range step must be > 0 (got {step}).")
    vals = list(range(start, end + 1, step))
    if not vals:
        raise ValueError(f"--{name}-range [{start}, {end}, {step}] produced an empty list.")
    return vals


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Run simple DFM grid backtest and collect FULL forecast paths "
            "(y_hat, y_real_Q per quarter/horizon/spec) into long/wide CSVs."
        )
    )

    ap.add_argument(
        "--X",
        required=True,
        help="Full standardized predictor panel CSV.",
    )
    ap.add_argument(
        "--y",
        required=True,
        help="Full standardized target CSV aligned to X.",
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
        help="Optional mask CSV when --ragged-mode=mask.",
    )

    # Hyperparameter ranges
    ap.add_argument(
        "--q-range",
        type=int,
        nargs=3,
        metavar=("Q_MIN", "Q_MAX", "Q_STEP"),
        required=True,
        help="Range (start end step) of static factor dimensions q, inclusive.",
    )
    ap.add_argument(
        "--r-range",
        type=int,
        nargs=3,
        metavar=("R_MIN", "R_MAX", "R_STEP"),
        required=True,
        help="Range (start end step) of dynamic factor dimensions r, inclusive.",
    )
    ap.add_argument(
        "--p-range",
        type=int,
        nargs=3,
        metavar=("P_MIN", "P_MAX", "P_STEP"),
        required=True,
        help="Range (start end step) of VAR orders p for factor dynamics, inclusive.",
    )

    ap.add_argument(
        "--out-long-csv",
        required=True,
        help="Output CSV path for the long-format forecasts (one row per forecast).",
    )
    ap.add_argument(
        "--out-wide-csv",
        required=True,
        help="Output CSV path for the wide-format forecasts.",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    X_path = Path(args.X)
    y_path = Path(args.y)
    out_long = Path(args.out_long_csv)
    out_wide = Path(args.out_wide_csv)

    if not X_path.exists():
        raise FileNotFoundError(f"X panel not found: {X_path}")
    if not y_path.exists():
        raise FileNotFoundError(f"y target not found: {y_path}")

    if args.ragged_mode == "mask":
        if args.mask_path is None:
            raise ValueError("ragged-mode='mask' requires --mask-path.")
        mask_path = Path(args.mask_path)
        if not mask_path.exists():
            raise FileNotFoundError(f"Mask CSV not found: {mask_path}")
    else:
        mask_path = None

    # Grid
    q_list = _range_to_list(args.q_range, "q")
    r_list = _range_to_list(args.r_range, "r")
    p_list = _range_to_list(args.p_range, "p")
    grid = [(q, r, p) for q, r, p in product(q_list, r_list, p_list)]

    # Base config (we override grid per spec)
    cfg = DFMBacktestConfigSimple(
        panel_id=args.panel_id,
        X_path=X_path,
        y_path=y_path,
        train_start=args.train_start,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        monthly_freq=args.monthly_freq,
        grid=(),
        mask_path=mask_path,
        ragged_mode=args.ragged_mode,
    )

    # Load X, y, mask, y_q once
    X, y = load_X_y_simple(cfg.X_path, cfg.y_path, monthly_freq=cfg.monthly_freq)
    mask = None
    if cfg.ragged_mode == "mask" and cfg.mask_path is not None:
        mask = load_mask_simple(cfg.mask_path, monthly_freq=cfg.monthly_freq)
    y_q = build_quarterly_target_from_monthly(y)

    all_rows: list[pd.DataFrame] = []

    print(f"DFM forecast panel {args.panel_id}: {len(grid)} specs...")
    for (q, r, p) in grid:
        if r > q:
            continue

        spec_id = f"q{q}_r{r}_p{p}"
        print(f"  spec {spec_id}...")

        df_spec = collect_forecast_panel_single_spec_simple(
            cfg=cfg,
            X=X,
            y=y,
            y_q=y_q,
            mask=mask,
            q=q,
            r=r,
            p=p,
            min_obs=30,
        )

        if df_spec.empty:
            print(f"    [WARN] no forecasts for {spec_id}; skipping")
            continue

        df_spec = df_spec.copy()
        df_spec["panel_id"] = args.panel_id
        df_spec["spec"] = spec_id
        df_spec["q"] = q
        df_spec["r"] = r
        df_spec["p"] = p

        all_rows.append(df_spec)

    if not all_rows:
        raise RuntimeError("No forecasts collected for any spec; check your dates or grid.")

    df_long: pd.DataFrame = pd.concat(all_rows, ignore_index=True)

    # Basic column sanity
    required = {"spec", "panel_id", "q", "p", "year", "quarter", "y_real_Q", "y_hat"}
    missing = required.difference(df_long.columns)
    if missing:
        raise ValueError(f"Forecast DataFrame missing required columns: {missing}")

    out_long.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(out_long, index=False)

    # Build wide: one column per (panel_id, spec, month_in_quarter) y_hat
    if "month_in_quarter" in df_long.columns:
        horizon_col = "month_in_quarter"
    elif "horizon" in df_long.columns:
        horizon_col = "horizon"
    else:
        raise ValueError(
            "df_long is missing 'month_in_quarter' or 'horizon'; cannot build wide format."
        )

    key_cols = ["year", "quarter"]
    id_cols = key_cols + ["panel_id", "spec", "q", "p", horizon_col]

    df_wide = df_long[id_cols + ["y_hat"]].copy()
    df_wide = df_wide.drop_duplicates(subset=id_cols)

    # Pivot to wide: index = (year, quarter); columns = (panel_id, spec, horizon)
    df_pivot = df_wide.pivot_table(
        index=key_cols,
        columns=["panel_id", "spec", horizon_col],
        values="y_hat",
    )

    # y_real_Q as Series with MultiIndex (year, quarter)
    y_real = (
        df_long[key_cols + ["y_real_Q"]]
        .drop_duplicates(subset=key_cols)
        .set_index(key_cols)["y_real_Q"]
    )

    # Concatenate along index, no merge/join
    df_out = pd.concat([y_real.to_frame(), df_pivot], axis=1).reset_index()

    df_out.to_csv(out_wide, index=False)

    print(
        f"[OK] wrote DFM forecast long: {out_long} shape={df_long.shape}, "
        f"wide: {out_wide} shape={df_out.shape}"
    )


if __name__ == "__main__":
    main()
