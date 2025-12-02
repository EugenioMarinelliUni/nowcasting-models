#!/usr/bin/env python3
from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path
import sys

import numpy as np
import pandas as pd

# ensure src/ on path
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


def _range_to_list(range_vals: list[int], name: str) -> list[int]:
    """
    Convert [start, end, step] into an inclusive integer list.

    Example: [1, 5, 2] -> [1, 3, 5]
    """
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
            "Collect per-vintage RMSE (abs error) for all (q,r,p) specs "
            "in a grid, producing:\n"
            "  (1) a long panel (Date, q, r, p, abs_err, best flags), and\n"
            "  (2) a wide Date x spec matrix of abs_err.\n"
            "Also marks the best spec per month and per quarter."
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
        help="Identifier for this run (stored as a column).",
    )
    ap.add_argument(
        "--train-start",
        required=True,
        help="Training start YYYY-MM-DD (expanding window starts here).",
    )
    ap.add_argument(
        "--eval-start",
        required=True,
        help="Evaluation start YYYY-MM-DD.",
    )
    ap.add_argument(
        "--eval-end",
        required=True,
        help="Evaluation end YYYY-MM-DD.",
    )
    ap.add_argument(
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Month-start (MS) or month-end (ME) dating. Default: MS.",
    )

    ap.add_argument(
        "--ragged-mode",
        default="none",
        choices=["none", "mask"],
        help="Ragged-edge handling: 'none' or 'mask'.",
    )
    ap.add_argument(
        "--mask-path",
        default=None,
        help="Mask CSV (Date + same series as X) if ragged-mode=mask.",
    )

    # range-style hyperparameters: all required
    ap.add_argument(
        "--q-range",
        type=int,
        nargs=3,
        metavar=("Q_MIN", "Q_MAX", "Q_STEP"),
        required=True,
        help="Range of static factor dimensions q: start end step (inclusive).",
    )
    ap.add_argument(
        "--r-range",
        type=int,
        nargs=3,
        metavar=("R_MIN", "R_MAX", "R_STEP"),
        required=True,
        help="Range of dynamic factor dimensions r: start end step (inclusive).",
    )
    ap.add_argument(
        "--p-range",
        type=int,
        nargs=3,
        metavar=("P_MIN", "P_MAX", "P_STEP"),
        required=True,
        help="Range of VAR orders p: start end step (inclusive).",
    )

    ap.add_argument(
        "--out-long-csv",
        required=True,
        help=(
            "Output CSV for long panel: Date, q, r, p, abs_err, spec, "
            "best_spec_month, is_best_month, best_spec_quarter, is_best_quarter."
        ),
    )
    ap.add_argument(
        "--out-wide-csv",
        required=True,
        help=(
            "Output CSV for wide matrix: index=Date, columns=spec "
            "(e.g. q3_r1_p2), values=abs_err."
        ),
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    X_path = Path(args.X)
    y_path = Path(args.y)
    if not X_path.exists():
        raise FileNotFoundError(f"X panel not found: {X_path}")
    if not y_path.exists():
        raise FileNotFoundError(f"y target not found: {y_path}")

    mask: pd.DataFrame | None = None
    if args.ragged_mode == "mask":
        if args.mask_path is None:
            raise ValueError("ragged-mode='mask' requires --mask-path.")
        mask_path = Path(args.mask_path)
        if not mask_path.exists():
            raise FileNotFoundError(f"Mask CSV not found: {mask_path}")
        mask = load_mask_simple(mask_path, monthly_freq=args.monthly_freq)

    # hyperparameter grids
    q_list = _range_to_list(args.q_range, "q")
    r_list = _range_to_list(args.r_range, "r")
    p_list = _range_to_list(args.p_range, "p")
    grid = [(q, r, p) for q, r, p in product(q_list, r_list, p_list)]

    # load data once
    X, y = load_X_y_simple(X_path, y_path, monthly_freq=args.monthly_freq)
    y_q = build_quarterly_target_from_monthly(y)

    cfg = DFMBacktestConfigSimple(
        panel_id=args.panel_id,
        X_path=X_path,
        y_path=y_path,
        train_start=args.train_start,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        monthly_freq=args.monthly_freq,
        grid=grid,
        mask_path=None,            # ragged handled via explicit mask below
        ragged_mode=args.ragged_mode,
    )

    all_rows: list[pd.DataFrame] = []

    # optional progress bar over specs
    try:
        from tqdm import tqdm  # type: ignore[import]
    except ImportError:  # pragma: no cover
        iterator = grid
    else:
        iterator = tqdm(
            grid,
            desc=f"DFM RMSE panel {cfg.panel_id}",
            total=len(grid),
            unit="spec",
        )

    for (q, r, p) in iterator:
        if r > q:
            continue

        df_spec = collect_forecast_panel_single_spec_simple(
            cfg=cfg,
            X=X,
            y=y,
            y_q=y_q,
            mask=mask,
            q=q,
            r=r,
            p=p,
        )

        if df_spec.empty:
            continue

        df_spec = df_spec.copy()
        # per-vintage RMSE with one observation is sqrt(sq_err) = abs(error)
        df_spec["abs_err"] = np.sqrt(df_spec["sq_err"])

        df_spec["q"] = q
        df_spec["r"] = r
        df_spec["p"] = p
        df_spec["panel_id"] = cfg.panel_id

        all_rows.append(
            df_spec[
                [
                    "Date",
                    "y_real_Q",
                    "y_hat",
                    "err",
                    "sq_err",
                    "abs_err",
                    "q",
                    "r",
                    "p",
                    "panel_id",
                ]
            ]
        )

    if not all_rows:
        raise RuntimeError("No forecasts collected for any spec; check your dates or grid.")

    df_long = pd.concat(all_rows, axis=0).sort_values(["Date", "q", "r", "p"])

    # Construct spec label and year/quarter
    df_long["spec"] = (
        "q" + df_long["q"].astype(int).astype(str)
        + "_r" + df_long["r"].astype(int).astype(str)
        + "_p" + df_long["p"].astype(int).astype(str)
    )
    df_long["year"] = df_long["Date"].dt.year.astype(int)
    df_long["quarter"] = ((df_long["Date"].dt.month - 1) // 3 + 1).astype(int)

    # ---------- WIDE MATRIX: Date x spec of abs_err ----------
    df_wide = (
        df_long.pivot_table(
            index="Date",
            columns="spec",
            values="abs_err",
            aggfunc="mean",  # one obs per (Date,spec) anyway
        )
        .sort_index()
    )

    out_long = Path(args.out_long_csv)
    out_long.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(out_long, index=False)
    print(f"[OK] wrote long RMSE panel: {out_long}  shape={df_long.shape}")

    out_wide = Path(args.out_wide_csv)
    out_wide.parent.mkdir(parents=True, exist_ok=True)
    df_wide.to_csv(out_wide, index=True)
    print(f"[OK] wrote wide RMSE matrix: {out_wide}  shape={df_wide.shape}")

    # ---------- BEST SPEC PER MONTH ----------
    # On wide matrix: one min per Date across columns (specs)
    best_rmse_per_month = df_wide.min(axis=1)
    best_spec_per_month = df_wide.idxmin(axis=1)

    df_best_month = pd.DataFrame(
        {
            "Date": best_rmse_per_month.index,
            "best_spec_month": best_spec_per_month.values,
            "best_rmse_month": best_rmse_per_month.values,
        }
    ).sort_values("Date")

    # Map back into long panel
    month_map = best_spec_per_month.to_dict()
    df_long["best_spec_month"] = df_long["Date"].map(month_map)
    df_long["is_best_month"] = (
        df_long["spec"] == df_long["best_spec_month"]
    ).astype(int)

    # ---------- BEST SPEC PER QUARTER ----------
    # Average abs_err over months of each quarter/spec
    grp_q = (
        df_long.groupby(["year", "quarter", "spec"])["abs_err"]
        .mean()
        .reset_index(name="mean_abs_err_q")
    )

    # For each (year, quarter), pick spec with minimal mean_abs_err_q
    idx_min = grp_q.groupby(["year", "quarter"])["mean_abs_err_q"].idxmin()
    best_q = grp_q.loc[idx_min].reset_index(drop=True)

    # Build a mapping (year, quarter) -> best spec
    best_q["key"] = list(zip(best_q["year"], best_q["quarter"]))
    map_q = dict(zip(best_q["key"], best_q["spec"]))

    # Attach to long panel
    df_long["key_q"] = list(zip(df_long["year"], df_long["quarter"]))
    df_long["best_spec_quarter"] = df_long["key_q"].map(map_q)
    df_long["is_best_quarter"] = (
        df_long["spec"] == df_long["best_spec_quarter"]
    ).astype(int)

    # Clean helper key
    df_long = df_long.drop(columns=["key_q"])

    # ---------- WRITE UPDATED LONG PANEL WITH BEST FLAGS ----------
    # Overwrite out_long with enriched version
    df_long.to_csv(out_long, index=False)
    print(f"[OK] updated long RMSE panel with best-spec flags: {out_long}")

    # ---------- SUMMARY CSVs FOR BEST SPECS ----------
    # 1) best per month
    best_month_path = out_wide.with_name(out_wide.stem + "__best_month.csv")
    df_best_month.to_csv(best_month_path, index=False)
    print(f"[OK] wrote best spec per month: {best_month_path}")

    # 2) best per quarter
    best_q_out = best_q[["year", "quarter", "spec", "mean_abs_err_q"]].rename(
        columns={
            "spec": "best_spec_quarter",
            "mean_abs_err_q": "best_rmse_quarter",
        }
    )
    best_quarter_path = out_wide.with_name(out_wide.stem + "__best_quarter.csv")
    best_q_out.to_csv(best_quarter_path, index=False)
    print(f"[OK] wrote best spec per quarter: {best_quarter_path}")


if __name__ == "__main__":
    main()
