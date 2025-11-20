from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional

from dfm_pipeline.realtime.scaling import (
    Scaler,
    load_panel_csv,
    compute_and_optionally_save_scaler,
)


def _maybe_load_scaler(path: Optional[str]) -> Optional[Scaler]:
    if path is None:
        return None
    p = Path(path)
    return Scaler.load_csv(p)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Build out-of-sample (OOS) z-panels for explanatory variables (X) "
            "and targets (y) using a **frozen** scaler from a training window. "
            "You can either supply existing scalers or compute them from training raw panels."
        )
    )

    # --------- Inputs for X (explanatory) ---------
    ap.add_argument("--train-raw-x", type=str, default=None,
                    help="Training-window RAW X panel (to compute scaler_X if --scaler-x not given).")
    ap.add_argument("--oos-raw-x", type=str, required=True,
                    help="OOS RAW X panel to transform (any interval).")
    ap.add_argument("--scaler-x", type=str, default=None,
                    help="Path to existing scaler for X (CSV with columns mu,sigma). If absent, will compute from --train-raw-x.")
    ap.add_argument("--out-x", type=str, required=True,
                    help="Output path for OOS z-panel X (CSV).")

    # --------- Inputs for y (target) ---------
    ap.add_argument("--train-raw-y", type=str, default=None,
                    help="Training-window RAW y panel/series (to compute scaler_Y if --scaler-y not given).")
    ap.add_argument("--oos-raw-y", type=str, default=None,
                    help="OOS RAW y panel/series to transform (optional).")
    ap.add_argument("--scaler-y", type=str, default=None,
                    help="Path to existing scaler for y. If absent and --oos-raw-y provided, will compute from --train-raw-y.")
    ap.add_argument("--out-y", type=str, default=None,
                    help="Output path for OOS z-panel y (CSV). If omitted, y is skipped.")

    # --------- Options ---------
    ap.add_argument("--align-to-train-cols", type=str, default="true",
                    help="If true, OOS panels are reindexed to training/scaler column order. [true|false]")
    ap.add_argument("--eps", type=float, default=1e-12, help="Min std for stability (σ<eps -> σ:=1).")

    args = ap.parse_args()
    align = args.align_to_train_cols.lower() == "true"

    # ----------------- X: scaler -----------------
    scaler_x = _maybe_load_scaler(args.scaler_x)
    if scaler_x is None:
        if args.train_raw_x is None:
            raise ValueError("Neither --scaler-x nor --train-raw-x provided for X.")
        train_x = load_panel_csv(Path(args.train_raw_x))
        scaler_x = compute_and_optionally_save_scaler(train_x, Path(args.scaler_x) if args.scaler_x else None, eps=args.eps)

    # ----------------- X: transform OOS -----------------
    X_oos_raw = load_panel_csv(Path(args.oos_raw_x))
    X_oos_z = scaler_x.apply(X_oos_raw, align=align)
    out_x = Path(args.out_x); out_x.parent.mkdir(parents=True, exist_ok=True)
    X_oos_z.to_csv(out_x)

    print(f"[OOS] Wrote standardized X: {out_x}  (rows={X_oos_z.shape[0]}, cols={X_oos_z.shape[1]})")

    # ----------------- y: optional branch -----------------
    if args.oos_raw_y and args.out_y:
        scaler_y = _maybe_load_scaler(args.scaler_y)
        if scaler_y is None:
            if args.train_raw_y is None:
                raise ValueError("You requested y output but provided neither --scaler-y nor --train-raw-y.")
            train_y = load_panel_csv(Path(args.train_raw_y))
            scaler_y = compute_and_optionally_save_scaler(train_y, Path(args.scaler_y) if args.scaler_y else None, eps=args.eps)

        Y_oos_raw = load_panel_csv(Path(args.oos_raw_y))
        Y_oos_z = scaler_y.apply(Y_oos_raw, align=align)
        out_y = Path(args.out_y); out_y.parent.mkdir(parents=True, exist_ok=True)
        Y_oos_z.to_csv(out_y)
        print(f"[OOS] Wrote standardized y: {out_y}  (rows={Y_oos_z.shape[0]}, cols={Y_oos_z.shape[1]})")
    else:
        if any([args.oos_raw_y, args.out_y, args.scaler_y, args.train_raw_y]):
            print("[WARN] Skipping y because either --oos-raw-y or --out-y is missing.")
