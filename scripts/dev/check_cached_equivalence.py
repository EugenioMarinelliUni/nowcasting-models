"""
End-to-end equivalence check: NEW vs NEW_CACHED using the FAST evaluator.

This avoids guessing internal function signatures and directly tests the
actual pipeline you run in practice.

It:
  1) runs scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast.py twice
     - once with DFM_STATE_SPACE_IMPL unset (NEW)
     - once with DFM_STATE_SPACE_IMPL=new_cached
  2) compares predictions.csv and scores.json
  3) prints first mismatching rows + abs diff summary
  4) exits non-zero if differences exceed tolerance
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _run_fast_eval(
    *,
    panel_csv: str,
    target_csv: str,
    outdir: Path,
    eval_start: str,
    eval_end: str,
    r: int,
    p: int,
    max_iter: int,
    tol: str,
    impl: str | None,
) -> None:
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()

    # Reduce timing noise / avoid thread oversubscription.
    env["OMP_NUM_THREADS"] = "1"
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"
    env["NUMEXPR_NUM_THREADS"] = "1"

    # Select impl.
    if impl is None:
        env.pop("DFM_STATE_SPACE_IMPL", None)
    else:
        env["DFM_STATE_SPACE_IMPL"] = impl

    cmd = [
        sys.executable,
        "scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast.py",
        "--panel-csv",
        panel_csv,
        "--target-csv",
        target_csv,
        "--date-col",
        "sasdate",
        "--target-col",
        "y",
        "--outdir",
        str(outdir),
        "--r",
        str(r),
        "--p",
        str(p),
        "--max-iter",
        str(max_iter),
        "--tol",
        str(tol),
        "--eval-start",
        eval_start,
        "--eval-end",
        eval_end,
        "--delay-style",
        "none",
        "--gdp-rel",
        "0",
    ]

    print("============================================================")
    print(f"RUN: FAST eval | impl={'NEW' if impl is None else impl} | outdir={outdir}")
    print("============================================================")
    subprocess.run(cmd, env=env, check=True)


def _compare_predictions(a_path: Path, b_path: Path, float_tol: float) -> tuple[pd.DataFrame, pd.Series]:
    A = pd.read_csv(a_path)
    B = pd.read_csv(b_path)

    key_cols = ["eval_date", "moq", "horizon", "target_date", "actual"]
    A_cols = set(A.columns)
    B_cols = set(B.columns)
    if A_cols != B_cols:
        raise RuntimeError(f"Column mismatch:\nA={sorted(A_cols)}\nB={sorted(B_cols)}")

    merged = A.merge(B, on=key_cols, suffixes=("_new", "_cached"), how="inner")
    if len(merged) != len(A) or len(merged) != len(B):
        raise RuntimeError(
            f"Row alignment mismatch after merge: len(A)={len(A)}, len(B)={len(B)}, len(merged)={len(merged)}. "
            "Check keys and ordering."
        )

    merged["abs_diff"] = (merged["pred_new"] - merged["pred_cached"]).abs()
    mism = merged.loc[merged["abs_diff"] > float_tol].copy()
    return mism, merged["abs_diff"]


def _compare_scores(a_path: Path, b_path: Path) -> dict:
    with a_path.open("r", encoding="utf-8") as f:
        A = json.load(f)
    with b_path.open("r", encoding="utf-8") as f:
        B = json.load(f)

    # Simple structural comparison + numeric deltas where possible.
    # If keys differ, report that immediately.
    if A.keys() != B.keys():
        return {"ok": False, "reason": "Top-level keys differ", "A_keys": sorted(A.keys()), "B_keys": sorted(B.keys())}

    # Focus on the most important parts.
    out = {"ok": True, "deltas": {}}

    def _num(x):
        return isinstance(x, (int, float)) and not isinstance(x, bool)

    def _walk(prefix, a, b):
        if type(a) != type(b):
            out["ok"] = False
            out["deltas"][prefix] = {"type_mismatch": (str(type(a)), str(type(b)))}
            return
        if isinstance(a, dict):
            if a.keys() != b.keys():
                out["ok"] = False
                out["deltas"][prefix] = {"keys_mismatch": (sorted(a.keys()), sorted(b.keys()))}
                return
            for k in a.keys():
                _walk(f"{prefix}.{k}" if prefix else k, a[k], b[k])
        elif isinstance(a, list):
            if len(a) != len(b):
                out["ok"] = False
                out["deltas"][prefix] = {"len_mismatch": (len(a), len(b))}
                return
            for i, (ai, bi) in enumerate(zip(a, b)):
                _walk(f"{prefix}[{i}]", ai, bi)
        else:
            if _num(a) and _num(b):
                delta = float(a) - float(b)
                if delta != 0.0:
                    out["deltas"][prefix] = {"A": float(a), "B": float(b), "delta": delta}
            else:
                if a != b:
                    out["deltas"][prefix] = {"A": a, "B": b}

    _walk("", A, B)

    # If only tiny floating-point differences exist, keep ok=True.
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-csv", required=True)
    ap.add_argument("--target-csv", required=True)
    ap.add_argument("--eval-start", default="2010-01-01")
    ap.add_argument("--eval-end", default="2010-01-01")
    ap.add_argument("--r", type=int, default=1)
    ap.add_argument("--p", type=int, default=1)
    ap.add_argument("--max-iter", type=int, default=15)
    ap.add_argument("--tol", default="1e-4")
    ap.add_argument("--float-tol", type=float, default=1e-12, help="Tolerance for prediction equality checks.")
    ap.add_argument("--outroot", default="runs/check_cached_equivalence")
    args = ap.parse_args()

    outroot = Path(args.outroot)
    out_new = outroot / "new"
    out_cached = outroot / "new_cached"

    _run_fast_eval(
        panel_csv=args.panel_csv,
        target_csv=args.target_csv,
        outdir=out_new,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        r=args.r,
        p=args.p,
        max_iter=args.max_iter,
        tol=args.tol,
        impl=None,
    )
    _run_fast_eval(
        panel_csv=args.panel_csv,
        target_csv=args.target_csv,
        outdir=out_cached,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        r=args.r,
        p=args.p,
        max_iter=args.max_iter,
        tol=args.tol,
        impl="new_cached",
    )

    pred_new = out_new / "predictions.csv"
    pred_cached = out_cached / "predictions.csv"
    scores_new = out_new / "scores.json"
    scores_cached = out_cached / "scores.json"

    print("============================================================")
    print("COMPARE: predictions.csv (NEW vs NEW_CACHED)")
    print("============================================================")
    mism, abs_diff = _compare_predictions(pred_new, pred_cached, args.float_tol)

    if len(mism) == 0:
        print("[OK] No mismatches above tolerance.")
    else:
        print(f"[FAIL] Found {len(mism)} mismatching rows out of {len(abs_diff)} (tol={args.float_tol:g})")
        cols = ["eval_date", "moq", "horizon", "target_date", "pred_new", "pred_cached", "actual", "abs_diff"]
        print(mism.sort_values("abs_diff", ascending=False).head(25)[cols].to_string(index=False))

    print("============================================================")
    print("ABS DIFF SUMMARY")
    print("============================================================")
    print(abs_diff.describe())
    print(f"max_abs_diff = {abs_diff.max()}")

    print("============================================================")
    print("COMPARE: scores.json (NEW vs NEW_CACHED)")
    print("============================================================")
    score_cmp = _compare_scores(scores_new, scores_cached)
    if score_cmp.get("ok", False):
        if len(score_cmp.get("deltas", {})) == 0:
            print("[OK] scores.json identical.")
        else:
            # Likely tiny float differences; print a limited view.
            deltas = score_cmp["deltas"]
            print(f"[WARN] scores.json has {len(deltas)} differing leaves (often float roundoff). Showing first 40:")
            for i, (k, v) in enumerate(deltas.items()):
                if i >= 40:
                    break
                print(f"- {k}: {v}")
    else:
        print("[FAIL] scores.json structural mismatch:")
        print(score_cmp)

    # Exit policy: fail only if prediction diffs exceed tolerance or score structure differs.
    if len(mism) > 0 or not score_cmp.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
