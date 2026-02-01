from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _sign_equal_with_ties(pred: np.ndarray, actual: np.ndarray, ties: str) -> np.ndarray:
    """
    Boolean vector for directional accuracy: sign(pred) == sign(actual).

    ties:
      - "incorrect": any case where pred==0 or actual==0 counts as False
      - "correct":   any case where pred==0 or actual==0 counts as True
      - "ignore":    drop cases where pred==0 or actual==0 (handled by caller)
    """
    sp = np.sign(pred)
    sa = np.sign(actual)

    eq = sp == sa
    if ties == "incorrect":
        eq[(sp == 0) | (sa == 0)] = False
        return eq
    if ties == "correct":
        eq[(sp == 0) | (sa == 0)] = True
        return eq
    raise ValueError(f"unknown ties mode: {ties}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compute NOW-horizon RMSE + FDA overall and by moq.")
    ap.add_argument("--predictions-csv", required=True, help="Path to predictions.csv")
    ap.add_argument("--outdir", required=True, help="Output directory to write metrics")
    ap.add_argument("--horizon", default="now", help="Which horizon to evaluate (default: now)")
    ap.add_argument(
        "--ties",
        choices=["incorrect", "correct", "ignore"],
        default="incorrect",
        help="How to treat sign ties where pred==0 or actual==0.",
    )
    args = ap.parse_args()

    pred_path = Path(args.predictions_csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Read + enforce DataFrame type (helps type checkers/linters)
    df = pd.DataFrame(pd.read_csv(pred_path))

    # Filter horizon
    df = df[df["horizon"] == args.horizon].copy()

    # Coerce numeric
    df["pred"] = pd.to_numeric(df["pred"], errors="coerce")
    df["actual"] = pd.to_numeric(df["actual"], errors="coerce")

    # Drop NaNs (DataFrame signature)
    df = df.dropna(subset=["pred", "actual"]).copy()

    pred = df["pred"].to_numpy(dtype=float)
    actual = df["actual"].to_numpy(dtype=float)

    err = pred - actual
    rmse = float(np.sqrt(np.mean(err**2))) if err.size else float("nan")

    if args.ties == "ignore":
        mask = (pred != 0.0) & (actual != 0.0)
        pred2 = pred[mask]
        actual2 = actual[mask]
        fda = float(np.mean(np.sign(pred2) == np.sign(actual2))) if pred2.size else float("nan")
    else:
        fda = float(np.mean(_sign_equal_with_ties(pred, actual, ties=args.ties))) if pred.size else float("nan")

    rows = []
    for moq, g in df.groupby("moq"):
        p = g["pred"].to_numpy(dtype=float)
        a = g["actual"].to_numpy(dtype=float)

        e = p - a
        rmse_g = float(np.sqrt(np.mean(e**2))) if e.size else float("nan")

        if args.ties == "ignore":
            m = (p != 0.0) & (a != 0.0)
            fda_g = float(np.mean(np.sign(p[m]) == np.sign(a[m]))) if m.sum() else float("nan")
        else:
            fda_g = float(np.mean(_sign_equal_with_ties(p, a, ties=args.ties))) if p.size else float("nan")

        rows.append({"moq": int(moq), "n": int(len(g)), "rmse": rmse_g, "fda": fda_g})

    by_moq = pd.DataFrame(rows).sort_values("moq") if rows else pd.DataFrame(columns=["moq", "n", "rmse", "fda"])

    (outdir / "now_rows.csv").write_text(df.to_csv(index=False), encoding="utf-8")
    (outdir / "now_metrics_overall.csv").write_text(
        f"horizon,n,rmse,fda,ties\n{args.horizon},{len(df)},{rmse:.12g},{fda:.12g},{args.ties}\n",
        encoding="utf-8",
    )
    (outdir / "now_metrics_by_moq.csv").write_text(by_moq.to_csv(index=False), encoding="utf-8")

    print(f"n={len(df)} rmse={rmse:.12g} fda={fda:.12g} ties={args.ties}")
    print(by_moq.to_string(index=False))
    print(f"[OK] wrote: {outdir/'now_metrics_overall.csv'}")
    print(f"[OK] wrote: {outdir/'now_metrics_by_moq.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
