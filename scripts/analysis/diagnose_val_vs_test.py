# scripts/analysis/diagnose_val_vs_test.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


QEND_MONTHS = {3, 6, 9, 12}


def rmse(x: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    return float(np.sqrt(np.mean(x * x)))


def summarize_series(x: pd.Series) -> dict:
    v = x.to_numpy(dtype=float, copy=True)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return {
            "n": 0,
            "mean": np.nan,
            "std": np.nan,
            "p05": np.nan,
            "p50": np.nan,
            "p95": np.nan,
            "rmse": np.nan,
        }
    return {
        "n": int(v.size),
        "mean": float(np.mean(v)),
        "std": float(np.std(v, ddof=0)),
        "p05": float(np.quantile(v, 0.05)),
        "p50": float(np.quantile(v, 0.50)),
        "p95": float(np.quantile(v, 0.95)),
        "rmse": rmse(v),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--oos-csv", required=True, type=str, help="mf_dfm_*.csv produced by the script")
    ap.add_argument("--train-end", required=True, type=str)
    ap.add_argument("--val-end", required=True, type=str)
    ap.add_argument("--out-dir", default=None, type=str)
    args = ap.parse_args()

    path = Path(args.oos_csv)
    if not path.exists():
        raise FileNotFoundError(path)

    out_dir = Path(args.out_dir) if args.out_dir else path.parent / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(path, index_col=0, parse_dates=True).sort_index()
    dates = pd.DatetimeIndex(df.index)

    train_end = pd.to_datetime(args.train_end)
    val_end = pd.to_datetime(args.val_end)

    val_mask = np.asarray((dates > train_end) & (dates <= val_end), dtype=bool)
    test_mask = np.asarray(dates > val_end, dtype=bool)

    if "y_q_used_in_filter" in df.columns:
        y_used = df["y_q_used_in_filter"].to_numpy(dtype=float)
        # If using an OOS file (masked after val_end), y_used is observed during validation.
        # If using a VAL-NOWCAST file (masked after train_end), y_used is NaN during validation.
        used_in_val = np.isfinite(y_used[val_mask]).sum()
        if used_in_val > 0:
            print(
                f"[WARN] y_q_used_in_filter is observed inside validation for {used_in_val} months.\n"
                f"       This file is not a pure nowcast path for validation.\n"
                f"       For validation diagnostics, prefer mf_dfm_val_nowcast_*.csv."
            )

    qend = np.asarray([m in QEND_MONTHS for m in dates.month], dtype=bool)
    yq_obs = df["y_q"]
    qend_obs = qend & np.isfinite(yq_obs.to_numpy(dtype=float))

    qend_val = qend_obs & val_mask
    qend_test = qend_obs & test_mask

    yq_val = yq_obs.iloc[np.where(qend_val)[0]]
    yq_test = yq_obs.iloc[np.where(qend_test)[0]]

    e1_val = df.loc[dates[val_mask], "error_m1"]
    e2_val = df.loc[dates[val_mask], "error_m2"]
    e3_val = df.loc[dates[val_mask], "error_m3"]

    e1_test = df.loc[dates[test_mask], "error_m1"]
    e2_test = df.loc[dates[test_mask], "error_m2"]
    e3_test = df.loc[dates[test_mask], "error_m3"]

    e3_all = df["error_m3"]
    e3_qend_val = e3_all.iloc[np.where(qend_val)[0]]
    se3_qend_val_sorted = (e3_qend_val.astype(float) ** 2).sort_values(ascending=False)

    top_k = 15
    top_val = se3_qend_val_sorted.head(top_k).to_frame(name="sq_error_m3")
    top_val["error_m3"] = e3_qend_val.loc[top_val.index].astype(float)
    top_val["y_q"] = yq_obs.loc[top_val.index].astype(float)
    top_val["y_q_hat"] = df.loc[top_val.index, "y_q_hat"].astype(float)

    rows = []
    rows.append({"window": "VAL", "series": "y_q (qend)", **summarize_series(yq_val)})
    rows.append({"window": "TEST", "series": "y_q (qend)", **summarize_series(yq_test)})

    rows.append({"window": "VAL", "series": "error_m1", **summarize_series(e1_val)})
    rows.append({"window": "VAL", "series": "error_m2", **summarize_series(e2_val)})
    rows.append({"window": "VAL", "series": "error_m3", **summarize_series(e3_val)})

    rows.append({"window": "TEST", "series": "error_m1", **summarize_series(e1_test)})
    rows.append({"window": "TEST", "series": "error_m2", **summarize_series(e2_test)})
    rows.append({"window": "TEST", "series": "error_m3", **summarize_series(e3_test)})

    summary = pd.DataFrame(rows)
    summary_path = out_dir / "val_test_summary.csv"
    summary.to_csv(summary_path, index=False)

    top_path = out_dir / "val_top_quarters_by_sq_error_m3.csv"
    top_val.to_csv(top_path, index_label="Date")

    plt.figure()
    plt.hist(yq_val.dropna().to_numpy(), bins=30, alpha=0.6, label="val")
    plt.hist(yq_test.dropna().to_numpy(), bins=30, alpha=0.6, label="test")
    plt.title("Realized y_q at quarter ends (standardized)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "hist_yq_qend_val_vs_test.png", dpi=150)
    plt.close()

    plt.figure()
    plt.hist(e3_val.dropna().to_numpy(), bins=30, alpha=0.6, label="val")
    plt.hist(e3_test.dropna().to_numpy(), bins=30, alpha=0.6, label="test")
    plt.title("error_m3 distributions (standardized)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "hist_error_m3_val_vs_test.png", dpi=150)
    plt.close()

    # Time-ordered plot (fix: do NOT plot the sorted-by-size series)
    se3_qend_val_ts = (e3_qend_val.astype(float) ** 2).sort_index()
    plt.figure()
    plt.plot(se3_qend_val_ts.index, se3_qend_val_ts.to_numpy())
    plt.title("Validation quarter-end squared errors (m3) — time ordered")
    plt.tight_layout()
    plt.savefig(out_dir / "val_sq_error_m3_qend_timeseries.png", dpi=150)
    plt.close()

    print(f"Wrote: {summary_path}")
    print(f"Wrote: {top_path}")
    print(f"Wrote plots to: {out_dir}")


if __name__ == "__main__":
    main()
