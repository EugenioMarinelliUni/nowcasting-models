from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qrf_pipeline.diagnostics import summarize_by_month_of_quarter, summarize_by_target_quarter


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Summarize QRF pseudo-RT output folders.")
    p.add_argument("--root", required=True)
    p.add_argument("--outdir", required=True)
    return p


def main() -> None:
    args = build_parser().parse_args()

    root = Path(args.root)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    rows = []
    moq_rows = []

    for score_path in tqdm(sorted(root.rglob("scores.json")), desc="Summarizing QRF runs", unit="run"):
        run_dir = score_path.parent
        pred_path = run_dir / "predictions.csv"

        with open(score_path, "r", encoding="utf-8") as f:
            s = json.load(f)

        q = s.get("quantile", {})

        row = {
            "run": run_dir.name,
            "outdir": str(run_dir),
            "n": s.get("n"),
            "rmse": s.get("rmse"),
            "mae": s.get("mae"),
            "score_scale": s.get("score_scale"),
            "mean_pinball": q.get("mean_pinball"),
            "crps_approx": q.get("crps_approx"),
            "coverage_50": q.get("coverage_50"),
            "width_50": q.get("avg_width_50"),
            "winkler_50": q.get("winkler_50"),
            "coverage_error_50": q.get("coverage_error_50"),
            "coverage_80": q.get("coverage_80"),
            "width_80": q.get("avg_width_80"),
            "winkler_80": q.get("winkler_80"),
            "coverage_error_80": q.get("coverage_error_80"),
        }

        cfg_path = run_dir / "run_config.json"
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            row.update(
                {
                    "backend": cfg.get("backend"),
                    "n_predictors": cfg.get("n_predictors"),
                    "n_lags": cfg.get("n_lags"),
                    "n_y_lags": cfg.get("n_y_lags"),
                    "min_samples_leaf": cfg.get("min_samples_leaf"),
                    "max_features": cfg.get("max_features"),
                    "n_jobs": cfg.get("n_jobs"),
                    "predictors_path": cfg.get("predictors_path"),
                    "delay_style": cfg.get("delay_style"),
                    "delay_map_path": cfg.get("delay_map_path"),
                    "gdp_rel": cfg.get("gdp_rel"),
                }
            )

        rows.append(row)

        if pred_path.exists():
            pred = pd.read_csv(pred_path, parse_dates=["eval_date", "target_date"])
            moq = summarize_by_month_of_quarter(pred)
            moq.insert(0, "run", run_dir.name)
            moq.insert(1, "outdir", str(run_dir))
            moq_rows.append(moq)

            tq = summarize_by_target_quarter(pred)
            tq.to_csv(run_dir / "summary_by_target_quarter.csv", index=False)

    summary = pd.DataFrame(rows)
    if not summary.empty:
        summary = summary.sort_values(["rmse", "mean_pinball"], na_position="last")

    summary_path = outdir / "summary_all_runs.csv"
    summary.to_csv(summary_path, index=False)

    moq_path = outdir / "summary_by_moq.csv"
    if moq_rows:
        pd.concat(moq_rows, ignore_index=True).to_csv(moq_path, index=False)

    print("Saved:", summary_path)
    if moq_rows:
        print("Saved:", moq_path)


if __name__ == "__main__":
    main()
