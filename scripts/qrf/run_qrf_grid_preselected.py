from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from itertools import product
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd

from qrf_pipeline.config import max_features_label, normalize_max_features
from tqdm import tqdm


def _parse_csv(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_int_grid(value: str) -> list[int]:
    return [int(x) for x in _parse_csv(value)]


def _parse_max_features_grid(value: str) -> list[str | float | int | None]:
    out = []
    for item in _parse_csv(value):
        out.append(normalize_max_features(item))
    return out


def _method_map(pred_root: Path, methods: list[str], top_k: int) -> dict[str, Path]:
    out = {}
    for method in methods:
        candidates = [
            pred_root / f"{method}__top{top_k}.json",
            pred_root / method / "selected_predictors.json",
            pred_root / f"{method}.json",
        ]
        for p in candidates:
            if p.exists():
                out[method] = p
                break
        else:
            raise FileNotFoundError(
                f"No predictor list found for method={method}. Tried: {candidates}"
            )
    return out


def _read_scores(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _score_row(run: dict[str, Any], scores: dict[str, Any], phase: str) -> dict[str, Any]:
    q = scores.get("quantile", {})
    return {
        "phase": phase,
        "method": run["method"],
        "run": run["run_name"],
        "n": scores.get("n"),
        "rmse": scores.get("rmse"),
        "mae": scores.get("mae"),
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
        "n_lags": run["n_lags"],
        "n_y_lags": run["n_y_lags"],
        "min_samples_leaf": run["min_samples_leaf"],
        "max_features": run["max_features"],
        "n_estimators": run["n_estimators"],
        "n_predictors": run["n_predictors"],
        "predictors_path": str(run["predictors_path"]),
        "outdir": str(run["outdir"]),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Grid-search QRF separately for each predictor-selection method."
    )

    p.add_argument("--x-path", required=True)
    p.add_argument("--y-path", required=True)
    p.add_argument("--pred-root", required=True)
    p.add_argument("--out-root", required=True)

    p.add_argument("--methods", default="sis,tstat_lm,lars_lm,lars_tscv")
    p.add_argument("--top-k", type=int, default=20)

    p.add_argument("--tune-start", required=True)
    p.add_argument("--tune-end", required=True)
    p.add_argument("--test-start", default=None)
    p.add_argument("--test-end", default=None)

    p.add_argument("--quantiles", default="0.10,0.25,0.50,0.75,0.90")
    p.add_argument("--n-lags-grid", default="3,6")
    p.add_argument("--n-y-lags-grid", default="2")
    p.add_argument("--min-samples-leaf-grid", default="1,2,3,5")
    p.add_argument("--max-features-grid", default="sqrt,0.5,1.0")
    p.add_argument("--n-estimators-grid", default="500")
    p.add_argument("--n-predictors-grid", default=None)
    p.add_argument("--min-train-rows", type=int, default=36)
    p.add_argument("--random-state", type=int, default=123)
    p.add_argument("--n-jobs", type=int, default=-1)

    p.add_argument("--delay-style", default="none")
    p.add_argument("--delay-map-path", default=None)
    p.add_argument("--gdp-rel", type=int, default=0)
    p.add_argument("--no-qe-leak", action=argparse.BooleanOptionalAction, default=True)

    p.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--selection-metric", choices=["rmse", "mae", "mean_pinball", "crps_approx"], default="rmse")
    p.add_argument("--run-test", action=argparse.BooleanOptionalAction, default=True)

    return p


def main() -> None:
    args = build_parser().parse_args()

    pred_root = Path(args.pred_root)
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    methods = _parse_csv(args.methods)
    predictor_paths = _method_map(pred_root, methods, args.top_k)

    n_predictors_grid = _parse_int_grid(args.n_predictors_grid) if args.n_predictors_grid else [args.top_k]

    runs = []
    for method, predictors_path in predictor_paths.items():
        for n_predictors, n_lags, n_y_lags, min_leaf, max_features, n_estimators in product(
            n_predictors_grid,
            _parse_int_grid(args.n_lags_grid),
            _parse_int_grid(args.n_y_lags_grid),
            _parse_int_grid(args.min_samples_leaf_grid),
            _parse_max_features_grid(args.max_features_grid),
            _parse_int_grid(args.n_estimators_grid),
        ):
            max_features = normalize_max_features(max_features)
            mf_label = max_features_label(max_features)

            run_name = (
                f"qrf_{method}_top{n_predictors}"
                f"_l{n_lags}"
                f"_yl{n_y_lags}"
                f"_leaf{min_leaf}"
                f"_est{n_estimators}"
                f"_{mf_label}"
            )

            runs.append(
                {
                    "method": method,
                    "predictors_path": predictors_path,
                    "n_predictors": n_predictors,
                    "n_lags": n_lags,
                    "n_y_lags": n_y_lags,
                    "min_samples_leaf": min_leaf,
                    "max_features": max_features,
                    "n_estimators": n_estimators,
                    "run_name": run_name,
                    "outdir": out_root / "tune" / method / run_name,
                }
            )

    print("Planned tuning runs:", len(runs))
    print("Output root:", out_root)

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for run in tqdm(runs, desc="QRF validation grid", unit="run"):
        scores_path = run["outdir"] / "scores.json"

        if not (args.skip_existing and scores_path.exists()):
            cmd = [
                sys.executable,
                "scripts/qrf/run_qrf_pseudort.py",
                "--backend", "qrf",
                "--quantiles", args.quantiles,
                "--x-path", args.x_path,
                "--y-path", args.y_path,
                "--eval-start", args.tune_start,
                "--eval-end", args.tune_end,
                "--predictors-path", str(run["predictors_path"]),
                "--n-predictors", str(run["n_predictors"]),
                "--n-lags", str(run["n_lags"]),
                "--n-y-lags", str(run["n_y_lags"]),
                "--min-train-rows", str(args.min_train_rows),
                "--n-estimators", str(run["n_estimators"]),
                "--min-samples-leaf", str(run["min_samples_leaf"]),
                "--max-features", str(run["max_features"]),
                "--random-state", str(args.random_state),
                "--n-jobs", str(args.n_jobs),
                "--delay-style", args.delay_style,
                "--gdp-rel", str(args.gdp_rel),
                "--outdir", str(run["outdir"]),
            ]

            if args.delay_map_path:
                cmd += ["--delay-map-path", args.delay_map_path]

            cmd.append("--no-qe-leak" if args.no_qe_leak else "--no-no-qe-leak")
            cmd.append("--progress" if args.progress else "--no-progress")

            tqdm.write(f"RUN tune {run['run_name']}")
            result = subprocess.run(cmd)

            if result.returncode != 0:
                failures.append(
                    {
                        "phase": "tune",
                        "method": run["method"],
                        "run": run["run_name"],
                        "returncode": result.returncode,
                        "outdir": str(run["outdir"]),
                    }
                )
                continue

        if scores_path.exists():
            rows.append(_score_row(run, _read_scores(scores_path), phase="tune"))

    summary_path = out_root / "grid_summary_tune.csv"
    _write_csv(summary_path, rows)

    if failures:
        _write_csv(out_root / "grid_failures.csv", failures)

    print("Saved tuning summary to:", summary_path)

    if not rows:
        print("No successful tuning runs.")
        return

    df = pd.DataFrame(rows)
    metric = args.selection_metric
    df_sorted = df.sort_values(["method", metric, "rmse", "mean_pinball"], na_position="last")
    best_by_method = df_sorted.groupby("method", as_index=False).first()
    best_path = out_root / "best_by_method_tune.csv"
    best_by_method.to_csv(best_path, index=False)
    print("Saved best-by-method tuning table to:", best_path)

    print("\nBest by method:")
    print(best_by_method[["method", "run", "rmse", "mae", "mean_pinball", "crps_approx"]])

    if not args.run_test:
        return

    if not args.test_start or not args.test_end:
        print("Skipping test stage because --test-start/--test-end were not provided.")
        return

    test_rows: list[dict[str, Any]] = []

    for _, best in tqdm(best_by_method.iterrows(), total=len(best_by_method), desc="QRF final test", unit="method"):
        run = {
            "method": best["method"],
            "predictors_path": Path(best["predictors_path"]),
            "n_predictors": int(best["n_predictors"]),
            "n_lags": int(best["n_lags"]),
            "n_y_lags": int(best["n_y_lags"]),
            "min_samples_leaf": int(best["min_samples_leaf"]),
            "max_features": normalize_max_features(best["max_features"]),
            "n_estimators": int(best["n_estimators"]),
            "run_name": str(best["run"]).replace("qrf_", "qrf_test_"),
            "outdir": out_root / "test" / str(best["method"]) / str(best["run"]).replace("qrf_", "qrf_test_"),
        }

        scores_path = run["outdir"] / "scores.json"

        if not (args.skip_existing and scores_path.exists()):
            cmd = [
                sys.executable,
                "scripts/qrf/run_qrf_pseudort.py",
                "--backend", "qrf",
                "--quantiles", args.quantiles,
                "--x-path", args.x_path,
                "--y-path", args.y_path,
                "--eval-start", args.test_start,
                "--eval-end", args.test_end,
                "--predictors-path", str(run["predictors_path"]),
                "--n-predictors", str(run["n_predictors"]),
                "--n-lags", str(run["n_lags"]),
                "--n-y-lags", str(run["n_y_lags"]),
                "--min-train-rows", str(args.min_train_rows),
                "--n-estimators", str(run["n_estimators"]),
                "--min-samples-leaf", str(run["min_samples_leaf"]),
                "--max-features", str(run["max_features"]),
                "--random-state", str(args.random_state),
                "--n-jobs", str(args.n_jobs),
                "--delay-style", args.delay_style,
                "--gdp-rel", str(args.gdp_rel),
                "--outdir", str(run["outdir"]),
            ]

            if args.delay_map_path:
                cmd += ["--delay-map-path", args.delay_map_path]

            cmd.append("--no-qe-leak" if args.no_qe_leak else "--no-no-qe-leak")
            cmd.append("--progress" if args.progress else "--no-progress")

            tqdm.write(f"RUN test {run['run_name']}")
            result = subprocess.run(cmd)

            if result.returncode != 0:
                failures.append(
                    {
                        "phase": "test",
                        "method": run["method"],
                        "run": run["run_name"],
                        "returncode": result.returncode,
                        "outdir": str(run["outdir"]),
                    }
                )
                continue

        if scores_path.exists():
            test_rows.append(_score_row(run, _read_scores(scores_path), phase="test"))

    test_summary_path = out_root / "grid_summary_test.csv"
    _write_csv(test_summary_path, test_rows)
    print("Saved test summary to:", test_summary_path)

    if failures:
        _write_csv(out_root / "grid_failures.csv", failures)


if __name__ == "__main__":
    main()
