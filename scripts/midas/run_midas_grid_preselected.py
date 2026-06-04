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
from tqdm import tqdm


VALID_WEIGHT_SCHEMES = {"beta", "exp_almon", "equal", "unrestricted"}
VALID_COMBINATIONS = {"mean", "median", "trimmed_mean", "inverse_rmse", "top_k"}


def _parse_csv(value: str | None) -> list[str]:
    if value is None:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_int_grid(value: str | None) -> list[int]:
    return [int(x) for x in _parse_csv(value)]


def _parse_float_grid(value: str | None) -> list[float]:
    return [float(x) for x in _parse_csv(value)]


def _parse_bool_grid(value: str | None) -> list[bool]:
    items = _parse_csv(value)
    out: list[bool] = []
    for item in items:
        low = item.lower()
        if low in {"1", "true", "yes", "y"}:
            out.append(True)
        elif low in {"0", "false", "no", "n"}:
            out.append(False)
        else:
            raise ValueError(f"Cannot parse boolean grid value: {item!r}")
    return out


def _safe_float_label(value: float) -> str:
    text = f"{float(value):g}".replace("-", "m").replace(".", "p")
    return text


def _method_map(pred_root: Path, methods: list[str], top_k: int) -> dict[str, Path]:
    out: dict[str, Path] = {}
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


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    return value


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_json_ready(data), f, indent=2)


def _score_row(run: dict[str, Any], scores: dict[str, Any], phase: str) -> dict[str, Any]:
    raw = scores.get("raw", {}) if isinstance(scores.get("raw"), dict) else {}
    standardized = scores.get("standardized", {}) if isinstance(scores.get("standardized"), dict) else {}

    return {
        "phase": phase,
        "method": run["method"],
        "run": run["run_name"],
        "n": scores.get("n"),
        "rmse": scores.get("rmse"),
        "mae": scores.get("mae"),
        "std_rmse": standardized.get("rmse", scores.get("rmse")),
        "std_mae": standardized.get("mae", scores.get("mae")),
        "raw_rmse": raw.get("rmse"),
        "raw_mae": raw.get("mae"),
        "n_monthly_lags": run["n_monthly_lags"],
        "n_y_lags": run["n_y_lags"],
        "weight_scheme": run["weight_scheme"],
        "combination": run["combination"],
        "trimmed_alpha": run["trimmed_alpha"],
        "combination_top_k": run["combination_top_k"],
        "ridge_alpha": run["ridge_alpha"],
        "validation_tail_rows": run["validation_tail_rows"],
        "moq_specific": run["moq_specific"],
        "n_predictors": run["n_predictors"],
        "max_iter": run["max_iter"],
        "n_starts": run["n_starts"],
        "accept_nonconverged": run["accept_nonconverged"],
        "predictors_path": str(run["predictors_path"]),
        "outdir": str(run["outdir"]),
        "scores_path": str(run["outdir"] / "scores.json"),
    }


def _inventory_row(run: dict[str, Any], phase: str) -> dict[str, Any]:
    scores_path = run["outdir"] / "scores.json"
    completed = scores_path.exists()
    return {
        "phase": phase,
        "method": run["method"],
        "run": run["run_name"],
        "completed": completed,
        "status": "completed" if completed else "missing",
        "n_monthly_lags": run["n_monthly_lags"],
        "n_y_lags": run["n_y_lags"],
        "weight_scheme": run["weight_scheme"],
        "combination": run["combination"],
        "trimmed_alpha": run["trimmed_alpha"],
        "combination_top_k": run["combination_top_k"],
        "ridge_alpha": run["ridge_alpha"],
        "validation_tail_rows": run["validation_tail_rows"],
        "moq_specific": run["moq_specific"],
        "n_predictors": run["n_predictors"],
        "max_iter": run["max_iter"],
        "n_starts": run["n_starts"],
        "accept_nonconverged": run["accept_nonconverged"],
        "predictors_path": str(run["predictors_path"]),
        "outdir": str(run["outdir"]),
        "scores_path": str(scores_path),
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


def _write_inventory(out_root: Path, runs: list[dict[str, Any]], phase: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = [_inventory_row(run, phase=phase) for run in runs]
    df = pd.DataFrame(rows)
    completed = df[df["completed"]].copy()
    missing = df[~df["completed"]].copy()

    prefix = "tune" if phase == "tune" else "test"
    df.to_csv(out_root / f"{prefix}_expected_runs.csv", index=False)
    completed.to_csv(out_root / f"{prefix}_completed_runs.csv", index=False)
    missing.to_csv(out_root / f"{prefix}_missing_runs.csv", index=False)

    if phase == "tune":
        completed.to_csv(out_root / "completed_grid_runs.csv", index=False)
        missing.to_csv(out_root / "missing_grid_runs.csv", index=False)

    return completed, missing


def _run_pseudort(
    *,
    args: argparse.Namespace,
    run: dict[str, Any],
    phase: str,
    eval_start: str,
    eval_end: str,
    skip_completed: bool,
    failures: list[dict[str, Any]],
) -> bool:
    scores_path = run["outdir"] / "scores.json"

    if skip_completed and scores_path.exists():
        tqdm.write(f"SKIP existing {phase} {run['run_name']}")
        return True

    cmd = [
        sys.executable,
        "scripts/midas/run_midas_pseudort.py",
        "--x-path", args.x_path,
        "--y-path", args.y_path,
        "--eval-start", eval_start,
        "--eval-end", eval_end,
        "--predictors-path", str(run["predictors_path"]),
        "--n-predictors", str(run["n_predictors"]),
        "--n-monthly-lags", str(run["n_monthly_lags"]),
        "--n-y-lags", str(run["n_y_lags"]),
        "--min-train-rows", str(args.min_train_rows),
        "--n-jobs", str(args.n_jobs),
        "--weight-scheme", str(run["weight_scheme"]),
        "--combination", str(run["combination"]),
        "--trimmed-alpha", str(run["trimmed_alpha"]),
        "--max-iter", str(run["max_iter"]),
        "--tol", str(args.tol),
        "--n-starts", str(run["n_starts"]),
        "--parameter-bound", str(args.parameter_bound),
        "--fallback-weight-scheme", str(args.fallback_weight_scheme),
        "--ridge-alpha", str(run["ridge_alpha"]),
        "--validation-tail-rows", str(run["validation_tail_rows"]),
        "--delay-style", args.delay_style,
        "--gdp-rel", str(args.gdp_rel),
        "--outdir", str(run["outdir"]),
    ]

    if run["combination"] == "top_k" and run["combination_top_k"] is not None:
        cmd += ["--top-k", str(run["combination_top_k"])]

    cmd.append("--moq-specific" if run["moq_specific"] else "--no-moq-specific")
    cmd.append("--warm-start" if args.warm_start else "--no-warm-start")
    cmd.append("--accept-nonconverged" if run["accept_nonconverged"] else "--no-accept-nonconverged")
    cmd.append("--no-qe-leak" if args.no_qe_leak else "--no-no-qe-leak")
    cmd.append("--progress" if args.progress else "--no-progress")
    cmd.append("--write-diagnostics" if args.write_diagnostics else "--no-write-diagnostics")

    if args.delay_map_path:
        cmd += ["--delay-map-path", args.delay_map_path]
    if args.y_raw_path:
        cmd += ["--y-raw-path", args.y_raw_path]
    if args.target_scaler_path:
        cmd += ["--target-scaler-path", args.target_scaler_path]
    if args.target_mean is not None:
        cmd += ["--target-mean", str(args.target_mean)]
    if args.target_std is not None:
        cmd += ["--target-std", str(args.target_std)]

    tqdm.write(f"RUN {phase} {run['run_name']}")
    result = subprocess.run(cmd)

    if result.returncode != 0:
        failures.append(
            {
                "phase": phase,
                "method": run["method"],
                "run": run["run_name"],
                "returncode": result.returncode,
                "outdir": str(run["outdir"]),
                "scores_path": str(scores_path),
            }
        )
        return False

    return scores_path.exists()


def _run_from_summary_row(best: pd.Series, out_root: Path, scope: str) -> dict[str, Any]:
    original_run = str(best["run"])
    test_name = f"midas_test_{scope}_{original_run.removeprefix('midas_')}"
    return {
        "method": str(best["method"]),
        "predictors_path": Path(str(best["predictors_path"])),
        "n_predictors": int(best["n_predictors"]),
        "n_monthly_lags": int(best["n_monthly_lags"]),
        "n_y_lags": int(best["n_y_lags"]),
        "weight_scheme": str(best["weight_scheme"]),
        "combination": str(best["combination"]),
        "trimmed_alpha": float(best["trimmed_alpha"]),
        "combination_top_k": None if pd.isna(best.get("combination_top_k")) else int(best["combination_top_k"]),
        "ridge_alpha": float(best["ridge_alpha"]),
        "validation_tail_rows": int(best["validation_tail_rows"]),
        "moq_specific": bool(best["moq_specific"]),
        "max_iter": int(best["max_iter"]),
        "n_starts": int(best["n_starts"]),
        "accept_nonconverged": bool(best["accept_nonconverged"]),
        "run_name": test_name,
        "outdir": out_root / "test" / scope / test_name,
        "source_tune_run": original_run,
        "source_tune_outdir": str(best["outdir"]),
    }


def _metric_sort_columns(metric: str) -> list[str]:
    if metric in {"rmse", "mae", "raw_rmse", "raw_mae"}:
        return [metric, "rmse", "mae"]
    return ["rmse", "mae"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Grid-search MIDAS separately for each predictor-selection method."
    )

    p.add_argument("--x-path", required=True)
    p.add_argument("--y-path", required=True)
    p.add_argument("--pred-root", required=True)
    p.add_argument("--out-root", required=True)

    p.add_argument("--methods", default="sis,tstat_lm,lars_lm,lars_tscv")
    p.add_argument("--top-k", type=int, default=20, help="Number of predictors loaded from each selected-predictor file.")

    p.add_argument("--tune-start", required=True)
    p.add_argument("--tune-end", required=True)
    p.add_argument("--test-start", default=None)
    p.add_argument("--test-end", default=None)

    p.add_argument("--n-monthly-lags-grid", default="3,6")
    p.add_argument("--n-y-lags-grid", default="0,1,2")
    p.add_argument("--weight-schemes-grid", default="beta,exp_almon,equal,unrestricted")
    p.add_argument("--combination-grid", default="mean,median,trimmed_mean,inverse_rmse")
    p.add_argument("--trimmed-alpha-grid", default="0.10")
    p.add_argument("--combination-top-k-grid", default="5,10")
    p.add_argument("--ridge-alpha-grid", default="0.0")
    p.add_argument("--validation-tail-rows-grid", default="0")
    p.add_argument("--moq-specific-grid", default="false")
    p.add_argument("--n-predictors-grid", default=None)

    p.add_argument("--min-train-rows", type=int, default=36)
    p.add_argument(
        "--n-jobs",
        type=int,
        default=1,
        help="Parallel jobs over predictor-specific MIDAS fits inside each vintage. Use -1 for all cores.",
    )
    p.add_argument("--max-iter-grid", default="200")
    p.add_argument("--tol", type=float, default=1e-6)
    p.add_argument("--n-starts-grid", default="4")
    p.add_argument("--parameter-bound", type=float, default=3.0)
    p.add_argument("--fallback-weight-scheme", default="equal")
    p.add_argument("--accept-nonconverged-grid", default="true")

    p.add_argument("--delay-style", default="none")
    p.add_argument("--delay-map-path", default=None)
    p.add_argument("--gdp-rel", type=int, default=0)
    p.add_argument("--no-qe-leak", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--warm-start", action=argparse.BooleanOptionalAction, default=True)

    p.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--force", action="store_true", help="Rerun configurations even when scores.json already exists.")
    p.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Deprecated alias. Prefer --resume/--no-resume or --force.",
    )
    p.add_argument(
        "--selection-metric",
        choices=["rmse", "mae", "raw_rmse", "raw_mae"],
        default="rmse",
    )
    p.add_argument("--run-test", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument(
        "--test-scope",
        choices=["global", "by_method", "both"],
        default="global",
    )
    p.add_argument("--write-diagnostics", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--y-raw-path", default=None)
    p.add_argument("--target-scaler-path", default=None)
    p.add_argument("--target-mean", type=float, default=None)
    p.add_argument("--target-std", type=float, default=None)

    return p


def main() -> None:
    args = build_parser().parse_args()

    pred_root = Path(args.pred_root)
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    methods = _parse_csv(args.methods)
    predictor_paths = _method_map(pred_root, methods, args.top_k)

    n_predictors_grid = _parse_int_grid(args.n_predictors_grid) if args.n_predictors_grid else [args.top_k]
    weight_schemes = _parse_csv(args.weight_schemes_grid)
    combinations = _parse_csv(args.combination_grid)

    invalid_weight = [x for x in weight_schemes if x not in VALID_WEIGHT_SCHEMES]
    if invalid_weight:
        raise ValueError(f"Invalid weight schemes: {invalid_weight}")

    invalid_combinations = [x for x in combinations if x not in VALID_COMBINATIONS]
    if invalid_combinations:
        raise ValueError(f"Invalid combinations: {invalid_combinations}")

    runs: list[dict[str, Any]] = []

    for method, predictors_path in predictor_paths.items():
        for (
            n_predictors,
            n_monthly_lags,
            n_y_lags,
            weight_scheme,
            combination,
            trimmed_alpha,
            ridge_alpha,
            validation_tail_rows,
            moq_specific,
            max_iter,
            n_starts,
            accept_nonconverged,
        ) in product(
            n_predictors_grid,
            _parse_int_grid(args.n_monthly_lags_grid),
            _parse_int_grid(args.n_y_lags_grid),
            weight_schemes,
            combinations,
            _parse_float_grid(args.trimmed_alpha_grid),
            _parse_float_grid(args.ridge_alpha_grid),
            _parse_int_grid(args.validation_tail_rows_grid),
            _parse_bool_grid(args.moq_specific_grid),
            _parse_int_grid(args.max_iter_grid),
            _parse_int_grid(args.n_starts_grid),
            _parse_bool_grid(args.accept_nonconverged_grid),
        ):
            combination_top_k_values: list[int | None]
            if combination == "top_k":
                combination_top_k_values = _parse_int_grid(args.combination_top_k_grid)
                if not combination_top_k_values:
                    raise ValueError("combination='top_k' requires --combination-top-k-grid")
            else:
                combination_top_k_values = [None]

            for combination_top_k in combination_top_k_values:
                run_name = (
                    f"midas_{method}_top{n_predictors}"
                    f"_xl{n_monthly_lags}"
                    f"_yl{n_y_lags}"
                    f"_{weight_scheme}"
                    f"_{combination}"
                    f"_ta{_safe_float_label(trimmed_alpha)}"
                    f"_ridge{_safe_float_label(ridge_alpha)}"
                    f"_vtr{validation_tail_rows}"
                    f"_moq{int(moq_specific)}"
                    f"_mi{max_iter}"
                    f"_ns{n_starts}"
                    f"_anc{int(accept_nonconverged)}"
                )
                if combination_top_k is not None:
                    run_name += f"_ctop{combination_top_k}"

                runs.append(
                    {
                        "method": method,
                        "predictors_path": predictors_path,
                        "n_predictors": n_predictors,
                        "n_monthly_lags": n_monthly_lags,
                        "n_y_lags": n_y_lags,
                        "weight_scheme": weight_scheme,
                        "combination": combination,
                        "trimmed_alpha": trimmed_alpha,
                        "combination_top_k": combination_top_k,
                        "ridge_alpha": ridge_alpha,
                        "validation_tail_rows": validation_tail_rows,
                        "moq_specific": moq_specific,
                        "max_iter": max_iter,
                        "n_starts": n_starts,
                        "accept_nonconverged": accept_nonconverged,
                        "run_name": run_name,
                        "outdir": out_root / "tune" / method / run_name,
                    }
                )

    print("Planned tuning runs:", len(runs))
    print("Output root:", out_root)

    skip_completed = bool(args.resume)
    if args.skip_existing is not None:
        skip_completed = bool(args.skip_existing)
    if args.force:
        skip_completed = False

    completed_start, missing_start = _write_inventory(out_root, runs, phase="tune")
    print("Completed tuning runs at start:", len(completed_start))
    print("Missing tuning runs at start:", len(missing_start))
    print("Resume mode:", skip_completed)

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for run in tqdm(runs, desc="MIDAS validation grid", unit="run"):
        _run_pseudort(
            args=args,
            run=run,
            phase="tune",
            eval_start=args.tune_start,
            eval_end=args.tune_end,
            skip_completed=skip_completed,
            failures=failures,
        )

        scores_path = run["outdir"] / "scores.json"
        if scores_path.exists():
            rows.append(_score_row(run, _read_scores(scores_path), phase="tune"))

        _write_inventory(out_root, runs, phase="tune")

    completed_end, missing_end = _write_inventory(out_root, runs, phase="tune")

    summary_path = out_root / "grid_summary_tune.csv"
    complete_summary_path = out_root / "grid_summary_complete.csv"
    _write_csv(summary_path, rows)
    _write_csv(complete_summary_path, rows)

    if failures:
        _write_csv(out_root / "grid_failures.csv", failures)

    print("Saved tuning summary to:", summary_path)
    print("Saved complete-summary alias to:", complete_summary_path)
    print("Saved completed inventory to:", out_root / "completed_grid_runs.csv")
    print("Saved missing inventory to:", out_root / "missing_grid_runs.csv")
    print("Completed tuning runs at end:", len(completed_end))
    print("Missing tuning runs at end:", len(missing_end))

    if not rows:
        print("No successful tuning runs.")
        return

    df = pd.DataFrame(rows)
    metric = args.selection_metric
    df_sorted = df.sort_values(_metric_sort_columns(metric), na_position="last")

    best_global = df_sorted.iloc[0].copy()
    best_global_df = pd.DataFrame([best_global.to_dict()])
    best_global_csv = out_root / "best_global_tune.csv"
    best_global_json = out_root / "best_global_config.json"
    best_global_df.to_csv(best_global_csv, index=False)
    _write_json(best_global_json, best_global.to_dict())

    method_sort_cols = ["method", *_metric_sort_columns(metric)]
    best_by_method = (
        df.sort_values(method_sort_cols, na_position="last")
        .groupby("method", as_index=False)
        .first()
    )
    best_by_method_path = out_root / "best_by_method_tune.csv"
    best_by_method.to_csv(best_by_method_path, index=False)

    print("Saved global-best tuning table to:", best_global_csv)
    print("Saved global-best config to:", best_global_json)
    print("Saved best-by-method tuning table to:", best_by_method_path)

    report_cols = [
        "method",
        "run",
        "rmse",
        "mae",
        "raw_rmse",
        "raw_mae",
        "n_monthly_lags",
        "n_y_lags",
        "weight_scheme",
        "combination",
        "validation_tail_rows",
        "moq_specific",
    ]

    print("\nBest global validation configuration:")
    print(best_global_df[[c for c in report_cols if c in best_global_df.columns]])

    print("\nBest by method:")
    print(best_by_method[[c for c in report_cols if c in best_by_method.columns]])

    if len(missing_end) > 0:
        print("\nGrid is incomplete. Final test stage is skipped until missing tuning runs are completed.")
        return

    if not args.run_test:
        return

    if not args.test_start or not args.test_end:
        print("Skipping test stage because --test-start/--test-end were not provided.")
        return

    test_runs: list[dict[str, Any]] = []

    if args.test_scope in {"global", "both"}:
        test_runs.append(_run_from_summary_row(best_global, out_root, scope="global"))

    if args.test_scope in {"by_method", "both"}:
        for _, best in best_by_method.iterrows():
            test_runs.append(_run_from_summary_row(best, out_root, scope=f"method_{best['method']}"))

    _write_inventory(out_root, test_runs, phase="test")

    test_rows: list[dict[str, Any]] = []
    for run in tqdm(test_runs, desc="MIDAS final test", unit="run"):
        _run_pseudort(
            args=args,
            run=run,
            phase="test",
            eval_start=args.test_start,
            eval_end=args.test_end,
            skip_completed=skip_completed,
            failures=failures,
        )

        scores_path = run["outdir"] / "scores.json"
        if scores_path.exists():
            test_rows.append(_score_row(run, _read_scores(scores_path), phase="test"))

        _write_inventory(out_root, test_runs, phase="test")

    test_summary_path = out_root / "grid_summary_test.csv"
    _write_csv(test_summary_path, test_rows)
    _write_inventory(out_root, test_runs, phase="test")

    if failures:
        _write_csv(out_root / "grid_failures.csv", failures)

    print("Saved test summary to:", test_summary_path)


if __name__ == "__main__":
    main()
