from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qrf_pipeline.config import QRFConfig
from qrf_pipeline.diagnostics import summarize_by_month_of_quarter, summarize_by_target_quarter
from qrf_pipeline.importance import write_importance_outputs
from qrf_pipeline.pseudort import QRFPseudoRTConfig, run_qrf_pseudort
from qrf_pipeline.raw_scale import load_target_scaler
from rt_benchmarks.config_io import load_delay_map, select_predictors
from rt_benchmarks.data_io import load_panel_csv, load_target_csv
from rt_benchmarks.outputs import write_predictions, write_run_config, write_scores
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


def _parse_predictors_arg(value: str | None) -> list[str] | None:
    if value is None:
        return None

    items = [x.strip() for x in value.split(",")]
    items = [x for x in items if x]

    return items or None


def _parse_quantiles(value: str) -> tuple[float, ...]:
    items = [x.strip() for x in value.split(",") if x.strip()]
    quantiles = tuple(float(x) for x in items)

    if not quantiles:
        raise ValueError("--quantiles cannot be empty")

    for q in quantiles:
        if q <= 0.0 or q >= 1.0:
            raise ValueError(f"Invalid quantile {q}. Quantiles must be inside (0, 1).")

    if tuple(sorted(quantiles)) != quantiles:
        raise ValueError("--quantiles must be sorted increasingly")

    return quantiles


def _parse_max_features(value: str) -> int | float | str | None:
    value = str(value).strip()

    if value.lower() in {"none", "null"}:
        return None

    if value in {"sqrt", "log2"}:
        return value

    try:
        if "." in value:
            out = float(value)
            if out <= 0.0:
                raise ValueError
            return out

        out_int = int(value)
        if out_int <= 0:
            raise ValueError
        return out_int

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--max-features must be one of: sqrt, log2, None, positive int, or positive float."
        ) from exc


def _build_qrf_config(
    args: argparse.Namespace,
    predictors: list[str],
    quantiles: tuple[float, ...],
) -> QRFConfig:
    kwargs: dict[str, Any] = {
        "predictors": predictors,
        "n_lags": args.n_lags,
        "n_y_lags": args.n_y_lags,
        "min_train_rows": args.min_train_rows,
        "n_estimators": args.n_estimators,
        "min_samples_leaf": args.min_samples_leaf,
        "max_features": args.max_features,
        "random_state": args.random_state,
        "backend": args.backend,
        "quantiles": quantiles,
        "n_jobs": args.n_jobs,
        "save_feature_importance": bool(args.save_feature_importance),
    }

    sig = inspect.signature(QRFConfig)
    supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
    cfg = QRFConfig(**supported)

    # Compatibility for older dataclass signatures.
    for k, v in kwargs.items():
        if not hasattr(cfg, k):
            setattr(cfg, k, v)

    return cfg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run QRF / RF pseudo-real-time nowcast benchmark."
    )

    parser.add_argument("--x-path", required=True)
    parser.add_argument("--y-path", required=True)
    parser.add_argument("--outdir", default="outputs/qrf/basic_nowcast")

    parser.add_argument("--eval-start", required=True)
    parser.add_argument("--eval-end", required=True)

    parser.add_argument(
        "--backend",
        choices=["rf_point", "qrf"],
        default="rf_point",
        help="rf_point uses point Random Forest; qrf uses quantile forest.",
    )

    parser.add_argument(
        "--quantiles",
        default="0.10,0.25,0.50,0.75,0.90",
        help="Comma-separated quantiles used when backend='qrf'.",
    )

    parser.add_argument("--predictors", default=None)
    parser.add_argument("--predictors-path", default=None)
    parser.add_argument("--predictors-col", default=None)
    parser.add_argument("--n-predictors", type=int, default=20)

    parser.add_argument("--n-lags", type=int, default=3)
    parser.add_argument("--n-y-lags", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=36)

    parser.add_argument("--n-estimators", type=int, default=500)
    parser.add_argument("--min-samples-leaf", type=int, default=5)
    parser.add_argument("--max-features", type=_parse_max_features, default="sqrt")
    parser.add_argument("--random-state", type=int, default=123)
    parser.add_argument("--n-jobs", type=int, default=-1)

    parser.add_argument("--delay-style", default="none")
    parser.add_argument("--delay-map-path", default=None)
    parser.add_argument("--gdp-rel", type=int, default=0)

    parser.add_argument(
        "--no-qe-leak",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prevent quarter-end nowcast from using the just-released quarterly target.",
    )

    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show a tqdm progress bar over pseudo-real-time vintages.",
    )

    parser.add_argument(
        "--save-feature-importance",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save feature_importance.csv and feature_importance_by_predictor.csv when available.",
    )

    parser.add_argument(
        "--write-diagnostics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save summary_by_moq.csv and summary_by_target_quarter.csv.",
    )

    parser.add_argument(
        "--y-raw-path",
        default=None,
        help="Optional raw-scale target CSV. If supplied, raw-scale scores use this actual target.",
    )

    parser.add_argument(
        "--target-scaler-path",
        default=None,
        help="Optional JSON scaler for inverse-transforming standardized predictions to raw scale.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    X = load_panel_csv(args.x_path)
    y = load_target_csv(args.y_path)

    y_raw = load_target_csv(args.y_raw_path) if args.y_raw_path else None

    pred_to_raw_fn = None
    if args.target_scaler_path:
        scaler = load_target_scaler(args.target_scaler_path)

        def pred_to_raw_fn(value, target_date):
            return scaler.inverse(value)

    predictors = select_predictors(
        X_columns=list(X.columns),
        predictors=_parse_predictors_arg(args.predictors),
        predictors_path=args.predictors_path,
        predictors_col=args.predictors_col,
        n_predictors=args.n_predictors,
    )

    delay_map = load_delay_map(args.delay_map_path)

    vintage_cfg = VintageConfig(
        delay_style=args.delay_style,
        delay_map=delay_map,
        gdp_rel=args.gdp_rel,
        no_qe_leak=bool(args.no_qe_leak),
    )

    sample_cfg = SampleBuilderConfig(
        vintage_cfg=vintage_cfg,
        min_train_rows=args.min_train_rows,
        include_row_metadata=True,
    )

    quantiles = _parse_quantiles(args.quantiles)

    model_cfg = _build_qrf_config(
        args=args,
        predictors=predictors,
        quantiles=quantiles,
    )

    eval_cfg = QRFPseudoRTConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        horizons=("now",),
        drop_invalid_rows=True,
        show_progress=bool(args.progress),
        collect_feature_importance=bool(args.save_feature_importance),
    )

    pred_df, scores = run_qrf_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
        y_raw_full=y_raw,
        pred_to_raw_fn=pred_to_raw_fn,
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pred_path = write_predictions(pred_df, outdir)
    scores_path = write_scores(scores, outdir)

    feature_importance_paths = None
    if args.save_feature_importance:
        imp = pred_df.attrs.get("feature_importance")
        if imp is not None and not imp.empty:
            feature_importance_paths = write_importance_outputs(imp, outdir)

    diagnostics_paths = {}
    if args.write_diagnostics:
        moq = summarize_by_month_of_quarter(pred_df, quantiles=quantiles)
        tq = summarize_by_target_quarter(pred_df)

        moq_path = outdir / "summary_by_moq.csv"
        tq_path = outdir / "summary_by_target_quarter.csv"

        moq.to_csv(moq_path, index=False)
        tq.to_csv(tq_path, index=False)

        diagnostics_paths = {
            "summary_by_moq": str(moq_path),
            "summary_by_target_quarter": str(tq_path),
        }

    run_cfg_path = write_run_config(
        {
            "x_path": args.x_path,
            "y_path": args.y_path,
            "y_raw_path": args.y_raw_path,
            "target_scaler_path": args.target_scaler_path,
            "outdir": str(outdir),
            "eval_start": args.eval_start,
            "eval_end": args.eval_end,
            "horizons": ["now"],
            "backend": args.backend,
            "quantiles": list(quantiles),
            "predictors": predictors,
            "predictors_path": args.predictors_path,
            "predictors_col": args.predictors_col,
            "n_predictors": args.n_predictors,
            "n_lags": args.n_lags,
            "n_y_lags": args.n_y_lags,
            "min_train_rows": args.min_train_rows,
            "n_estimators": args.n_estimators,
            "min_samples_leaf": args.min_samples_leaf,
            "max_features": args.max_features,
            "random_state": args.random_state,
            "n_jobs": args.n_jobs,
            "delay_style": args.delay_style,
            "delay_map_path": args.delay_map_path,
            "gdp_rel": args.gdp_rel,
            "no_qe_leak": bool(args.no_qe_leak),
            "progress": bool(args.progress),
            "save_feature_importance": bool(args.save_feature_importance),
            "write_diagnostics": bool(args.write_diagnostics),
        },
        outdir,
    )

    print("QRF pseudo-RT run completed")
    print("Backend:", args.backend)
    print("X shape:", X.shape)
    print("y length:", len(y))
    print("Predictors used:", predictors)
    print("Max features:", args.max_features, type(args.max_features).__name__)
    print("n_jobs:", args.n_jobs)
    print("Prediction rows:", len(pred_df))
    print("Scores:", scores)
    print("Saved predictions to:", pred_path)
    print("Saved scores to:", scores_path)
    print("Saved run config to:", run_cfg_path)

    if feature_importance_paths:
        print("Saved feature importance to:", feature_importance_paths[0])
        print("Saved predictor importance to:", feature_importance_paths[1])

    for name, path in diagnostics_paths.items():
        print(f"Saved {name} to:", path)


if __name__ == "__main__":
    main()
