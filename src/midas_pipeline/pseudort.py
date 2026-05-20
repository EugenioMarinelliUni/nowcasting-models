from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

from midas_pipeline.combine import combine_midas_forecasts
from midas_pipeline.config import MIDASConfig
from midas_pipeline.feature_builder import extract_midas_features
from midas_pipeline.fit import fit_univariate_midas_with_diagnostics
from midas_pipeline.predict import predict_univariate_midas
from rt_benchmarks.metrics import compute_basic_metrics
from rt_benchmarks.sample_builder import (
    SampleBuilderConfig,
    build_current_feature_row,
    build_direct_training_sample,
)
from rt_benchmarks.targeting import horizon_target_date, month_of_quarter


@dataclass(frozen=True)
class MIDASPseudoRTConfig:
    eval_start: str
    eval_end: str
    horizons: tuple[str, ...] = ("now",)
    drop_invalid_rows: bool = True
    show_progress: bool = False


def _to_month_start(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if pd.isna(out):
        raise ValueError("timestamp cannot be NaT")
    return pd.Timestamp(year=out.year, month=out.month, day=1)


def _normalize_monthly_frame_index(X: pd.DataFrame) -> pd.DataFrame:
    X = X.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(X.index))
    if idx.hasnans:
        raise ValueError("X index contains NaT values")
    X.index = idx.to_period("M").to_timestamp(how="start")
    return X.sort_index()


def _normalize_monthly_series_index(y: pd.Series) -> pd.Series:
    y = y.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(y.index))
    if idx.hasnans:
        raise ValueError("y index contains NaT values")
    y.index = idx.to_period("M").to_timestamp(how="start")
    return y.sort_index()


def _midas_feature_fn_factory(
    predictor: str,
    model_cfg: MIDASConfig,
) -> Callable:
    def _feature_fn(
        Xv: pd.DataFrame,
        yv: pd.Series,
        vintage_date: pd.Timestamp,
        horizon: str,
    ) -> dict[str, float] | None:
        return extract_midas_features(
            Xv=Xv,
            yv=yv,
            predictor=predictor,
            n_monthly_lags=model_cfg.n_monthly_lags,
            n_y_lags=model_cfg.n_y_lags,
        )

    return _feature_fn


def _sanitize_training_sample(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    drop_invalid_rows: bool,
) -> tuple[pd.DataFrame, pd.Series]:
    if X_train.empty or y_train.empty:
        return X_train.copy(), y_train.copy()

    X_num = X_train.apply(pd.to_numeric, errors="coerce")
    X_num = X_num.dropna(axis=1, how="all")
    y_num = pd.to_numeric(y_train, errors="coerce")

    if not drop_invalid_rows:
        return X_num.reset_index(drop=True), y_num.reset_index(drop=True)

    mask = np.isfinite(y_num.to_numpy(dtype=float))
    if X_num.shape[1] > 0:
        mask &= np.isfinite(X_num.to_numpy(dtype=float)).all(axis=1)

    return X_num.loc[mask].reset_index(drop=True), y_num.loc[mask].reset_index(drop=True)


def _sanitize_current_row(
    X_now: pd.DataFrame | None,
    reference_columns: list[str] | None = None,
) -> pd.DataFrame | None:
    if X_now is None:
        return None

    X_now = X_now.apply(pd.to_numeric, errors="coerce")
    X_now = X_now.dropna(axis=1, how="all")

    if reference_columns is not None:
        if not reference_columns:
            return None
        missing = [c for c in reference_columns if c not in X_now.columns]
        if missing:
            return None
        X_now = X_now[reference_columns]

    if X_now.empty:
        return None

    arr = X_now.to_numpy(dtype=float)
    if not np.isfinite(arr).all():
        return None

    return X_now


def _get_actual_value(y: pd.Series, target_date: pd.Timestamp) -> float:
    target_date = _to_month_start(target_date)
    if target_date not in y.index:
        return float("nan")
    val = y.loc[target_date]
    return float(val) if not pd.isna(val) else float("nan")


def _iter_eval_months(eval_months: pd.DatetimeIndex, show_progress: bool):
    if not show_progress:
        return eval_months

    try:
        from tqdm import tqdm
    except ImportError:
        return eval_months

    return tqdm(eval_months, desc="MIDAS pseudo-RT", unit="vintage")


def _apply_moq_filter(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    meta: pd.DataFrame,
    moq: int,
    enabled: bool,
) -> tuple[pd.DataFrame, pd.Series]:
    if not enabled:
        return X_train, y_train

    if meta is None or "moq" not in meta.columns:
        return X_train.iloc[0:0], y_train.iloc[0:0]

    mask = meta["moq"].astype(int).to_numpy() == int(moq)
    return X_train.iloc[mask], y_train.iloc[mask]


def _validation_split(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    validation_tail_rows: int,
    min_train_rows: int,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    if validation_tail_rows <= 0:
        return X_train, y_train, X_train.iloc[0:0], y_train.iloc[0:0]

    if len(X_train) <= min_train_rows + validation_tail_rows:
        return X_train, y_train, X_train.iloc[0:0], y_train.iloc[0:0]

    n_val = int(validation_tail_rows)

    X_fit = X_train.iloc[:-n_val].reset_index(drop=True)
    y_fit = y_train.iloc[:-n_val].reset_index(drop=True)
    X_val = X_train.iloc[-n_val:].reset_index(drop=True)
    y_val = y_train.iloc[-n_val:].reset_index(drop=True)

    return X_fit, y_fit, X_val, y_val


def _compute_validation_rmse(model, X_val: pd.DataFrame, y_val: pd.Series) -> float:
    if X_val.empty or y_val.empty:
        return float("nan")

    preds = []
    actuals = []

    for i in range(len(X_val)):
        try:
            pred = predict_univariate_midas(model, X_val.iloc[i])
            actual = float(y_val.iloc[i])
        except Exception:
            continue

        if np.isfinite(pred) and np.isfinite(actual):
            preds.append(float(pred))
            actuals.append(float(actual))

    if not preds:
        return float("nan")

    p = np.asarray(preds, dtype=float)
    a = np.asarray(actuals, dtype=float)

    return float(np.sqrt(np.mean((p - a) ** 2)))


def _serialize_dict(obj: dict[str, float] | None) -> str:
    return json.dumps(obj or {})


def _serialize_list(obj) -> str:
    if obj is None:
        return "[]"
    return json.dumps(obj)


def run_midas_pseudort(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_cfg: MIDASPseudoRTConfig,
    sample_cfg: SampleBuilderConfig,
    model_cfg: MIDASConfig,
    *,
    y_raw_full: pd.Series | None = None,
    pred_to_raw_fn: Callable[[float, pd.Timestamp], float] | None = None,
    return_details: bool = False,
):
    model_cfg.validate()

    X_full = _normalize_monthly_frame_index(X_full)
    y_full = _normalize_monthly_series_index(y_full)

    if y_raw_full is not None:
        y_raw_full = _normalize_monthly_series_index(y_raw_full)

    eval_start = _to_month_start(eval_cfg.eval_start)
    eval_end = _to_month_start(eval_cfg.eval_end)

    eval_months = X_full.index[(X_full.index >= eval_start) & (X_full.index <= eval_end)]
    if len(eval_months) == 0:
        raise ValueError("No evaluation months found in the requested eval window")

    min_train_rows = max(sample_cfg.min_train_rows, model_cfg.min_train_rows)

    rows: list[dict[str, Any]] = []
    diagnostics_rows: list[dict[str, Any]] = []
    lag_weight_rows: list[dict[str, Any]] = []
    predictor_forecast_rows: list[dict[str, Any]] = []

    warm_params: dict[tuple[str, str], dict[str, float]] = {}

    for eval_date in _iter_eval_months(eval_months, eval_cfg.show_progress):
        eval_ms = _to_month_start(eval_date)

        for horizon in eval_cfg.horizons:
            if horizon != "now":
                raise NotImplementedError("MIDAS currently supports horizon='now' only")

            target_date = _to_month_start(horizon_target_date(eval_ms, horizon))
            moq = month_of_quarter(eval_ms)
            actual = _get_actual_value(y_full, target_date)

            forecast_records: list[dict[str, Any]] = []

            current_diag_indices: list[int] = []
            current_forecast_indices: list[int] = []

            for predictor in model_cfg.predictors:
                feature_fn = _midas_feature_fn_factory(predictor, model_cfg)

                base = {
                    "eval_date": eval_ms,
                    "target_date": target_date,
                    "horizon": horizon,
                    "moq": moq,
                    "predictor": predictor,
                }

                X_train, y_train, meta = build_direct_training_sample(
                    X_full=X_full,
                    y_full=y_full,
                    eval_date=eval_ms,
                    horizon=horizon,
                    cfg=sample_cfg,
                    feature_fn=feature_fn,
                )

                X_train, y_train = _apply_moq_filter(
                    X_train=X_train,
                    y_train=y_train,
                    meta=meta,
                    moq=moq,
                    enabled=model_cfg.moq_specific,
                )

                X_train, y_train = _sanitize_training_sample(
                    X_train=X_train,
                    y_train=y_train,
                    drop_invalid_rows=eval_cfg.drop_invalid_rows,
                )

                def add_failed(status: str) -> None:
                    diag = {
                        **base,
                        "requested_weight_scheme": model_cfg.weight_scheme,
                        "actual_weight_scheme": None,
                        "converged": False,
                        "used_fallback": False,
                        "objective_value": np.nan,
                        "in_sample_rmse": np.nan,
                        "validation_rmse": np.nan,
                        "n_iter": 0,
                        "n_train_rows": int(len(X_train)),
                        "n_features": int(X_train.shape[1]) if not X_train.empty else 0,
                        "status_message": status,
                        "weight_params": "{}",
                        "best_theta": "[]",
                        "best_start_index": np.nan,
                        "all_start_objectives": "[]",
                        "pred": np.nan,
                        "used_in_combination": False,
                        "combination_weight": np.nan,
                        "selection_rank": np.nan,
                    }
                    diagnostics_rows.append(diag)
                    current_diag_indices.append(len(diagnostics_rows) - 1)

                    fc = {
                        **base,
                        "pred": np.nan,
                        "actual": actual,
                        "abs_err": np.nan,
                        "in_sample_rmse": np.nan,
                        "validation_rmse": np.nan,
                        "used_in_combination": False,
                        "combination_weight": np.nan,
                        "selection_rank": np.nan,
                        "status_message": status,
                    }
                    predictor_forecast_rows.append(fc)
                    current_forecast_indices.append(len(predictor_forecast_rows) - 1)

                if len(X_train) < min_train_rows:
                    add_failed("insufficient training rows")
                    continue

                feature_columns = list(X_train.columns)

                X_now = build_current_feature_row(
                    X_full=X_full,
                    y_full=y_full,
                    eval_date=eval_ms,
                    horizon=horizon,
                    cfg=sample_cfg,
                    feature_fn=feature_fn,
                )

                X_now = _sanitize_current_row(
                    X_now=X_now,
                    reference_columns=feature_columns,
                )

                if X_now is None:
                    add_failed("current feature row unavailable")
                    continue

                try:
                    X_fit, y_fit, X_val, y_val = _validation_split(
                        X_train=X_train,
                        y_train=y_train,
                        validation_tail_rows=model_cfg.validation_tail_rows,
                        min_train_rows=min_train_rows,
                    )

                    initial_params = None
                    key = (predictor, model_cfg.weight_scheme)
                    if model_cfg.warm_start:
                        initial_params = warm_params.get(key)

                    validation_rmse = float("nan")

                    if not X_val.empty and len(X_fit) >= min_train_rows:
                        val_fit = fit_univariate_midas_with_diagnostics(
                            X_train=X_fit,
                            y_train=y_fit,
                            predictor=predictor,
                            n_monthly_lags=model_cfg.n_monthly_lags,
                            n_y_lags=model_cfg.n_y_lags,
                            weight_scheme=model_cfg.weight_scheme,
                            max_iter=model_cfg.max_iter,
                            tol=model_cfg.tol,
                            n_starts=model_cfg.n_starts,
                            parameter_bound=model_cfg.parameter_bound,
                            fallback_weight_scheme=model_cfg.fallback_weight_scheme,
                            ridge_alpha=model_cfg.ridge_alpha,
                            initial_params=initial_params,
                        )
                        validation_rmse = _compute_validation_rmse(
                            val_fit.model,
                            X_val=X_val,
                            y_val=y_val,
                        )

                    fit_result = fit_univariate_midas_with_diagnostics(
                        X_train=X_train,
                        y_train=y_train,
                        predictor=predictor,
                        n_monthly_lags=model_cfg.n_monthly_lags,
                        n_y_lags=model_cfg.n_y_lags,
                        weight_scheme=model_cfg.weight_scheme,
                        max_iter=model_cfg.max_iter,
                        tol=model_cfg.tol,
                        n_starts=model_cfg.n_starts,
                        parameter_bound=model_cfg.parameter_bound,
                        fallback_weight_scheme=model_cfg.fallback_weight_scheme,
                        ridge_alpha=model_cfg.ridge_alpha,
                        initial_params=initial_params,
                    )

                    if model_cfg.warm_start and fit_result.model.weight_params:
                        warm_params[key] = dict(fit_result.model.weight_params)

                    pred_i = predict_univariate_midas(fit_result.model, X_now.iloc[0])

                    diag = asdict(fit_result.diagnostics)
                    diag_row = {
                        **base,
                        **diag,
                        "validation_rmse": validation_rmse,
                        "weight_params": _serialize_dict(diag.get("weight_params")),
                        "best_theta": _serialize_list(diag.get("best_theta")),
                        "all_start_objectives": _serialize_list(diag.get("all_start_objectives")),
                        "pred": float(pred_i) if np.isfinite(pred_i) else np.nan,
                        "used_in_combination": False,
                        "combination_weight": np.nan,
                        "selection_rank": np.nan,
                    }

                    diagnostics_rows.append(diag_row)
                    current_diag_indices.append(len(diagnostics_rows) - 1)

                    fc_row = {
                        **base,
                        "pred": float(pred_i) if np.isfinite(pred_i) else np.nan,
                        "actual": actual,
                        "abs_err": abs(float(pred_i) - actual) if np.isfinite(pred_i) and np.isfinite(actual) else np.nan,
                        "in_sample_rmse": float(fit_result.diagnostics.in_sample_rmse),
                        "validation_rmse": validation_rmse,
                        "used_in_combination": False,
                        "combination_weight": np.nan,
                        "selection_rank": np.nan,
                        "status_message": fit_result.diagnostics.status_message,
                    }

                    predictor_forecast_rows.append(fc_row)
                    current_forecast_indices.append(len(predictor_forecast_rows) - 1)

                    for i, w in enumerate(fit_result.model.lag_weights, start=1):
                        lag_weight_rows.append(
                            {
                                **base,
                                "weight_scheme": fit_result.model.weight_scheme,
                                "lag_index": i,
                                "weight": float(w),
                            }
                        )

                    if np.isfinite(pred_i):
                        forecast_records.append(
                            {
                                "predictor": predictor,
                                "pred": float(pred_i),
                                "in_sample_rmse": float(fit_result.diagnostics.in_sample_rmse),
                                "validation_rmse": validation_rmse,
                                "objective_value": float(fit_result.diagnostics.objective_value),
                            }
                        )

                except Exception as exc:
                    add_failed(f"fit/predict failed: {exc}")
                    continue

            pred, selected_records = combine_midas_forecasts(
                forecast_records,
                method=model_cfg.combination,
                trimmed_alpha=model_cfg.trimmed_alpha,
                top_k=model_cfg.top_k,
            )

            selected_by_predictor = {
                r["predictor"]: r
                for r in selected_records
            }

            for idx in current_diag_indices:
                predictor = diagnostics_rows[idx]["predictor"]
                selected = selected_by_predictor.get(predictor)
                if selected is not None:
                    diagnostics_rows[idx]["used_in_combination"] = True
                    diagnostics_rows[idx]["combination_weight"] = selected.get("combination_weight", np.nan)
                    diagnostics_rows[idx]["selection_rank"] = selected.get("selection_rank", np.nan)

            for idx in current_forecast_indices:
                predictor = predictor_forecast_rows[idx]["predictor"]
                selected = selected_by_predictor.get(predictor)
                if selected is not None:
                    predictor_forecast_rows[idx]["used_in_combination"] = True
                    predictor_forecast_rows[idx]["combination_weight"] = selected.get("combination_weight", np.nan)
                    predictor_forecast_rows[idx]["selection_rank"] = selected.get("selection_rank", np.nan)

            if pred_to_raw_fn is not None and np.isfinite(pred):
                pred_raw = float(pred_to_raw_fn(pred, target_date))
            else:
                pred_raw = pred

            if y_raw_full is not None:
                actual_raw = _get_actual_value(y_raw_full, target_date)
            else:
                actual_raw = actual

            rows.append(
                {
                    "eval_date": eval_ms,
                    "target_date": target_date,
                    "horizon": horizon,
                    "moq": moq,
                    "weight_scheme": model_cfg.weight_scheme,
                    "combination": model_cfg.combination,
                    "moq_specific": model_cfg.moq_specific,
                    "n_candidate_models": len(model_cfg.predictors),
                    "n_models_attempted": len(model_cfg.predictors),
                    "n_models_used": len(selected_records),
                    "pred": pred,
                    "actual": actual,
                    "pred_raw": pred_raw,
                    "actual_raw": actual_raw,
                }
            )

    pred_df = pd.DataFrame(rows).sort_values(["eval_date", "horizon"]).reset_index(drop=True)

    score_df = pred_df.copy()
    use_raw = pred_to_raw_fn is not None and y_raw_full is not None
    if use_raw:
        score_df["pred"] = score_df["pred_raw"]
        score_df["actual"] = score_df["actual_raw"]

    scores = compute_basic_metrics(score_df)
    scores["weight_scheme"] = model_cfg.weight_scheme
    scores["combination"] = model_cfg.combination
    scores["moq_specific"] = model_cfg.moq_specific
    scores["validation_tail_rows"] = model_cfg.validation_tail_rows

    diagnostics_df = pd.DataFrame(diagnostics_rows)
    lag_weights_df = pd.DataFrame(lag_weight_rows)
    predictor_forecasts_df = pd.DataFrame(predictor_forecast_rows)

    if return_details:
        return pred_df, scores, diagnostics_df, lag_weights_df, predictor_forecasts_df

    return pred_df, scores