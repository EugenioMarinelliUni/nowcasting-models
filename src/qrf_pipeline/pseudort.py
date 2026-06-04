from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from qrf_pipeline.config import QRFConfig
from qrf_pipeline.diagnostics import compute_qrf_density_diagnostics
from qrf_pipeline.feature_builder import extract_qrf_features
from qrf_pipeline.fit import fit_qrf_point
from qrf_pipeline.importance import extract_feature_importance
from qrf_pipeline.predict import predict_qrf_point
from rt_benchmarks.metrics import compute_basic_metrics
from rt_benchmarks.sample_builder import (
    SampleBuilderConfig,
    build_current_feature_row,
    build_direct_training_sample,
)
from rt_benchmarks.targeting import horizon_target_date, month_of_quarter

try:
    from qrf_pipeline.fit import fit_qrf_quantile
except ImportError:
    fit_qrf_quantile = None


@dataclass(frozen=True)
class QRFPseudoRTConfig:
    eval_start: str
    eval_end: str
    horizons: tuple[str, ...] = ("now",)
    drop_invalid_rows: bool = True
    show_progress: bool = False
    collect_feature_importance: bool = True


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
    X = X.sort_index()
    return X


def _normalize_monthly_series_index(y: pd.Series) -> pd.Series:
    y = y.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(y.index))
    if idx.hasnans:
        raise ValueError("y index contains NaT values")
    y.index = idx.to_period("M").to_timestamp(how="start")
    y = y.sort_index()
    return y


def _qrf_feature_fn_factory(model_cfg: QRFConfig) -> Callable:
    def _feature_fn(
        Xv: pd.DataFrame,
        yv: pd.Series,
        vintage_date: pd.Timestamp,
        horizon: str,
    ) -> dict[str, float] | None:
        return extract_qrf_features(
            Xv=Xv,
            yv=yv,
            eval_date=vintage_date,
            predictors=model_cfg.predictors,
            n_lags=model_cfg.n_lags,
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
    y_num = pd.to_numeric(y_train, errors="coerce")

    if not drop_invalid_rows:
        return X_num, y_num

    mask = np.isfinite(y_num.to_numpy(dtype=float))
    if X_num.shape[1] > 0:
        mask &= np.isfinite(X_num.to_numpy(dtype=float)).all(axis=1)

    X_num = X_num.loc[mask].reset_index(drop=True)
    y_num = y_num.loc[mask].reset_index(drop=True)

    return X_num, y_num


def _sanitize_current_row(X_now: pd.DataFrame | None) -> pd.DataFrame | None:
    if X_now is None:
        return None

    X_now = X_now.apply(pd.to_numeric, errors="coerce")
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
    if pd.isna(val):
        return float("nan")

    return float(val)


def _q_col_name(q: float) -> str:
    pct = int(round(q * 100))
    return f"pred_q{pct}"


def _get_backend(model_cfg: QRFConfig) -> str:
    return str(getattr(model_cfg, "backend", "rf_point"))


def _get_quantiles(model_cfg: QRFConfig) -> tuple[float, ...]:
    default = (0.10, 0.25, 0.50, 0.75, 0.90)
    quantiles = getattr(model_cfg, "quantiles", default)
    return tuple(float(q) for q in quantiles)


def _align_current_row_to_model(X_now: pd.DataFrame, model: Any) -> pd.DataFrame:
    feature_columns = getattr(model, "feature_columns", None)

    if feature_columns is None and hasattr(model, "estimator"):
        feature_columns = getattr(model.estimator, "feature_names_in_", None)

    if feature_columns is None:
        return X_now

    feature_columns = list(feature_columns)
    return X_now.reindex(columns=feature_columns)


def _predict_qrf_quantiles(
    model: Any,
    X_now: pd.DataFrame,
    quantiles: tuple[float, ...],
) -> dict[float, float]:
    X_now = _align_current_row_to_model(X_now, model)

    estimator = getattr(model, "estimator", model)

    if hasattr(estimator, "predict"):
        try:
            pred = estimator.predict(X_now, quantiles=list(quantiles))
        except TypeError:
            pred = estimator.predict(X_now, quantiles=np.asarray(quantiles))
    else:
        raise TypeError("QRF model does not expose a predict method")

    arr = np.asarray(pred, dtype=float)

    if arr.ndim == 0:
        arr = arr.reshape(1, 1)
    elif arr.ndim == 1:
        arr = arr.reshape(1, -1)

    if arr.shape[0] != 1:
        arr = arr[:1, :]

    if arr.shape[1] != len(quantiles):
        raise ValueError(
            f"Expected {len(quantiles)} quantile predictions, got shape={arr.shape}"
        )

    return {q: float(arr[0, i]) for i, q in enumerate(quantiles)}


def _median_from_quantiles(q_preds: dict[float, float]) -> float:
    for q, val in q_preds.items():
        if abs(q - 0.5) < 1e-12:
            return float(val)

    values = [q_preds[q] for q in sorted(q_preds)]
    return float(np.median(values))


def _make_eval_iterator(eval_months: pd.DatetimeIndex, show_progress: bool):
    if not show_progress:
        return eval_months

    if tqdm is None:
        print("Warning: tqdm is not installed; progress bar disabled.")
        return eval_months

    return tqdm(
        eval_months,
        desc="QRF pseudo-RT",
        unit="vintage",
        ascii=True,
    )


def _apply_raw_transform(
    value: float,
    target_date: pd.Timestamp,
    pred_to_raw_fn: Callable[[float, pd.Timestamp], float] | None,
) -> float:
    if pred_to_raw_fn is None or not np.isfinite(value):
        return value
    return float(pred_to_raw_fn(float(value), target_date))


def _append_feature_importance(
    rows: list[dict[str, Any]],
    *,
    model: Any,
    eval_date: pd.Timestamp,
    target_date: pd.Timestamp,
    horizon: str,
    backend: str,
) -> None:
    feature_columns = list(getattr(model, "feature_columns", []))
    imp = extract_feature_importance(model, feature_columns)
    if imp.empty:
        return

    for _, r in imp.iterrows():
        rows.append(
            {
                "eval_date": eval_date,
                "target_date": target_date,
                "horizon": horizon,
                "backend": backend,
                "feature": str(r["feature"]),
                "importance": float(r["importance"]),
            }
        )


def _point_scores_for_columns(
    pred_df: pd.DataFrame,
    *,
    pred_col: str,
    actual_col: str,
) -> dict:
    score_df = pred_df[[pred_col, actual_col]].copy()
    score_df = score_df.rename(columns={pred_col: "pred", actual_col: "actual"})
    return compute_basic_metrics(score_df)


def _quantile_diag_for_columns(
    pred_df: pd.DataFrame,
    *,
    quantiles: tuple[float, ...],
    actual_col: str,
    raw: bool = False,
) -> dict:
    cols = {"actual": pd.to_numeric(pred_df[actual_col], errors="coerce")}

    for q in quantiles:
        base = _q_col_name(q)
        col = f"{base}_raw" if raw else base
        if col in pred_df.columns:
            cols[base] = pd.to_numeric(pred_df[col], errors="coerce")

    qdf = pd.DataFrame(cols, index=pred_df.index)
    out = compute_qrf_density_diagnostics(qdf, quantiles, actual_col="actual")
    out["score_scale"] = "raw" if raw else "standardized"
    return out


def run_qrf_pseudort(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_cfg: QRFPseudoRTConfig,
    sample_cfg: SampleBuilderConfig,
    model_cfg: QRFConfig,
    *,
    y_raw_full: pd.Series | None = None,
    pred_to_raw_fn: Callable[[float, pd.Timestamp], float] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Run a pseudo-real-time direct-forecast QRF / RF benchmark.

    Supported backends:
    - rf_point: point RandomForestRegressor
    - qrf: RandomForestQuantileRegressor / quantile forest backend

    Scores are always reported on the standardized scale at the top level.
    If a raw target or inverse-transform function is supplied, raw-scale point
    and density metrics are added under scores["raw"] and scores["quantile_raw"],
    with convenience flat keys such as rmse_raw and mae_raw.
    """
    X_full = _normalize_monthly_frame_index(X_full)
    y_full = _normalize_monthly_series_index(y_full)

    if y_raw_full is not None:
        y_raw_full = _normalize_monthly_series_index(y_raw_full)

    eval_start = _to_month_start(eval_cfg.eval_start)
    eval_end = _to_month_start(eval_cfg.eval_end)

    eval_months = X_full.index[(X_full.index >= eval_start) & (X_full.index <= eval_end)]
    if len(eval_months) == 0:
        raise ValueError("No evaluation months found in the requested eval window")

    feature_fn = _qrf_feature_fn_factory(model_cfg)
    min_train_rows = max(sample_cfg.min_train_rows, model_cfg.min_train_rows)

    backend = _get_backend(model_cfg)
    quantiles = _get_quantiles(model_cfg)

    rows: list[dict[str, Any]] = []
    importance_rows: list[dict[str, Any]] = []
    iterator = _make_eval_iterator(eval_months, eval_cfg.show_progress)

    for eval_date in iterator:
        eval_ms = _to_month_start(eval_date)

        for horizon in eval_cfg.horizons:
            if horizon != "now":
                raise NotImplementedError(
                    "QRF pseudo-RT implementation currently supports horizon='now' only."
                )

            target_date = _to_month_start(horizon_target_date(eval_ms, horizon))
            moq = month_of_quarter(eval_ms)

            X_train, y_train, _meta = build_direct_training_sample(
                X_full=X_full,
                y_full=y_full,
                eval_date=eval_ms,
                horizon=horizon,
                cfg=sample_cfg,
                feature_fn=feature_fn,
            )

            X_train, y_train = _sanitize_training_sample(
                X_train=X_train,
                y_train=y_train,
                drop_invalid_rows=eval_cfg.drop_invalid_rows,
            )

            n_train_rows = int(len(X_train))
            n_features = int(X_train.shape[1]) if not X_train.empty else 0

            pred = float("nan")
            q_preds: dict[float, float] = {}

            if n_train_rows >= min_train_rows:
                X_now = build_current_feature_row(
                    X_full=X_full,
                    y_full=y_full,
                    eval_date=eval_ms,
                    horizon=horizon,
                    cfg=sample_cfg,
                    feature_fn=feature_fn,
                )
                X_now = _sanitize_current_row(X_now)

                if X_now is not None:
                    model = None

                    if backend == "rf_point":
                        model = fit_qrf_point(
                            X_train=X_train,
                            y_train=y_train,
                            cfg=model_cfg,
                        )
                        pred = float(predict_qrf_point(model, X_now))

                    elif backend == "qrf":
                        if fit_qrf_quantile is None:
                            raise ImportError(
                                "backend='qrf' requires fit_qrf_quantile in qrf_pipeline.fit"
                            )

                        model = fit_qrf_quantile(
                            X_train=X_train,
                            y_train=y_train,
                            cfg=model_cfg,
                        )
                        q_preds = _predict_qrf_quantiles(
                            model=model,
                            X_now=X_now,
                            quantiles=quantiles,
                        )
                        pred = _median_from_quantiles(q_preds)

                    else:
                        raise ValueError(
                            f"Unsupported QRF backend: {backend}. "
                            "Expected 'rf_point' or 'qrf'."
                        )

                    if (
                        model is not None
                        and getattr(model_cfg, "save_feature_importance", True)
                        and eval_cfg.collect_feature_importance
                    ):
                        _append_feature_importance(
                            importance_rows,
                            model=model,
                            eval_date=eval_ms,
                            target_date=target_date,
                            horizon=horizon,
                            backend=backend,
                        )

            actual = _get_actual_value(y_full, target_date)

            pred_raw = _apply_raw_transform(pred, target_date, pred_to_raw_fn)
            if y_raw_full is not None:
                actual_raw = _get_actual_value(y_raw_full, target_date)
            else:
                actual_raw = _apply_raw_transform(actual, target_date, pred_to_raw_fn)

            row: dict[str, Any] = {
                "eval_date": eval_ms,
                "target_date": target_date,
                "horizon": horizon,
                "moq": moq,
                "n_train_rows": n_train_rows,
                "n_features": n_features,
                "pred": pred,
                "actual": actual,
                "pred_raw": pred_raw,
                "actual_raw": actual_raw,
            }

            if backend == "qrf":
                for q in quantiles:
                    col = _q_col_name(q)
                    q_val = q_preds.get(q, float("nan"))
                    row[col] = q_val
                    row[f"{col}_raw"] = _apply_raw_transform(q_val, target_date, pred_to_raw_fn)

            rows.append(row)

    pred_df = (
        pd.DataFrame(rows)
        .sort_values(["eval_date", "horizon"])
        .reset_index(drop=True)
    )

    if importance_rows:
        pred_df.attrs["feature_importance"] = pd.DataFrame(importance_rows)
    else:
        pred_df.attrs["feature_importance"] = pd.DataFrame(
            columns=["eval_date", "target_date", "horizon", "backend", "feature", "importance"]
        )

    standardized_scores = _point_scores_for_columns(
        pred_df,
        pred_col="pred",
        actual_col="actual",
    )
    standardized_scores["score_scale"] = "standardized"

    scores = dict(standardized_scores)
    scores["backend"] = backend
    scores["standardized"] = dict(standardized_scores)

    use_raw = pred_to_raw_fn is not None or y_raw_full is not None
    if use_raw:
        raw_scores = _point_scores_for_columns(
            pred_df,
            pred_col="pred_raw",
            actual_col="actual_raw",
        )
        raw_scores["score_scale"] = "raw"
        scores["raw"] = raw_scores
        scores["n_raw"] = raw_scores.get("n")
        scores["rmse_raw"] = raw_scores.get("rmse")
        scores["mae_raw"] = raw_scores.get("mae")

    if backend == "qrf":
        quantile_std = _quantile_diag_for_columns(
            pred_df,
            quantiles=quantiles,
            actual_col="actual",
            raw=False,
        )
        scores["quantile"] = quantile_std
        scores["quantile_standardized"] = quantile_std

        if use_raw and all(f"{_q_col_name(q)}_raw" in pred_df.columns for q in quantiles):
            scores["quantile_raw"] = _quantile_diag_for_columns(
                pred_df,
                quantiles=quantiles,
                actual_col="actual_raw",
                raw=True,
            )

    return pred_df, scores
