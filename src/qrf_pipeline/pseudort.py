from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

from qrf_pipeline.config import QRFConfig
from qrf_pipeline.feature_builder import extract_qrf_features
from qrf_pipeline.fit import fit_qrf_point
from qrf_pipeline.predict import predict_qrf_point
from rt_benchmarks.metrics import compute_basic_metrics
from rt_benchmarks.sample_builder import (
    SampleBuilderConfig,
    build_current_feature_row,
    build_direct_training_sample,
)
from rt_benchmarks.targeting import horizon_target_date, month_of_quarter


@dataclass(frozen=True)
class QRFPseudoRTConfig:
    eval_start: str
    eval_end: str
    horizons: tuple[str, ...] = ("now",)
    drop_invalid_rows: bool = True


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
    Run a pseudo-real-time direct-forecast QRF benchmark.

    Initial design:
    - intended for horizon="now" first
    - uses the shared sample builder to create monthly-vintage training rows
    - fits a point RandomForestRegressor using qrf_pipeline.fit.fit_qrf_point
    - returns a prediction DataFrame plus summary metrics

    Parameters
    ----------
    X_full : pd.DataFrame
        Full monthly predictor panel.
    y_full : pd.Series
        Target series used for model fitting. This may be standardized.
    eval_cfg : QRFPseudoRTConfig
        Evaluation-window configuration.
    sample_cfg : SampleBuilderConfig
        Shared sample-builder configuration.
    model_cfg : QRFConfig
        QRF/forest model configuration.
    y_raw_full : pd.Series | None
        Optional raw-scale target series. If provided, actual_raw will be taken
        from this series.
    pred_to_raw_fn : callable | None
        Optional function to convert a model-scale prediction to raw scale:
            pred_to_raw_fn(pred_value, target_date) -> raw_pred_value

        If omitted, pred_raw mirrors pred so the output schema remains usable.

    Returns
    -------
    pred_df : pd.DataFrame
        Row-level pseudo-RT predictions.
    scores : dict
        Summary metrics computed on pred/actual if no raw mapping is supplied,
        otherwise on pred_raw/actual_raw.
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

    rows: list[dict[str, Any]] = []

    for eval_date in eval_months:
        eval_ms = _to_month_start(eval_date)

        for horizon in eval_cfg.horizons:
            if horizon != "now":
                raise NotImplementedError(
                    "Initial QRF pseudo-RT implementation supports horizon='now' only."
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
                    model = fit_qrf_point(
                        X_train=X_train,
                        y_train=y_train,
                        cfg=model_cfg,
                    )
                    pred = predict_qrf_point(model, X_now)

            actual = _get_actual_value(y_full, target_date)

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
                    "n_train_rows": n_train_rows,
                    "n_features": n_features,
                    "pred": pred,
                    "actual": actual,
                    "pred_raw": pred_raw,
                    "actual_raw": actual_raw,
                }
            )

    pred_df = pd.DataFrame(rows).sort_values(["eval_date", "horizon"]).reset_index(drop=True)

    # If a raw mapping was supplied, score on raw scale; otherwise score on model scale.
    score_df = pred_df.copy()
    use_raw = pred_to_raw_fn is not None or y_raw_full is not None
    if use_raw:
        score_df["pred"] = score_df["pred_raw"]
        score_df["actual"] = score_df["actual_raw"]

    scores = compute_basic_metrics(score_df)
    return pred_df, scores