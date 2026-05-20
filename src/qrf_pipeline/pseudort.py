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

    qs = sorted(q_preds)
    values = [q_preds[q] for q in qs]
    return float(np.median(values))


def _pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, q: float) -> float:
    err = y_true - y_pred
    loss = np.maximum(q * err, (q - 1.0) * err)
    return float(np.nanmean(loss))


def _compute_quantile_metrics(
    pred_df: pd.DataFrame,
    quantiles: tuple[float, ...],
) -> dict[str, Any]:
    if pred_df.empty:
        return {
            "n": 0,
            "pinball": {},
            "mean_pinball": float("nan"),
            "coverage_50": float("nan"),
            "avg_width_50": float("nan"),
            "coverage_80": float("nan"),
            "avg_width_80": float("nan"),
        }

    actual = pd.to_numeric(pred_df["actual"], errors="coerce").to_numpy(dtype=float)
    base_mask = np.isfinite(actual)

    pinball: dict[str, float] = {}

    for q in quantiles:
        col = _q_col_name(q)
        if col not in pred_df.columns:
            continue

        q_pred = pd.to_numeric(pred_df[col], errors="coerce").to_numpy(dtype=float)
        mask = base_mask & np.isfinite(q_pred)

        if mask.sum() == 0:
            pinball[str(q)] = float("nan")
        else:
            pinball[str(q)] = _pinball_loss(actual[mask], q_pred[mask], q)

    finite_pinball = [v for v in pinball.values() if np.isfinite(v)]
    mean_pinball = float(np.mean(finite_pinball)) if finite_pinball else float("nan")

    def _coverage_and_width(q_low: float, q_high: float) -> tuple[float, float]:
        low_col = _q_col_name(q_low)
        high_col = _q_col_name(q_high)

        if low_col not in pred_df.columns or high_col not in pred_df.columns:
            return float("nan"), float("nan")

        lo = pd.to_numeric(pred_df[low_col], errors="coerce").to_numpy(dtype=float)
        hi = pd.to_numeric(pred_df[high_col], errors="coerce").to_numpy(dtype=float)

        mask = base_mask & np.isfinite(lo) & np.isfinite(hi)
        if mask.sum() == 0:
            return float("nan"), float("nan")

        inside = (actual[mask] >= lo[mask]) & (actual[mask] <= hi[mask])
        width = hi[mask] - lo[mask]

        return float(np.mean(inside)), float(np.mean(width))

    coverage_50, avg_width_50 = _coverage_and_width(0.25, 0.75)
    coverage_80, avg_width_80 = _coverage_and_width(0.10, 0.90)

    return {
        "n": int(base_mask.sum()),
        "pinball": pinball,
        "mean_pinball": mean_pinball,
        "coverage_50": coverage_50,
        "avg_width_50": avg_width_50,
        "coverage_80": coverage_80,
        "avg_width_80": avg_width_80,
    }


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

    The progress bar is controlled by eval_cfg.show_progress.
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

            actual = _get_actual_value(y_full, target_date)

            if pred_to_raw_fn is not None and np.isfinite(pred):
                pred_raw = float(pred_to_raw_fn(pred, target_date))
            else:
                pred_raw = pred

            if y_raw_full is not None:
                actual_raw = _get_actual_value(y_raw_full, target_date)
            else:
                actual_raw = actual

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
                    row[_q_col_name(q)] = q_preds.get(q, float("nan"))

            rows.append(row)

    pred_df = (
        pd.DataFrame(rows)
        .sort_values(["eval_date", "horizon"])
        .reset_index(drop=True)
    )

    score_df = pred_df.copy()
    use_raw = pred_to_raw_fn is not None or y_raw_full is not None

    if use_raw:
        score_df["pred"] = score_df["pred_raw"]
        score_df["actual"] = score_df["actual_raw"]

    scores = compute_basic_metrics(score_df)
    scores["backend"] = backend

    if backend == "qrf":
        scores["quantile"] = _compute_quantile_metrics(pred_df, quantiles)

    return pred_df, scores