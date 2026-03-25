from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

from rt_benchmarks.targeting import (
    horizon_target_date,
    month_of_quarter,
    target_release_date,
)
from rt_benchmarks.vintage import (
    VintageConfig,
    build_vintage_view,
)


FeatureFn = Callable[
    [pd.DataFrame, pd.Series, pd.Timestamp, str],
    dict[str, Any] | None,
]


@dataclass(frozen=True)
class SampleBuilderConfig:
    vintage_cfg: VintageConfig
    min_train_rows: int = 24
    include_row_metadata: bool = True


def _to_month_start(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if pd.isna(out):
        raise ValueError("timestamp cannot be NaT")
    return out.to_period("M").to_timestamp(how="start")


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


def _is_target_observable_by_eval_date(
    eval_date: pd.Timestamp,
    target_date: pd.Timestamp,
    gdp_rel: int,
) -> bool:
    """
    A row can be used for training only if its target would already be
    observable by the CURRENT evaluation month.
    """
    eval_ms = _to_month_start(eval_date)
    target_ms = _to_month_start(target_date)
    rel_date = _to_month_start(target_release_date(target_ms, gdp_rel))
    return eval_ms >= rel_date


def build_direct_training_sample(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_date: pd.Timestamp,
    horizon: str,
    cfg: SampleBuilderConfig,
    feature_fn: FeatureFn,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame | None]:
    """
    Build a supervised training sample for a direct-forecast model.

    Each row corresponds to one historical monthly vintage s <= eval_date.
    Features are extracted from the information set available at vintage s.
    The target is the realized GDP value associated with the requested horizon
    relative to s.

    Parameters
    ----------
    X_full : pd.DataFrame
        Full monthly predictor panel.
    y_full : pd.Series
        Full quarterly target series, indexed on monthly timestamps
        (typically quarter-end months only).
    eval_date : pd.Timestamp
        Current pseudo-real-time evaluation month.
    horizon : str
        Forecast horizon. For the initial implementation, use "now".
    cfg : SampleBuilderConfig
        Shared sample-building configuration.
    feature_fn : callable
        Signature:
            feature_fn(Xv, yv, vintage_date, horizon) -> dict | None

        Must return a flat feature dictionary or None if the row cannot
        be built for that historical vintage.

    Returns
    -------
    X_train : pd.DataFrame
        Training feature matrix.
    y_train : pd.Series
        Training target vector.
    row_meta : pd.DataFrame | None
        Optional metadata per row for debugging/alignment checks.
    """
    eval_ms = _to_month_start(eval_date)

    X_full = _normalize_monthly_frame_index(X_full)
    y_full = _normalize_monthly_series_index(y_full)

    rows: list[dict[str, Any]] = []
    targets: list[float] = []
    meta_rows: list[dict[str, Any]] = []

    candidate_vintages = X_full.index[X_full.index <= eval_ms]

    for vintage_date in candidate_vintages:
        vintage_ms = _to_month_start(vintage_date)

        Xv, yv = build_vintage_view(
            X_full=X_full,
            y_full=y_full,
            eval_date=vintage_ms,
            cfg=cfg.vintage_cfg,
        )

        target_date = _to_month_start(horizon_target_date(vintage_ms, horizon))

        if target_date not in y_full.index:
            continue

        y_target = y_full.loc[target_date]
        if not np.isfinite(y_target):
            continue

        if not _is_target_observable_by_eval_date(
            eval_date=eval_ms,
            target_date=target_date,
            gdp_rel=cfg.vintage_cfg.gdp_rel,
        ):
            continue

        feat = feature_fn(Xv, yv, vintage_ms, horizon)
        if feat is None:
            continue

        if not isinstance(feat, dict):
            raise TypeError("feature_fn must return dict | None")

        rows.append(feat)
        targets.append(float(y_target))

        if cfg.include_row_metadata:
            meta_rows.append(
                {
                    "vintage_date": vintage_ms,
                    "target_date": target_date,
                    "horizon": horizon,
                    "moq": month_of_quarter(vintage_ms),
                }
            )

    if not rows:
        empty_X = pd.DataFrame()
        empty_y = pd.Series(dtype=float, name="target")
        empty_meta = pd.DataFrame() if cfg.include_row_metadata else None
        return empty_X, empty_y, empty_meta

    X_train = pd.DataFrame(rows)
    y_train = pd.Series(targets, dtype=float, name="target")

    if len(X_train) != len(y_train):
        raise RuntimeError("X_train and y_train length mismatch")

    if cfg.include_row_metadata:
        row_meta = pd.DataFrame(meta_rows)
        if len(row_meta) != len(X_train):
            raise RuntimeError("row_meta and X_train length mismatch")
        return X_train, y_train, row_meta

    return X_train, y_train, None


def build_current_feature_row(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_date: pd.Timestamp,
    horizon: str,
    cfg: SampleBuilderConfig,
    feature_fn: FeatureFn,
) -> pd.DataFrame | None:
    """
    Build the single feature row corresponding to the CURRENT evaluation vintage.
    This is the row used for prediction after model fitting.
    """
    eval_ms = _to_month_start(eval_date)

    X_full = _normalize_monthly_frame_index(X_full)
    y_full = _normalize_monthly_series_index(y_full)

    Xv, yv = build_vintage_view(
        X_full=X_full,
        y_full=y_full,
        eval_date=eval_ms,
        cfg=cfg.vintage_cfg,
    )

    feat = feature_fn(Xv, yv, eval_ms, horizon)
    if feat is None:
        return None

    if not isinstance(feat, dict):
        raise TypeError("feature_fn must return dict | None")

    return pd.DataFrame([feat])


def validate_training_sample(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    cfg: SampleBuilderConfig,
) -> None:
    """
    Lightweight validation helper for runners.
    Raises ValueError if the training sample is unusable.
    """
    if len(X_train) != len(y_train):
        raise ValueError("X_train and y_train length mismatch")

    if len(X_train) < cfg.min_train_rows:
        raise ValueError(
            f"Insufficient training rows: got {len(X_train)}, "
            f"need at least {cfg.min_train_rows}"
        )

    if X_train.empty:
        raise ValueError("X_train is empty")

    if y_train.empty:
        raise ValueError("y_train is empty")