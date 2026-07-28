# src/dfm_pipeline/eval_pseudort/fast/bm_pseudort_fast.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from types import SimpleNamespace
from typing import Any, Callable, Optional, Tuple

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_bm_ml.forecasting import (
    observation_predictive_moments,
    propagate_state_moments,
)
from dfm_pipeline.dfm_bm_ml.scaling import PanelScaler, TargetOutputScaler
from dfm_pipeline.eval_pseudort.vintages import VintageProvider, VintageSnapshot
from dfm_pipeline.preprocessing.bm_inputs import validate_quarter_end_target_alignment
from dfm_pipeline.utils.threadpool import limit_blas_threads


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------

def _normalize_month_start(idx: Any) -> pd.DatetimeIndex:
    dt = pd.to_datetime(idx)
    return pd.DatetimeIndex(dt).to_period("M").to_timestamp(how="start")


def _month_in_quarter(dt: pd.Timestamp) -> int:
    m = int(pd.Timestamp(dt).month)
    return ((m - 1) % 3) + 1


def _quarter_end_month(dt: pd.Timestamp) -> pd.Timestamp:
    q = pd.Timestamp(dt).to_period("Q")
    return q.end_time.to_period("M").to_timestamp(how="start")


def _horizon_target_date(dt: pd.Timestamp, horizon: str) -> pd.Timestamp:
    q = pd.Timestamp(dt).to_period("Q")
    if horizon == "bac":
        q = q - 1
    elif horizon == "now":
        pass
    elif horizon == "for":
        q = q + 1
    else:
        raise ValueError(f"Unsupported horizon: {horizon!r}")
    return q.end_time.to_period("M").to_timestamp(how="start")


def _months_diff(a: pd.Timestamp, b: pd.Timestamp) -> int:
    a = pd.Timestamp(a).to_period("M").to_timestamp(how="start")
    b = pd.Timestamp(b).to_period("M").to_timestamp(how="start")
    return (a.year - b.year) * 12 + (a.month - b.month)


def _squeeze_last(M: np.ndarray) -> np.ndarray:
    M = np.asarray(M, dtype=float)
    if M.ndim == 3:
        return M[-1]
    return M


def _is_square(M: np.ndarray) -> bool:
    return M.ndim == 2 and M.shape[0] == M.shape[1]


def _to_attr(obj: Any, name: str) -> Any:
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict) and name in obj:
        return obj[name]
    raise AttributeError(f"Object has no attribute/key '{name}'. type={type(obj)}")


def _call_supported(fn: Callable[..., Any], /, **kwargs: Any) -> Any:
    import inspect

    sig = inspect.signature(fn)
    supported = set(sig.parameters.keys())
    filt = {k: v for k, v in kwargs.items() if k in supported}
    return fn(**filt)


# -----------------------------------------------------------------------------
# Masks for pseudo-real-time vintages
# -----------------------------------------------------------------------------

def _validate_delay_map(
    delay_map: object,
    predictor_columns: Any,
) -> dict[str, int]:
    """Validate a complete predictor-specific monthly release-lag map.

    Every predictor must appear exactly once. Lags are non-negative integers in
    months. Full coverage prevents omitted predictors from silently receiving an
    unsafe contemporaneous-release assumption.
    """
    if not isinstance(delay_map, dict):
        raise TypeError("delay_map must be a JSON object mapping predictor names to lags.")

    expected = [str(c) for c in predictor_columns]
    expected_set = set(expected)
    supplied_set = {str(k) for k in delay_map}
    missing = sorted(expected_set - supplied_set)
    unknown = sorted(supplied_set - expected_set)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing predictors={missing}")
        if unknown:
            details.append(f"unknown predictors={unknown}")
        raise ValueError(
            "delay_map must cover the predictor panel exactly; " + "; ".join(details)
        )

    validated: dict[str, int] = {}
    for col in expected:
        lag = delay_map[col]
        if isinstance(lag, bool) or not isinstance(lag, int):
            raise TypeError(
                f"Release lag for {col!r} must be a non-negative integer, got {lag!r}."
            )
        if lag < 0:
            raise ValueError(
                f"Release lag for {col!r} must be non-negative, got {lag}."
            )
        validated[col] = int(lag)
    return validated


def _apply_delay_mask(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    delay_style: str,
    delay_map: Optional[dict],
) -> pd.DataFrame:
    if delay_style == "none":
        return X.copy()
    if delay_style == "trailing_nan":
        raise ValueError(
            "delay_style='trailing_nan' has been removed because a common trailing "
            "lag is not a defensible substitute for predictor-specific publication lags. "
            "Use delay_style='json_map'."
        )
    if delay_style != "json_map":
        raise ValueError(f"Unknown delay_style: {delay_style!r}")
    if delay_map is None:
        raise ValueError("delay_style=json_map requires delay_map")

    validated = _validate_delay_map(delay_map, X.columns)
    out = X.copy()
    eval_ms = pd.Timestamp(eval_date).to_period("M").to_timestamp(how="start")
    for col in out.columns:
        cutoff = eval_ms - pd.offsets.MonthBegin(validated[str(col)])
        # This operation only adds NaNs; naturally missing observations remain NaN.
        out.loc[out.index > cutoff, col] = np.nan
    return out


def _mask_quarterly_target_release(
    y: pd.Series,
    eval_date: pd.Timestamp,
    gdp_rel: int,
    *,
    as_of_rule: str = "month_start",
) -> pd.Series:
    """Mask GDP values that were not released by the vintage cutoff.

    With a month-start cutoff, a release occurring during the evaluation month is
    still unavailable. With a month-end cutoff, it is available during that month.
    Actual historical-vintage providers normally bypass this approximation because
    their target snapshot already records release availability.
    """
    y = y.copy()
    if gdp_rel <= 0:
        return y
    if as_of_rule not in {"month_start", "month_end"}:
        raise ValueError("as_of_rule must be month_start or month_end.")

    y_idx = pd.DatetimeIndex(y.index)
    q_end = y_idx.to_period("Q").end_time.to_period("M").to_timestamp(how="start")
    release_month = q_end + pd.offsets.MonthBegin(int(gdp_rel))
    eval_ms = pd.Timestamp(eval_date).to_period("M").to_timestamp(how="start")
    mask = release_month >= eval_ms if as_of_rule == "month_start" else release_month > eval_ms
    y.loc[mask] = np.nan
    return y


def _apply_quarter_end_leakage_guard(
    y: pd.Series,
    eval_date: pd.Timestamp,
    *,
    no_qe_leak: bool,
) -> pd.Series:
    if not no_qe_leak:
        return y
    t = pd.Timestamp(eval_date).to_period("M").to_timestamp(how="start")
    if _month_in_quarter(t) == 3:
        y = y.copy()
        if t in y.index:
            y.loc[t] = np.nan
    return y


# -----------------------------------------------------------------------------
# Extract transition/measurement matrices from fit results
# -----------------------------------------------------------------------------

def _extract_transition_and_measurement(res: Any, *, n_obs_expected: int) -> tuple[np.ndarray, np.ndarray]:
    """Return (transition, measurement) from a BM result.

    Canonical BM API after the matrix fix:
      - res.A is the transition matrix, shape (n_state, n_state)
      - res.C is the measurement matrix, shape (n_obs, n_state)

    The shape fallback keeps older saved/intermediate objects readable.
    """
    A = _squeeze_last(np.asarray(_to_attr(res, "A"), dtype=float))
    C = _squeeze_last(np.asarray(_to_attr(res, "C"), dtype=float))

    # Preferred canonical path.
    if _is_square(A) and C.ndim == 2 and C.shape[0] == n_obs_expected and C.shape[1] == A.shape[0]:
        return A, C

    # Backward-compatible path for old objects that accidentally stored C=transition, A=measurement.
    if _is_square(C) and A.ndim == 2 and A.shape[0] == n_obs_expected and A.shape[1] == C.shape[0]:
        return C, A

    raise ValueError(
        "Cannot identify transition/measurement matrices from result.\n"
        f"Shapes: A={A.shape}, C={C.shape}, n_obs_expected={n_obs_expected}.\n"
        "Expected one square (n_state,n_state) and one (n_obs_expected,n_state)."
    )


def _forecast_state(Tmat: np.ndarray, a_last: np.ndarray, steps: int) -> np.ndarray:
    a = np.asarray(a_last, float).copy()
    for _ in range(int(steps)):
        a = Tmat @ a
    return a


def _target_location_scale(
    res: Any,
    *,
    n_obs_expected: int,
    scaling_mode: str,
) -> tuple[float, float]:
    """Return the target scaler actually used by the fitted vintage.

    Parameters are estimated in the transformed observation space, so forecasts
    must be inverted with the scaler attached to that same fit. Falling back to
    full-sample moments would both use the wrong normalization and leak future
    target information into early pseudo-real-time vintages.
    """
    scaler = getattr(res, "scaler", None)
    if scaler is None:
        if scaling_mode == "external_frozen":
            return 0.0, 1.0
        raise AttributeError(
            "A fitted vintage using internal scaling must expose its PanelScaler; "
            "cannot invert forecasts safely without it."
        )
    if not isinstance(scaler, PanelScaler):
        raise TypeError(f"Expected PanelScaler on fit result, got {type(scaler)!r}.")
    if scaler.mu.shape[0] != int(n_obs_expected):
        raise ValueError(
            f"Fit scaler has {scaler.mu.shape[0]} variables; expected {n_obs_expected}."
        )
    return scaler.column_location_scale(-1)


# -----------------------------------------------------------------------------
# Warm-start run state (refit mode)
# -----------------------------------------------------------------------------

@dataclass
class PRTRunState:
    params_init: Optional[object] = None
    em_cache: Optional[object] = None


# -----------------------------------------------------------------------------
# Fixed-params smoothing (NO EM) for each vintage
# -----------------------------------------------------------------------------

def _smooth_fixed_params(
    *,
    X_v: pd.DataFrame,
    y_v: pd.Series,
    params_fixed: Any,
    scaler_fixed: PanelScaler,
    model_config: Any,
):
    from dfm_pipeline.dfm_bm_ml.state_builder import build_state_space
    from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

    Y_raw = np.concatenate(
        [X_v.to_numpy(dtype=float), y_v.to_numpy(dtype=float)[:, None]],
        axis=1,
    )
    Y_scaled = scaler_fixed.transform(Y_raw)

    Tm, Qm, C_meas, R_meas, a0, P0, _idx = _call_supported(
        build_state_space,
        params=params_fixed,
        nM=int(X_v.shape[1]),
        nQ=1,
        r_by_block=tuple(int(x) for x in getattr(model_config, "r_by_block")),
        p=int(getattr(model_config, "p")),
        ppC=int(getattr(model_config, "ppC", 5)),
        mm_style=str(getattr(model_config, "mm_weight_style", "toolbox")),
        quarterly_meas_var_floor=float(getattr(model_config, "quarterly_meas_var_floor", 1e-6)),
        idio_ar1=bool(getattr(model_config, "idio_ar1", True)),
        jitter=float(getattr(model_config, "jitter", 1e-8)),
        P0_mode=str(getattr(model_config, "P0_mode", "steady_state")),
        a0_override=None,
        P0_override=None,
    )
    R_used = np.diag(R_meas) if np.asarray(R_meas).ndim == 2 else np.asarray(R_meas)
    smooth = kalman_filter_smoother(
        Y_scaled,
        StateSpaceParams(T=Tm, Q=Qm, C=C_meas, R=R_used, a0=a0, P0=P0),
    )
    return SimpleNamespace(
        A=Tm, Q=Qm, C=C_meas, R=R_meas,
        a_smooth=np.asarray(smooth.a_smooth, dtype=float),
        P_smooth=np.asarray(smooth.P_smooth, dtype=float),
        scaler=scaler_fixed, converged=True,
        diagnostics={"fixed_parameters": True, "final_loglik": float(smooth.loglik)},
    )


# -----------------------------------------------------------------------------
# One-vintage EM fit (refit mode)
# -----------------------------------------------------------------------------

def _fit_one_vintage_em(
    *,
    X_v: pd.DataFrame,
    y_v: pd.Series,
    fit_fn: Callable,
    model_config: Any,
    run_state: Optional[PRTRunState],
    warm_start: bool,
    verbose_em: bool = False,
):
    Y = X_v.to_numpy(dtype=float)
    y_arr = y_v.to_numpy(dtype=float)

    kwargs: dict[str, Any] = {}
    if warm_start and run_state is not None:
        if run_state.params_init is not None:
            kwargs["init_params"] = run_state.params_init
        if run_state.em_cache is not None:
            kwargs["em_cache"] = run_state.em_cache

    res = fit_fn(Y_monthly=Y, y_quarterly=y_arr, config=model_config, verbose=bool(verbose_em), **kwargs)

    if warm_start and run_state is not None:
        if hasattr(res, "params"):
            run_state.params_init = res.params
        if hasattr(res, "em_cache"):
            run_state.em_cache = res.em_cache

    return res


# -----------------------------------------------------------------------------
# Evaluation config
# -----------------------------------------------------------------------------

@dataclass
class EvalConfig:
    eval_start: str
    eval_end: str
    delay_style: str = "none"
    delay_json: Optional[str] = None
    gdp_rel: int = 1
    horizons: Tuple[str, ...] = ("now",)
    no_qe_leak: bool = True
    prediction_interval_levels: Tuple[float, ...] = (0.68, 0.90, 0.95)
    require_convergence: bool = False
    on_nonconvergence: str = "raise"  # raise|skip|keep
    apply_masks_to_vintage_provider: bool = False
    vintage_as_of_rule: str = "month_start"  # month_start|month_end
    parameter_mode: str = "recursive"  # recursive|fixed

    def validate(self) -> None:
        if int(self.gdp_rel) < 1:
            raise ValueError(
                "gdp_rel must be at least 1 in revised-panel pseudo-real-time mode; "
                "same-month quarterly GDP availability is leakage-prone."
            )
        if not bool(self.no_qe_leak):
            raise ValueError("Quarter-end target leakage protection cannot be disabled.")
        if self.delay_style not in {"none", "json_map"}:
            raise ValueError("delay_style must be 'none' or 'json_map'; trailing_nan was removed.")
        if self.delay_style == "json_map" and not self.delay_json:
            raise ValueError("delay_style=json_map requires delay_json.")
        if self.parameter_mode not in {"recursive", "fixed"}:
            raise ValueError("parameter_mode must be recursive or fixed.")
        if self.on_nonconvergence not in {"raise", "skip", "keep"}:
            raise ValueError("on_nonconvergence must be raise, skip, or keep.")
        if self.vintage_as_of_rule not in {"month_start", "month_end"}:
            raise ValueError("vintage_as_of_rule must be month_start or month_end.")
        for h in self.horizons:
            if h not in {"bac", "now", "for"}:
                raise ValueError(f"Unsupported horizon: {h!r}")
        for level in self.prediction_interval_levels:
            if not (0.0 < float(level) < 1.0):
                raise ValueError("Prediction interval levels must lie in (0,1).")


# -----------------------------------------------------------------------------
# Core pseudo-real-time loop
# -----------------------------------------------------------------------------

def run_pseudo_rt_eval_fast(
    *,
    X_full: pd.DataFrame,
    y_full: pd.Series,
    fit_fn: Callable,
    model_config: Any,
    eval_cfg: EvalConfig,
    warm_start: bool = True,
    fixed_params: Optional[bool] = None,
    train_end: Optional[str] = None,
    train_max_iter: Optional[int] = None,
    blas_threads: int | None = None,
    vintage_provider: Optional[VintageProvider] = None,
    target_output_scaler: Optional[TargetOutputScaler] = None,
) -> Tuple[pd.DataFrame, dict]:
    """Run a leakage-safe pseudo-real-time or true-vintage DFM evaluation.

    ``X_full`` and ``y_full`` define the common calendar, predictor order and the
    evaluation target.  Supplying ``vintage_provider`` replaces the masked
    revised-data information set with an actual historical snapshot at every
    evaluation date.  The provider is optional and does not alter the default
    revised-panel pseudo-real-time workflow.
    """
    from tqdm.auto import tqdm

    eval_cfg.validate()
    parameter_mode = str(eval_cfg.parameter_mode)
    if fixed_params is not None:
        # Backward-compatible Python API: the legacy boolean overrides the default
        # EvalConfig mode. New callers should set EvalConfig.parameter_mode.
        parameter_mode = "fixed" if bool(fixed_params) else "recursive"
    is_fixed = parameter_mode == "fixed"
    if is_fixed and str(getattr(model_config, "P0_mode", "steady_state")) == "estimated":
        raise ValueError(
            "DFM-fixed does not support P0_mode='estimated' because fixed evaluation "
            "must reuse a well-defined initial-state policy; use P0_mode='steady_state'."
        )
    if blas_threads is not None and int(blas_threads) > 0:
        limit_blas_threads(int(blas_threads))

    X_full = X_full.copy()
    X_full.index = _normalize_month_start(X_full.index)
    y_full = y_full.copy()
    y_full.index = _normalize_month_start(y_full.index)
    validate_quarter_end_target_alignment(y_full, name="y_full")

    idx = X_full.index.intersection(y_full.index).sort_values()
    X_full = X_full.loc[idx]
    y_full = y_full.loc[idx]
    if not idx.is_unique:
        raise ValueError("The monthly evaluation index must be unique.")

    eval_start = pd.Timestamp(eval_cfg.eval_start).to_period("M").to_timestamp(how="start")
    eval_end = pd.Timestamp(eval_cfg.eval_end).to_period("M").to_timestamp(how="start")

    delay_map = None
    if eval_cfg.delay_style == "json_map":
        import json
        if eval_cfg.delay_json is None:
            raise ValueError("delay_style=json_map requires delay_json")
        with open(eval_cfg.delay_json, "r", encoding="utf-8") as f:
            delay_map = _validate_delay_map(json.load(f), X_full.columns)

    horizons = tuple(eval_cfg.horizons)
    scaling_mode = str(getattr(model_config, "scaling_mode", "external_frozen"))
    output_scaler = target_output_scaler or TargetOutputScaler()

    # Warm starts are only directly reusable under a fixed observation scale.
    warm_start_effective = bool(warm_start and scaling_mode == "external_frozen")
    min_T = int(max(12, 3 * max(1, int(getattr(model_config, "p", 1))) + 5))
    min_q_obs = 1

    def get_snapshot(t: pd.Timestamp) -> tuple[pd.DataFrame, pd.Series, dict[str, Any]]:
        t = pd.Timestamp(t).to_period("M").to_timestamp(how="start")
        cal = idx[idx <= t]
        metadata: dict[str, Any]
        if vintage_provider is None:
            X_v = X_full.loc[cal].copy()
            y_v = y_full.loc[cal].copy()
            metadata = {"mode": "revised_panel_with_release_masks"}
        else:
            as_of = t if eval_cfg.vintage_as_of_rule == "month_start" else (t + pd.offsets.MonthEnd(0) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1))
            snapshot: VintageSnapshot = vintage_provider.get_vintage(as_of)
            X_v = snapshot.X.copy()
            y_v = snapshot.y.copy()
            X_v.index = _normalize_month_start(X_v.index)
            y_v.index = _normalize_month_start(y_v.index)
            X_v = X_v.reindex(index=cal, columns=X_full.columns)
            y_v = y_v.reindex(index=cal)
            metadata = dict(snapshot.metadata)
            metadata.setdefault("mode", "historical_vintage")

        apply_masks = vintage_provider is None or bool(eval_cfg.apply_masks_to_vintage_provider)
        if apply_masks:
            X_v = _apply_delay_mask(
                X_v,
                eval_date=t,
                delay_style=str(eval_cfg.delay_style),
                delay_map=delay_map,
            )
            y_v = _mask_quarterly_target_release(
                y_v,
                eval_date=t,
                gdp_rel=int(eval_cfg.gdp_rel),
                as_of_rule=str(eval_cfg.vintage_as_of_rule),
            )
            y_v = _apply_quarter_end_leakage_guard(
                y_v, t, no_qe_leak=bool(eval_cfg.no_qe_leak)
            )
        return X_v, y_v, metadata

    params_fixed = None
    scaler_fixed: Optional[PanelScaler] = None
    if is_fixed:
        if train_end is None:
            raise ValueError("DFM-fixed requires train_end")
        train_end_ts = pd.Timestamp(train_end).to_period("M").to_timestamp(how="start")
        X_tr, y_tr_m, _train_meta = get_snapshot(train_end_ts)
        if len(X_tr) < min_T:
            raise ValueError(f"Training window too short. len(X_tr)={len(X_tr)} < {min_T}")
        if np.isfinite(y_tr_m.to_numpy(dtype=float)).sum() < min_q_obs:
            raise ValueError("Training window has no quarterly observations after masking.")

        cfg_tr = model_config
        if train_max_iter is not None and hasattr(cfg_tr, "max_iter"):
            try:
                cfg_tr = cfg_tr.__class__(**cfg_tr.__dict__)
            except Exception:
                pass
            cfg_tr.max_iter = int(train_max_iter)

        res_tr = _fit_one_vintage_em(
            X_v=X_tr,
            y_v=y_tr_m,
            fit_fn=fit_fn,
            model_config=cfg_tr,
            run_state=None,
            warm_start=False,
            verbose_em=True,
        )
        if bool(eval_cfg.require_convergence) and not bool(getattr(res_tr, "converged", False)):
            if eval_cfg.on_nonconvergence == "raise":
                raise RuntimeError("Fixed-parameter training fit did not converge.")
        if not hasattr(res_tr, "params"):
            raise AttributeError("Training fit result has no .params for fixed_params mode.")
        params_fixed = res_tr.params
        scaler_fixed = getattr(res_tr, "scaler", None)
        if not isinstance(scaler_fixed, PanelScaler):
            raise TypeError("Training fit must return a PanelScaler for fixed_params evaluation.")

    out_rows: list[dict[str, Any]] = []
    run_state = PRTRunState() if (warm_start_effective and not is_fixed) else None
    vintages = [t for t in idx if eval_start <= t <= eval_end]

    pbar = tqdm(vintages, desc="Pseudo/real-time vintages", unit="vintage")
    for t in pbar:
        t = pd.Timestamp(t)
        pbar.set_postfix_str(t.strftime("%Y-%m"))
        X_v, y_v, vintage_meta = get_snapshot(t)
        if len(X_v) < min_T or np.isfinite(y_v.to_numpy(dtype=float)).sum() < min_q_obs:
            continue

        active_horizons: list[str] = []
        for horizon in horizons:
            target_date = _horizon_target_date(t, horizon)
            target_observed = bool(
                target_date in y_v.index and np.isfinite(float(y_v.loc[target_date]))
            )
            if horizon == "bac" and target_observed:
                continue
            active_horizons.append(horizon)
        if not active_horizons:
            continue

        if is_fixed:
            if params_fixed is None or scaler_fixed is None:
                raise RuntimeError("Fixed parameters/scaler are unavailable.")
            res = _smooth_fixed_params(
                X_v=X_v,
                y_v=y_v,
                params_fixed=params_fixed,
                scaler_fixed=scaler_fixed,
                model_config=model_config,
            )
        else:
            res = _fit_one_vintage_em(
                X_v=X_v,
                y_v=y_v,
                fit_fn=fit_fn,
                model_config=model_config,
                run_state=run_state,
                warm_start=warm_start_effective,
                verbose_em=False,
            )

        fit_converged = bool(getattr(res, "converged", True))
        if bool(eval_cfg.require_convergence) and not fit_converged:
            if eval_cfg.on_nonconvergence == "raise":
                raise RuntimeError(f"DFM fit did not converge at vintage {t:%Y-%m}.")
            if eval_cfg.on_nonconvergence == "skip":
                continue

        n_obs_expected = int(X_v.shape[1] + 1)
        A, C = _extract_transition_and_measurement(res, n_obs_expected=n_obs_expected)
        Q = np.asarray(getattr(res, "Q", np.zeros_like(A)), dtype=float)
        R = np.asarray(getattr(res, "R", np.zeros(n_obs_expected)), dtype=float)
        R_q = float(R[-1]) if R.ndim == 1 else float(R[-1, -1])
        mu_y, sd_y = _target_location_scale(
            res,
            n_obs_expected=n_obs_expected,
            scaling_mode=scaling_mode,
        )

        a_smooth = np.asarray(_to_attr(res, "a_smooth"), dtype=float)
        P_smooth = np.asarray(
            getattr(res, "P_smooth", np.zeros((a_smooth.shape[0], A.shape[0], A.shape[0]))),
            dtype=float,
        )
        if a_smooth.shape[0] != len(X_v) or P_smooth.shape[0] != len(X_v):
            raise ValueError("Smoothed state output is not aligned with the vintage panel.")

        Zq = C[-1, :]
        moq = _month_in_quarter(t)
        last_date = pd.Timestamp(X_v.index[-1])
        diag = getattr(res, "diagnostics", None) or {}

        for h in active_horizons:
            target_date = _horizon_target_date(t, h)
            target_available_in_information_set = bool(
                target_date in y_v.index and np.isfinite(float(y_v.loc[target_date]))
            )
            # A real-time backcast exists only while the completed quarter's GDP
            # release is genuinely unavailable in the as-of target history. Once it
            # is observed, skip the backcast rather than scoring a smoothed in-sample fit.
            if h == "bac" and target_available_in_information_set:
                continue
            actual_input = float(y_full.loc[target_date]) if target_date in y_full.index else float("nan")

            if target_date < X_v.index[0]:
                pred_model = pred_var_model = float("nan")
            elif target_date <= last_date and target_date in X_v.index:
                pos = int(X_v.index.get_loc(target_date))
                moments = observation_predictive_moments(
                    a_smooth[pos], P_smooth[pos], Zq, R_q
                )
                pred_model = moments.observation_mean
                pred_var_model = moments.observation_var
            else:
                steps = max(0, _months_diff(target_date, last_date))
                a_for, P_for = propagate_state_moments(
                    a_smooth[-1], P_smooth[-1], A, Q, steps
                )
                moments = observation_predictive_moments(a_for, P_for, Zq, R_q)
                pred_model = moments.observation_mean
                pred_var_model = moments.observation_var

            pred_sd_model = (
                float(np.sqrt(max(pred_var_model, 0.0)))
                if np.isfinite(pred_var_model)
                else float("nan")
            )
            pred_input = pred_model * sd_y + mu_y if np.isfinite(pred_model) else float("nan")
            pred_sd_input = pred_sd_model * abs(sd_y) if np.isfinite(pred_sd_model) else float("nan")
            actual_model = (
                (actual_input - mu_y) / sd_y
                if np.isfinite(actual_input) and sd_y != 0.0
                else float("nan")
            )
            pred_raw = output_scaler.to_output(pred_input) if np.isfinite(pred_input) else float("nan")
            actual_raw = output_scaler.to_output(actual_input) if np.isfinite(actual_input) else float("nan")
            pred_sd_raw = output_scaler.scale_sd(pred_sd_input) if np.isfinite(pred_sd_input) else float("nan")

            row: dict[str, Any] = {
                "eval_date": t,
                "moq": int(moq),
                "horizon": h,
                "target_date": pd.Timestamp(target_date),
                "pred_model_scale": float(pred_model),
                "actual_model_scale": float(actual_model),
                "pred_input_scale": float(pred_input),
                "actual_input_scale": float(actual_input),
                "pred_sd_model_scale": float(pred_sd_model),
                "pred_sd_input_scale": float(pred_sd_input),
                "pred_raw": float(pred_raw),
                "actual_raw": float(actual_raw),
                "pred_sd_raw": float(pred_sd_raw),
                # Backward-compatible aliases.
                "pred_scaled": float(pred_model),
                "actual_scaled": float(actual_model),
                "pred": float(pred_raw),
                "actual": float(actual_raw),
                "scaling_mode": scaling_mode,
                "output_scale_label": output_scaler.label,
                "scaler_mu_y": float(mu_y),
                "scaler_sd_y": float(sd_y),
                "output_scaler_mean": float(output_scaler.mean),
                "output_scaler_std": float(output_scaler.std),
                "warm_start_used": bool(warm_start_effective and not is_fixed),
                "parameter_mode": parameter_mode,
                "fixed_params": bool(is_fixed),
                "target_available_in_information_set": target_available_in_information_set,
                "vintage_mode": str(vintage_meta.get("mode", "unknown")),
                "em_converged": fit_converged,
                "em_iterations": int(diag.get("iterations", len(getattr(res, "loglik_trace", [])))),
                "em_final_loglik": float(diag.get("final_loglik", float("nan"))),
                "em_min_loglik_increment": float(diag.get("minimum_loglik_increment", float("nan"))),
                "em_rejected_steps": int(diag.get("rejected_steps", 0)),
                "no_qe_leak": bool(eval_cfg.no_qe_leak),
                "gdp_rel": int(eval_cfg.gdp_rel),
            }

            for level in eval_cfg.prediction_interval_levels:
                tag = int(round(100.0 * float(level)))
                z = NormalDist().inv_cdf(0.5 * (1.0 + float(level)))
                row[f"pi{tag}_lower_raw"] = float(pred_raw - z * pred_sd_raw)
                row[f"pi{tag}_upper_raw"] = float(pred_raw + z * pred_sd_raw)

            out_rows.append(row)

    pred_df = pd.DataFrame(out_rows)
    if not pred_df.empty:
        pred_df = pred_df.sort_values(["eval_date", "horizon", "moq"]).reset_index(drop=True)
    scores = compute_scores(pred_df)
    return pred_df, scores


# -----------------------------------------------------------------------------
# Scoring + MIQ pivot
# -----------------------------------------------------------------------------

def compute_scores(pred_df: pd.DataFrame) -> dict:
    if pred_df.empty:
        return {"rmse": float("nan"), "directional_accuracy": float("nan"), "by_moq": {}}

    df = pred_df.copy()
    mask = np.isfinite(df["pred"].to_numpy(float)) & np.isfinite(df["actual"].to_numpy(float))
    if int(mask.sum()) == 0:
        return {"rmse": float("nan"), "directional_accuracy": float("nan"), "by_moq": {}}

    err = df.loc[mask, "pred"].to_numpy(float) - df.loc[mask, "actual"].to_numpy(float)
    out: dict[str, Any] = {
        "rmse": float(np.sqrt(np.mean(err * err))),
        "mae": float(np.mean(np.abs(err))),
        "n_obs": int(mask.sum()),
    }

    now = df[df["horizon"] == "now"].sort_values(["target_date", "moq"])
    now = now.groupby("target_date").tail(1) if not now.empty else now
    if len(now) < 2:
        out["directional_accuracy"] = float("nan")
    else:
        p = now["pred"].to_numpy(float)
        a = now["actual"].to_numpy(float)
        valid = np.isfinite(p) & np.isfinite(a)
        p, a = p[valid], a[valid]
        out["directional_accuracy"] = (
            float(np.mean((np.diff(p) >= 0) == (np.diff(a) >= 0)))
            if len(p) >= 2 else float("nan")
        )

    by_moq: dict[str, dict[str, float]] = {}
    for moq, sub in df[df["horizon"] == "now"].groupby("moq"):
        valid = np.isfinite(sub["pred"].to_numpy(float)) & np.isfinite(sub["actual"].to_numpy(float))
        e = sub.loc[valid, "pred"].to_numpy(float) - sub.loc[valid, "actual"].to_numpy(float)
        by_moq[f"m{int(moq)}"] = {
            "rmse": float(np.sqrt(np.mean(e * e))) if e.size else float("nan"),
            "mae": float(np.mean(np.abs(e))) if e.size else float("nan"),
            "n_obs": int(e.size),
        }
    out["by_moq"] = by_moq

    coverage: dict[str, float] = {}
    for col in df.columns:
        if col.startswith("pi") and col.endswith("_lower_raw"):
            tag = col[2:].split("_")[0]
            upper = f"pi{tag}_upper_raw"
            if upper not in df.columns:
                continue
            valid = (
                np.isfinite(df[col].to_numpy(float))
                & np.isfinite(df[upper].to_numpy(float))
                & np.isfinite(df["actual_raw"].to_numpy(float))
            )
            if int(valid.sum()):
                actual = df.loc[valid, "actual_raw"]
                coverage[f"pi{tag}"] = float(
                    np.mean((actual >= df.loc[valid, col]) & (actual <= df.loc[valid, upper]))
                )
    out["interval_coverage"] = coverage
    return out


def _miq_pivot(pred_df: pd.DataFrame) -> pd.DataFrame:
    df = pred_df[pred_df["horizon"] == "now"].copy()
    keep = ["target_date", "moq", "pred_raw", "actual_raw"]
    df = df[keep].sort_values(["target_date", "moq"]).drop_duplicates(["target_date", "moq"], keep="last")

    wide = df.pivot(index="target_date", columns="moq", values="pred_raw")
    wide.columns = [f"nowcast_m{int(c)}" for c in wide.columns]

    actual = df.groupby("target_date")["actual_raw"].last().rename("actual")
    out = pd.concat([actual, wide], axis=1).sort_index()

    for c in ["nowcast_m1", "nowcast_m2", "nowcast_m3"]:
        if c not in out.columns:
            out[c] = np.nan
    return out


def _rmse(a, b) -> float:
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    m = np.isfinite(a) & np.isfinite(b)
    if int(m.sum()) == 0:
        return float("nan")
    e = a[m] - b[m]
    return float(np.sqrt(np.mean(e * e)))


def _directional_accuracy(series_pred, series_actual) -> float:
    p = np.asarray(series_pred, float)
    a = np.asarray(series_actual, float)
    m = np.isfinite(p) & np.isfinite(a)
    p = p[m]
    a = a[m]
    if p.size < 2:
        return float("nan")
    return float(np.mean((np.diff(p) >= 0) == (np.diff(a) >= 0)))


def _print_miq_scores(miq_df: pd.DataFrame) -> None:
    rmse_m1 = _rmse(miq_df["nowcast_m1"], miq_df["actual"])
    rmse_m2 = _rmse(miq_df["nowcast_m2"], miq_df["actual"])
    rmse_m3 = _rmse(miq_df["nowcast_m3"], miq_df["actual"])
    da_m3 = _directional_accuracy(miq_df["nowcast_m3"], miq_df["actual"])

    print("Month-in-quarter nowcast RMSE (target units as provided):")
    print(f"  m1: {rmse_m1:.6g}")
    print(f"  m2: {rmse_m2:.6g}")
    print(f"  m3: {rmse_m3:.6g}")
    print("Directional accuracy (quarter-to-quarter, using m3 nowcasts):")
    print(f"  DA: {da_m3:.4f}")


# -----------------------------------------------------------------------------
# IO helpers
# -----------------------------------------------------------------------------

def _read_panel_target(panel_csv: str, target_csv: str) -> tuple[pd.DataFrame, pd.Series]:
    X = pd.read_csv(panel_csv, index_col=0, parse_dates=True)
    ydf = pd.read_csv(target_csv, index_col=0, parse_dates=True)

    X.index = _normalize_month_start(X.index)
    ydf.index = _normalize_month_start(ydf.index)
    validate_quarter_end_target_alignment(ydf, name=str(target_csv))

    ynum = ydf.select_dtypes(include=[np.number])
    if ynum.shape[1] == 0:
        raise ValueError(f"No numeric columns in target CSV: {target_csv}")
    y = ynum.iloc[:, 0].astype(float)

    idx = X.index.intersection(y.index)
    X = X.loc[idx].sort_index()
    y = y.loc[idx].sort_index()
    return X, y


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main(argv=None) -> int:
    import argparse

    from dfm_pipeline.dfm_bm_ml.fit import fit_bm_dfm_fast, fit_bm_dfm_fast_numba
    from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig

    ap = argparse.ArgumentParser(
        description=(
            "BM-DFM pseudo-real-time loop over monthly vintages. "
            "Writes a CSV with quarter-end nowcasts by month-in-quarter (m1/m2/m3) "
            "and prints RMSE + directional accuracy."
        )
    )

    ap.add_argument("--panel", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--dataset", default="baseline")
    ap.add_argument("--method", choices=["fast", "numba"], default="numba")

    ap.add_argument("--r", type=int, required=True)
    ap.add_argument("--p", type=int, required=True)

    ap.add_argument("--max_iter", type=int, default=200)
    ap.add_argument("--tol", type=float, default=1e-6)
    ap.add_argument("--convergence_mode", choices=["absolute_ll", "toolbox_rel"], default="toolbox_rel")

    ap.add_argument("--eval_start", default=None)
    ap.add_argument("--eval_end", default=None)

    ap.add_argument(
        "--delay_style",
        required=True,
        choices=["none", "json_map"],
        help="Information-release policy; must be selected explicitly",
    )
    ap.add_argument("--delay_json", default=None)

    ap.add_argument("--gdp_rel", type=int, default=1)
    ap.add_argument("--warm_start", action="store_true")
    ap.add_argument("--blas_threads", type=int, default=1)

    ap.add_argument("--outdir", default="outputs/bm_pseudort_fast")
    ap.add_argument("--out_csv", default=None)

    ap.add_argument("--mm_weight_style", choices=["toolbox", "scaled"], default="toolbox")
    ap.add_argument("--enforce_quarterly_loading_constraint", action="store_true")
    ap.add_argument("--no_enforce_quarterly_loading_constraint", action="store_true")
    ap.add_argument("--fix_quarterly_R", action="store_true")
    ap.add_argument("--no_fix_quarterly_R", action="store_true")
    ap.add_argument(
        "--scaling_mode",
        choices=["external_frozen", "internal_per_run", "toolbox_vintage"],
        default="external_frozen",
    )

    ap.add_argument(
        "--parameter_mode",
        choices=["recursive", "fixed"],
        default="recursive",
        help="DFM-recursive re-estimates each vintage; DFM-fixed estimates once at train_end.",
    )
    ap.add_argument("--fixed_params", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--train_end", default=None)
    ap.add_argument("--train_max_iter", type=int, default=None)

    ap.add_argument(
        "--no_qe_leak",
        action="store_true",
        default=True,
        help="Deprecated compatibility flag; quarter-end leakage protection is always enabled",
    )

    args = ap.parse_args(argv)

    if int(args.blas_threads) > 0:
        limit_blas_threads(int(args.blas_threads))

    base = Path("dataset") / args.panel / args.dataset / args.tag
    panel_csv = base / "X_panel_z__bm.csv"
    target_csv = base / "y_target_z__bm_monthly.csv"

    if not panel_csv.exists():
        raise FileNotFoundError(str(panel_csv))
    if not target_csv.exists():
        raise FileNotFoundError(str(target_csv))

    X_full, y_full = _read_panel_target(str(panel_csv), str(target_csv))

    eval_start = args.eval_start or str(X_full.index[0].date())
    eval_end = args.eval_end or str(X_full.index[-1].date())

    eval_cfg = EvalConfig(
        eval_start=eval_start,
        eval_end=eval_end,
        delay_style=args.delay_style,
        delay_json=args.delay_json,
        gdp_rel=int(args.gdp_rel),
        horizons=("now",),
        no_qe_leak=True,
        parameter_mode=("fixed" if bool(args.fixed_params) else str(args.parameter_mode)),
    )

    cfg = BMDfmConfig(
        r_by_block=(int(args.r),),
        p=int(args.p),
        idio_ar1=True,
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        convergence_mode=str(args.convergence_mode),
        mm_weight_style=args.mm_weight_style,
        scaling_mode=args.scaling_mode,
    )

    if hasattr(cfg, "enforce_quarterly_loading_constraint"):
        if args.no_enforce_quarterly_loading_constraint:
            cfg.enforce_quarterly_loading_constraint = False
        elif args.enforce_quarterly_loading_constraint:
            cfg.enforce_quarterly_loading_constraint = True

    if hasattr(cfg, "fix_quarterly_R"):
        if args.no_fix_quarterly_R:
            cfg.fix_quarterly_R = False
        elif args.fix_quarterly_R:
            cfg.fix_quarterly_R = True

    cfg.validate()

    fit_fn = fit_bm_dfm_fast_numba if args.method == "numba" else fit_bm_dfm_fast

    pred_df, _scores = run_pseudo_rt_eval_fast(
        X_full=X_full,
        y_full=y_full,
        fit_fn=fit_fn,
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=bool(args.warm_start),
        train_end=args.train_end,
        train_max_iter=args.train_max_iter,
        blas_threads=int(args.blas_threads),
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    miq_df = _miq_pivot(pred_df)

    out_csv = Path(args.out_csv) if args.out_csv else outdir / (
        f"bm_pseudort_fast_miq__{args.panel}__{args.dataset}__{args.tag}__{args.method}"
        f"__r{args.r}__p{args.p}"
        + ("__fixed" if (args.fixed_params or args.parameter_mode == "fixed") else "")
        + ("__no_qe_leak" if args.no_qe_leak else "")
        + ".csv"
    )

    miq_df.to_csv(out_csv)
    print(f"Wrote month-in-quarter nowcasts to: {out_csv}")

    _print_miq_scores(miq_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
