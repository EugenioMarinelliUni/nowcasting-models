# src/dfm_pipeline/eval_pseudort/fast/bm_pseudort_fast.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional, Tuple

import numpy as np
import pandas as pd

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


def _months_diff(a: pd.Timestamp, b: pd.Timestamp) -> int:
    a = pd.Timestamp(a).to_period("M").to_timestamp(how="start")
    b = pd.Timestamp(b).to_period("M").to_timestamp(how="start")
    return (a.year - b.year) * 12 + (a.month - b.month)


def _safe_mean_std(x: np.ndarray) -> tuple[float, float]:
    x = np.asarray(x, float)
    m = np.isfinite(x)
    if int(m.sum()) == 0:
        return 0.0, 1.0
    mu = float(np.mean(x[m]))
    sd = float(np.std(x[m]))
    if (not np.isfinite(sd)) or sd == 0.0:
        sd = 1.0
    return mu, sd


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

def _apply_delay_mask(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    delay_style: str,
    delay_map: Optional[dict],
) -> pd.DataFrame:
    if delay_style in ("none", "trailing_nan"):
        return X

    if delay_style != "json_map":
        raise ValueError(f"Unknown delay_style: {delay_style!r}")

    if delay_map is None:
        raise ValueError("delay_style=json_map requires delay_map")

    X = X.copy()
    eval_ms = pd.Timestamp(eval_date).to_period("M").to_timestamp(how="start")

    for col, d in delay_map.items():
        if col not in X.columns:
            continue
        d = int(d)
        cutoff = eval_ms - pd.offsets.MonthBegin(d)
        X.loc[X.index > cutoff, col] = np.nan

    return X


def _mask_quarterly_target_release(
    y: pd.Series,
    eval_date: pd.Timestamp,
    gdp_rel: int,
) -> pd.Series:
    y = y.copy()
    if gdp_rel <= 0:
        return y

    y_idx = pd.DatetimeIndex(y.index)
    q_end = y_idx.to_period("Q").end_time.to_period("M").to_timestamp(how="start")
    release_month = q_end + pd.offsets.MonthBegin(int(gdp_rel))

    eval_ms = pd.Timestamp(eval_date).to_period("M").to_timestamp(how="start")
    mask = release_month > eval_ms
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
    model_config: Any,
):
    from dfm_pipeline.dfm_bm_ml.state_builder import build_state_space
    from dfm_pipeline.dfm_dyn.state_space_new import kalman_filter, kalman_smoother

    Y_monthly = X_v.to_numpy(dtype=float)
    y_quarterly = y_v.to_numpy(dtype=float)

    ss_out = _call_supported(
        build_state_space,
        params=params_fixed,
        nM=int(Y_monthly.shape[1]),
        nQ=1,
        r_by_block=tuple(int(x) for x in getattr(model_config, "r_by_block")),
        p=int(getattr(model_config, "p")),
        ppC=int(getattr(model_config, "ppC", 5)),
        mm_style=str(getattr(model_config, "mm_weight_style", "toolbox")),
        quarterly_meas_var_floor=float(getattr(model_config, "quarterly_meas_var_floor", 1e-6)),
        idio_ar1=bool(getattr(model_config, "idio_ar1", True)),
        jitter=float(getattr(model_config, "jitter", 1e-8)),
        P0_mode=str(getattr(model_config, "P0_mode", "diffuse")),
        a0_override=None,
        P0_override=None,
    )

    Tm, Qm, C_meas, R_meas, a0, P0, _idx = ss_out

    Y_obs = np.concatenate([Y_monthly, y_quarterly[:, None]], axis=1)

    kf = _call_supported(
        kalman_filter,
        Y=Y_obs,
        y=Y_obs,
        Z=C_meas,
        T=Tm,
        Q=Qm,
        R=R_meas,
        a0=a0,
        P0=P0,
    )

    try:
        ks = kalman_smoother(kf)
    except TypeError:
        try:
            ks = kalman_smoother(kf_res=kf)
        except TypeError:
            ks = _call_supported(
                kalman_smoother,
                kf_res=kf,
                kf=kf,
                Y=Y_obs,
                y=Y_obs,
                Z=C_meas,
                T=Tm,
                Q=Qm,
                R=R_meas,
            )

    a_smooth = np.asarray(_to_attr(ks, "a_smooth"), dtype=float)
    return SimpleNamespace(A=Tm, C=C_meas, a_smooth=a_smooth)


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
    gdp_rel: int = 0
    horizons: Tuple[str, ...] = ("now",)
    no_qe_leak: bool = False


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
    fixed_params: bool = False,
    train_end: Optional[str] = None,
    train_max_iter: Optional[int] = None,
    blas_threads: int | None = None,
) -> Tuple[pd.DataFrame, dict]:
    from tqdm.auto import tqdm

    if blas_threads is not None and int(blas_threads) > 0:
        limit_blas_threads(int(blas_threads))

    X_full = X_full.copy()
    X_full.index = _normalize_month_start(X_full.index)

    y_full = y_full.copy()
    y_full.index = _normalize_month_start(y_full.index)

    idx = X_full.index.intersection(y_full.index).sort_values()
    X_full = X_full.loc[idx]
    y_full = y_full.loc[idx]

    eval_start = pd.Timestamp(eval_cfg.eval_start).to_period("M").to_timestamp(how="start")
    eval_end = pd.Timestamp(eval_cfg.eval_end).to_period("M").to_timestamp(how="start")

    delay_map = None
    if getattr(eval_cfg, "delay_style", "none") == "json_map":
        import json

        if eval_cfg.delay_json is None:
            raise ValueError("delay_style=json_map requires delay_json")
        with open(eval_cfg.delay_json, "r", encoding="utf-8") as f:
            delay_map = json.load(f)

    horizons = tuple(getattr(eval_cfg, "horizons", ("now",)))

    scaling_mode = str(getattr(model_config, "scaling_mode", "external_frozen"))
    if scaling_mode == "external_frozen":
        mu_y, sd_y = 0.0, 1.0
    else:
        mu_y, sd_y = _safe_mean_std(y_full.to_numpy(dtype=float))

    min_T = int(max(12, 3 * max(1, int(getattr(model_config, "p", 1))) + 5))
    min_q_obs = 1

    params_fixed = None
    if fixed_params:
        if train_end is None:
            raise ValueError("fixed_params=True requires train_end")
        train_end_ts = pd.Timestamp(train_end).to_period("M").to_timestamp(how="start")

        X_tr = X_full.loc[:train_end_ts]
        y_tr = y_full.loc[:train_end_ts]

        if len(X_tr) < min_T:
            raise ValueError(f"Training window too short. len(X_tr)={len(X_tr)} < {min_T}")

        y_tr_m = _mask_quarterly_target_release(y_tr, eval_date=train_end_ts, gdp_rel=int(eval_cfg.gdp_rel))
        y_tr_m = _apply_quarter_end_leakage_guard(y_tr_m, train_end_ts, no_qe_leak=bool(eval_cfg.no_qe_leak))

        if np.isfinite(y_tr_m.to_numpy(dtype=float)).sum() < min_q_obs:
            raise ValueError("Training window has no quarterly observations after masking.")

        cfg_tr = model_config
        if train_max_iter is not None and hasattr(cfg_tr, "max_iter"):
            try:
                cfg_tr = cfg_tr.__class__(**cfg_tr.__dict__)
            except Exception:
                pass
            try:
                cfg_tr.max_iter = int(train_max_iter)
            except Exception:
                pass

        res_tr = _fit_one_vintage_em(
            X_v=X_tr,
            y_v=y_tr_m,
            fit_fn=fit_fn,
            model_config=cfg_tr,
            run_state=None,
            warm_start=False,
            verbose_em=True,
        )
        if not hasattr(res_tr, "params"):
            raise AttributeError("Training fit result has no .params for fixed_params mode.")
        params_fixed = res_tr.params

    out_rows = []
    run_state = PRTRunState() if (warm_start and (not fixed_params)) else None

    vintages = [t for t in idx if (t >= eval_start and t <= eval_end)]
    pbar = tqdm(vintages, desc="Pseudo-RT vintages", unit="vintage")
    for t in pbar:
        pbar.set_postfix_str(pd.Timestamp(t).strftime("%Y-%m"))

        X_v = X_full.loc[:t]
        y_v = y_full.loc[:t]

        if len(X_v) < min_T:
            continue

        X_v = _apply_delay_mask(
            X_v,
            eval_date=pd.Timestamp(t),
            delay_style=str(getattr(eval_cfg, "delay_style", "none")),
            delay_map=delay_map,
        )

        y_v = _mask_quarterly_target_release(
            y_v,
            eval_date=pd.Timestamp(t),
            gdp_rel=int(getattr(eval_cfg, "gdp_rel", 0)),
        )

        y_v = _apply_quarter_end_leakage_guard(
            y_v,
            pd.Timestamp(t),
            no_qe_leak=bool(getattr(eval_cfg, "no_qe_leak", False)),
        )

        if np.isfinite(y_v.to_numpy(dtype=float)).sum() < min_q_obs:
            continue

        if fixed_params:
            if params_fixed is None:
                raise RuntimeError("fixed_params=True but params_fixed is None")
            res = _smooth_fixed_params(X_v=X_v, y_v=y_v, params_fixed=params_fixed, model_config=model_config)
        else:
            res = _fit_one_vintage_em(
                X_v=X_v,
                y_v=y_v,
                fit_fn=fit_fn,
                model_config=model_config,
                run_state=run_state,
                warm_start=warm_start,
                verbose_em=False,
            )

        n_obs_expected = int(X_v.shape[1] + 1)
        Tmat, Z = _extract_transition_and_measurement(res, n_obs_expected=n_obs_expected)

        a_smooth = np.asarray(_to_attr(res, "a_smooth"), dtype=float)
        a_last = a_smooth[-1, :]

        Zq = Z[-1, :]
        moq = _month_in_quarter(pd.Timestamp(t))

        for h in horizons:
            if h != "now":
                raise ValueError(f"Unsupported horizon: {h!r}")

            target_date = _quarter_end_month(pd.Timestamp(t))

            if target_date not in idx:
                pred_scaled = float("nan")
                actual_raw = float("nan")
            else:
                steps = _months_diff(pd.Timestamp(target_date), pd.Timestamp(t))
                if steps < 0:
                    steps = 0
                a_for = _forecast_state(Tmat, a_last, steps)
                pred_scaled = float(Zq @ a_for)
                actual_raw = float(y_full.loc[target_date]) if target_date in y_full.index else float("nan")

            pred_raw = float(pred_scaled * sd_y + mu_y) if np.isfinite(pred_scaled) else float("nan")
            actual_scaled = (
                float((actual_raw - mu_y) / sd_y) if np.isfinite(actual_raw) and sd_y != 0 else float("nan")
            )

            out_rows.append(
                {
                    "eval_date": pd.Timestamp(t),
                    "moq": int(moq),
                    "horizon": h,
                    "target_date": pd.Timestamp(target_date),
                    "pred_scaled": pred_scaled,
                    "actual_scaled": actual_scaled,
                    "pred_raw": pred_raw,
                    "actual_raw": actual_raw,
                    "pred": pred_raw,
                    "actual": actual_raw,
                    "scaling_mode": scaling_mode,
                    "fixed_params": bool(fixed_params),
                    "no_qe_leak": bool(getattr(eval_cfg, "no_qe_leak", False)),
                    "gdp_rel": int(getattr(eval_cfg, "gdp_rel", 0)),
                }
            )

    pred_df = pd.DataFrame(out_rows).sort_values(["eval_date", "horizon", "moq"]).reset_index(drop=True)
    scores = compute_scores(pred_df)
    return pred_df, scores


# -----------------------------------------------------------------------------
# Scoring + MIQ pivot
# -----------------------------------------------------------------------------

def compute_scores(pred_df: pd.DataFrame) -> dict:
    df = pred_df.copy()
    m = np.isfinite(df["pred"].to_numpy(dtype=float)) & np.isfinite(df["actual"].to_numpy(dtype=float))
    if int(m.sum()) == 0:
        return {"rmse": float("nan"), "directional_accuracy": float("nan")}

    e = df.loc[m, "pred"].to_numpy(dtype=float) - df.loc[m, "actual"].to_numpy(dtype=float)
    rmse = float(np.sqrt(np.mean(e * e)))

    dff = df[df["horizon"] == "now"].copy()
    if dff.empty:
        da = float("nan")
    else:
        dff = dff.sort_values(["target_date", "moq"]).groupby("target_date").tail(1)
        p = dff["pred"].to_numpy(float)
        a = dff["actual"].to_numpy(float)
        mm = np.isfinite(p) & np.isfinite(a)
        p = p[mm]
        a = a[mm]
        if p.size < 2:
            da = float("nan")
        else:
            da = float(np.mean((np.diff(p) >= 0) == (np.diff(a) >= 0)))

    return {"rmse": rmse, "directional_accuracy": da}


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

    ap.add_argument("--delay_style", default="none", choices=["none", "trailing_nan", "json_map"])
    ap.add_argument("--delay_json", default=None)

    ap.add_argument("--gdp_rel", type=int, default=0)
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

    ap.add_argument("--fixed_params", action="store_true")
    ap.add_argument("--train_end", default=None)
    ap.add_argument("--train_max_iter", type=int, default=None)

    ap.add_argument("--no_qe_leak", action="store_true", help="Do not use quarter-end target observation at moq=3")

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
        no_qe_leak=bool(args.no_qe_leak),
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
        fixed_params=bool(args.fixed_params),
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
        + ("__fixed" if args.fixed_params else "")
        + ("__no_qe_leak" if args.no_qe_leak else "")
        + ".csv"
    )

    miq_df.to_csv(out_csv)
    print(f"Wrote month-in-quarter nowcasts to: {out_csv}")

    _print_miq_scores(miq_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
