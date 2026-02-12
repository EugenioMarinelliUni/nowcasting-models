from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from typing import Optional, Literal, Tuple, Callable, Any, Dict

import inspect
import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother


Horizon = Literal["bac", "now", "for"]
DelayStyle = Literal["none", "trailing_nan", "json_map"]


def month_of_quarter(d: pd.Timestamp) -> int:
    m = int(d.month)
    r = m % 3
    return 3 if r == 0 else r


def quarter_end_stamp(d: pd.Timestamp) -> pd.Timestamp:
    q = (int(d.month) - 1) // 3 + 1
    end_month = 3 * q
    return pd.Timestamp(year=int(d.year), month=end_month, day=1)


def add_months(d: pd.Timestamp, k: int) -> pd.Timestamp:
    return (d.to_period("M") + int(k)).to_timestamp(how="start")


def months_diff(start: pd.Timestamp, end: pd.Timestamp) -> int:
    """
    Integer number of months to move from start -> end.
    Positive if end is after start, negative otherwise.
    """
    return (int(end.year) - int(start.year)) * 12 + (int(end.month) - int(start.month))


def trailing_nan_delays(X: pd.DataFrame) -> pd.Series:
    delays: dict[str, int] = {}
    arr = X.to_numpy()
    for j, c in enumerate(X.columns):
        col = arr[:, j]
        k = 0
        for v in col[::-1]:
            if np.isfinite(v):
                break
            k += 1
        delays[str(c)] = int(k)
    return pd.Series(delays)


def apply_ragged_edge_mask(X: pd.DataFrame, delays: pd.Series, t_end: pd.Timestamp) -> pd.DataFrame:
    Xrt = X.loc[:t_end].copy()
    if Xrt.empty:
        return Xrt
    for c in Xrt.columns:
        d = int(delays.get(c, 0))
        if d > 0 and len(Xrt) >= d:
            Xrt.iloc[-d:, Xrt.columns.get_loc(c)] = np.nan
    return Xrt


def mask_quarterly_release(y: pd.Series, t_end: pd.Timestamp, gdp_rel: int) -> pd.Series:
    yrt = y.loc[:t_end].copy()
    if gdp_rel <= 0 or yrt.empty:
        return yrt
    d = int(gdp_rel)
    if len(yrt) >= d:
        yrt.iloc[-d:] = np.nan
    return yrt


def _sym(A: np.ndarray) -> np.ndarray:
    return 0.5 * (A + A.T)


def _chol_factor(A: np.ndarray, jitter: float = 1e-10, max_tries: int = 8) -> np.ndarray:
    A = _sym(A)
    j = 0.0
    for _ in range(max_tries):
        try:
            return np.linalg.cholesky(A + j * np.eye(A.shape[0]))
        except np.linalg.LinAlgError:
            j = jitter if j == 0.0 else (10.0 * j)
    w, V = np.linalg.eigh(A)
    w = np.maximum(w, jitter)
    A_pd = (V * w) @ V.T
    return np.linalg.cholesky(_sym(A_pd))


def _chol_solve(L: np.ndarray, B: np.ndarray) -> np.ndarray:
    y = np.linalg.solve(L, B)
    return np.linalg.solve(L.T, y)


def _kalman_update_subset(
    a_pr: np.ndarray,
    P_pr: np.ndarray,
    y_obs: np.ndarray,
    C_sub: np.ndarray,
    R_sub: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    One Kalman measurement update:
      prior (a_pr, P_pr) -> posterior (a_upd, P_upd)
    using only a subset of observation rows.
    """
    if y_obs.size == 0:
        return a_pr, P_pr

    v = y_obs - C_sub @ a_pr
    S = _sym(C_sub @ P_pr @ C_sub.T + R_sub)

    L = _chol_factor(S, jitter=1e-10)
    B = C_sub @ P_pr
    S_inv_B = _chol_solve(L, B)
    K = S_inv_B.T

    I_m = np.eye(P_pr.shape[0])
    I_KC = I_m - K @ C_sub
    P_upd = _sym(I_KC @ P_pr @ I_KC.T + K @ R_sub @ K.T)
    a_upd = a_pr + K @ v
    return a_upd, P_upd


def forecast_from_last(
    T: np.ndarray,
    Q: np.ndarray,
    a_last: np.ndarray,
    P_last: np.ndarray,
    steps: int,
) -> Tuple[np.ndarray, np.ndarray]:
    a = a_last.copy()
    P = P_last.copy()
    for _ in range(int(steps)):
        a = T @ a
        P = T @ P @ T.T + Q
    return a, P


@dataclass(frozen=True)
class PseudoRTEvalConfig:
    eval_start: str
    eval_end: str

    delay_style: DelayStyle = "none"
    delay_json: Optional[str] = None
    gdp_rel: int = 0

    horizons: Tuple[Horizon, ...] = ("bac", "now", "for")

    score_by_moq: bool = True
    score_by_covid: bool = True


def _filter_kwargs_for_dataclass(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not is_dataclass(cls):
        return kwargs
    valid = {f.name for f in fields(cls)}
    return {k: v for k, v in kwargs.items() if k in valid}


def compute_scores(pred_df: pd.DataFrame) -> dict:
    out: dict = {}
    df = pred_df.copy()

    df["err"] = df["pred"] - df["actual"]
    df["se"] = df["err"] ** 2

    def rmse_from_se(se: pd.Series) -> float:
        v = se.to_numpy(dtype=float)
        v = v[np.isfinite(v)]
        if v.size == 0:
            return float("nan")
        return float(np.sqrt(np.mean(v)))

    def fda(pred: pd.Series, actual: pd.Series) -> float:
        p = pred.to_numpy(dtype=float)
        a = actual.to_numpy(dtype=float)
        ok = np.isfinite(p) & np.isfinite(a) & (p != 0.0) & (a != 0.0)
        if ok.sum() == 0:
            return float("nan")
        return float(np.mean(np.sign(p[ok]) == np.sign(a[ok])))

    out["by_horizon"] = {}
    for h in sorted(df["horizon"].unique()):
        sub = df[df["horizon"] == h]
        out["by_horizon"][h] = {
            "n": int(sub["se"].notna().sum()),
            "rmse": rmse_from_se(sub["se"]),
            "fda": fda(sub["pred"], sub["actual"]),
        }

    if "moq" in df.columns:
        out["by_horizon_moq"] = {}
        for h in sorted(df["horizon"].unique()):
            out["by_horizon_moq"][h] = {}
            for moq in sorted(df["moq"].unique()):
                sub = df[(df["horizon"] == h) & (df["moq"] == moq)]
                out["by_horizon_moq"][h][int(moq)] = {
                    "n": int(sub["se"].notna().sum()),
                    "rmse": rmse_from_se(sub["se"]),
                    "fda": fda(sub["pred"], sub["actual"]),
                }

    if "covid" in df.columns:
        out["by_horizon_covid"] = {}
        for h in sorted(df["horizon"].unique()):
            out["by_horizon_covid"][h] = {}
            for flag in sorted(df["covid"].unique()):
                sub = df[(df["horizon"] == h) & (df["covid"] == flag)]
                out["by_horizon_covid"][h][str(flag)] = {
                    "n": int(sub["se"].notna().sum()),
                    "rmse": rmse_from_se(sub["se"]),
                    "fda": fda(sub["pred"], sub["actual"]),
                }

    return out


def _fit_accepts_kwargs(fit_fn: Callable) -> Tuple[bool, bool]:
    try:
        sig = inspect.signature(fit_fn)
        params = sig.parameters
        return ("init_params" in params), ("em_cache" in params)
    except Exception:
        return False, False


def _maybe_build_em_cache(model_config: Any, nM: int) -> Any:
    try:
        from dfm_pipeline.dfm_bm_ml.fast import build_em_cache
    except Exception:
        return None

    r_by_block = tuple(int(x) for x in getattr(model_config, "r_by_block"))
    return build_em_cache(
        nM=int(nM),
        nQ=1,
        r_by_block=r_by_block,
        blocks=getattr(model_config, "blocks", None),
        enforce_q_loading_constraint=bool(getattr(model_config, "enforce_quarterly_loading_constraint", True)),
    )


def run_pseudo_rt_eval(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    fit_fn: Callable,
    model_config: Any,
    eval_cfg: PseudoRTEvalConfig,
    *,
    warm_start: bool = False,
    use_em_cache: bool = False,
) -> Tuple[pd.DataFrame, dict]:
    """
    Expanding-window recursive pseudo-OOS evaluation.

    Quarter-end leakage fix:
      If horizon="now" and eval_date==target_date (quarter-end month),
      compute prediction using a_pred[t] updated only with monthly indicators at t,
      excluding quarterly observation.
    """
    if not isinstance(X_full.index, pd.DatetimeIndex):
        raise TypeError("X_full must be indexed by DatetimeIndex.")
    if not isinstance(y_full.index, pd.DatetimeIndex):
        raise TypeError("y_full must be indexed by DatetimeIndex.")

    X_full = X_full.sort_index()
    y_full = y_full.sort_index()

    eval_start = pd.to_datetime(eval_cfg.eval_start)
    eval_end = pd.to_datetime(eval_cfg.eval_end)

    eval_dates = X_full.loc[(X_full.index >= eval_start) & (X_full.index <= eval_end)].index
    if eval_dates.empty:
        raise ValueError("Evaluation window produced no dates on the panel monthly grid.")

    delays: Optional[pd.Series]
    if eval_cfg.delay_style == "trailing_nan":
        delays = trailing_nan_delays(X_full)
    elif eval_cfg.delay_style == "json_map":
        if eval_cfg.delay_json is None:
            raise ValueError("delay_style='json_map' requires delay_json path.")
        delays = pd.Series(pd.read_json(eval_cfg.delay_json, typ="series"))
        delays = delays.reindex(X_full.columns).fillna(0).astype(int)
    elif eval_cfg.delay_style == "none":
        delays = None
    else:
        raise ValueError(f"Unknown delay_style: {eval_cfg.delay_style!r}")

    nM = int(X_full.shape[1])

    accepts_init, accepts_cache = _fit_accepts_kwargs(fit_fn)
    em_cache = _maybe_build_em_cache(model_config, nM=nM) if (use_em_cache and accepts_cache) else None

    last_params = None
    rows: list[dict] = []

    for t in eval_dates:
        if delays is None:
            Xrt = X_full.loc[:t].copy()
        else:
            Xrt = apply_ragged_edge_mask(X_full, delays, t_end=t)

        yrt = mask_quarterly_release(y_full, t_end=t, gdp_rel=int(eval_cfg.gdp_rel))
        yrt = yrt.reindex(Xrt.index)

        Y_monthly = Xrt.to_numpy(dtype=float)
        y_quarterly = yrt.to_numpy(dtype=float)

        call_kwargs: Dict[str, Any] = dict(Y_monthly=Y_monthly, y_quarterly=y_quarterly, config=model_config)
        if accepts_cache and em_cache is not None:
            call_kwargs["em_cache"] = em_cache
        if accepts_init:
            call_kwargs["init_params"] = (last_params if warm_start else None)

        res = fit_fn(**call_kwargs)

        if warm_start and hasattr(res, "params_final") and getattr(res, "params_final") is not None:
            last_params = getattr(res, "params_final")

        Y_stack_raw = np.column_stack([Y_monthly, y_quarterly.reshape(-1, 1)]).astype(float)
        # Ensure the smoother sees observations on the same scale used to estimate the
        # state-space matrices (res.T/Q/C/R).
        if hasattr(res, "scaler") and getattr(res, "scaler") is not None:
            Y_stack = res.scaler.transform(Y_stack_raw)
        else:
            Y_stack = Y_stack_raw
        ss = StateSpaceParams(T=res.T, Q=res.Q, C=res.C, R=res.R, a0=res.a0, P0=res.P0)
        ks = kalman_filter_smoother(Y_stack, ss)

        Cq = res.C[nM, :].astype(float)

        a_t_filt = ks.a_filt[-1, :].astype(float)
        P_t_filt = ks.P_filt[-1, :, :].astype(float)

        t_moq = month_of_quarter(t)
        iQ = quarter_end_stamp(t)

        targets: dict[Horizon, pd.Timestamp] = {
            "bac": add_months(iQ, -3),
            "now": iQ,
            "for": add_months(iQ, 3),
        }

        for h in eval_cfg.horizons:
            target_date = targets[h]

            if target_date not in y_full.index:
                pred = np.nan
                actual = np.nan
            else:
                actual = float(y_full.loc[target_date])

                if target_date in Xrt.index:
                    idx_td = int(Xrt.index.get_loc(target_date))

                    if h == "now" and target_date == t:
                        a_pr = ks.a_pred[idx_td, :].astype(float)
                        P_pr = ks.P_pred[idx_td, :, :].astype(float)

                        x_row = Xrt.iloc[idx_td, :].to_numpy(dtype=float)
                        obs_idx = np.where(np.isfinite(x_row))[0]

                        if obs_idx.size == 0:
                            a_upd = a_pr
                        else:
                            C_m = res.C[:nM, :]
                            R_m = res.R[:nM, :nM]
                            C_sub = C_m[obs_idx, :]
                            R_sub = R_m[np.ix_(obs_idx, obs_idx)]

                            # Important: the state-space matrices (C/R) are defined on the
                            # scaled data used in estimation. So the monthly observations fed
                            # into this special update must be scaled consistently.
                            if hasattr(res, "scaler") and getattr(res, "scaler") is not None:
                                row_raw = np.concatenate([x_row, np.array([np.nan], dtype=float)], axis=0)[None, :]
                                row_scaled = res.scaler.transform(row_raw)[0, :nM]
                                y_obs = row_scaled[obs_idx]
                            else:
                                y_obs = x_row[obs_idx]
                            a_upd, _P_upd = _kalman_update_subset(a_pr, P_pr, y_obs, C_sub, R_sub)

                        pred = float(Cq @ a_upd)

                    else:
                        a_td = ks.a_smooth[idx_td, :].astype(float)
                        pred = float(Cq @ a_td)

                else:
                    steps = months_diff(t, target_date)
                    if steps < 0:
                        pred = np.nan
                    else:
                        a_f, _P_f = forecast_from_last(res.T, res.Q, a_t_filt, P_t_filt, steps=steps)
                        pred = float(Cq @ a_f)

            rows.append(
                {
                    "eval_date": t,
                    "moq": int(t_moq),
                    "horizon": h,
                    "target_date": target_date,
                    "pred": pred,
                    "actual": actual,
                }
            )

    pred_df = pd.DataFrame(rows)
    pred_df["eval_date"] = pd.to_datetime(pred_df["eval_date"])
    pred_df["target_date"] = pd.to_datetime(pred_df["target_date"])

    scores = compute_scores(pred_df)
    return pred_df, scores
