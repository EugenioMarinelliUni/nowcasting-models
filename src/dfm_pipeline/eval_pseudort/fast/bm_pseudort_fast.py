from __future__ import annotations

"""Fast pseudo real-time evaluation for BM-DFM.

This module provides a drop-in alternative to
:mod:`dfm_pipeline.eval_pseudort.bm_pseudort`.

It keeps the same evaluation logic and output schema, but reduces overhead by:

* Converting the full panel to numpy arrays once and slicing by integer position.
* Applying ragged-edge masks using vectorized numpy operations (grouped by delay).
* Avoiding repeated pandas `.loc[:t].copy()` inside the eval loop.

Additional optional speed-ups (logic-preserving):
* Warm-start EM using previous month parameters (sequential mode).
* Reuse EM cache across evaluation months.

The original evaluator is kept unchanged for comparison.
"""

from typing import Optional, Literal, Tuple, Callable, Dict, Any

import os
import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

from dfm_pipeline.dfm_bm_ml.fast.em_fast import build_em_cache


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


def compute_scores(pred_df: pd.DataFrame) -> dict:
    # Reuse the original implementation by importing lazily to avoid duplication.
    from ..bm_pseudort import compute_scores as _compute
    return _compute(pred_df)


def _mask_ragged_edge_numpy(Xrt: np.ndarray, delay_vec: np.ndarray) -> None:
    """In-place ragged-edge mask on the last d rows per column."""
    Tn = Xrt.shape[0]
    if Tn == 0:
        return

    dvals = np.unique(delay_vec)
    for d in dvals:
        d = int(d)
        if d <= 0:
            continue
        if Tn < d:
            continue
        cols = np.where(delay_vec == d)[0]
        if cols.size == 0:
            continue
        Xrt[-d:, cols] = np.nan


def _set_blas_threads(n: int) -> None:
    """Prevent BLAS oversubscription when using joblib parallel workers."""
    n = int(n)
    os.environ["OMP_NUM_THREADS"] = str(n)
    os.environ["MKL_NUM_THREADS"] = str(n)
    os.environ["OPENBLAS_NUM_THREADS"] = str(n)
    os.environ["VECLIB_MAXIMUM_THREADS"] = str(n)
    os.environ["NUMEXPR_NUM_THREADS"] = str(n)


def run_pseudo_rt_eval_fast(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    fit_fn: Callable[..., Any],
    model_config: Any,
    eval_cfg: Any,
    *,
    warm_start: bool = False,     # NEW
    n_jobs: int = 1,              # NEW (optional parallel mode)
    blas_threads: int = 1,        # NEW
) -> Tuple[pd.DataFrame, dict]:
    """Fast pseudo real-time evaluation.

    Logic matches :func:`dfm_pipeline.eval_pseudort.bm_pseudort.run_pseudo_rt_eval`
    but with reduced overhead and optional warm-start.
    """
    if warm_start and int(n_jobs) > 1:
        raise ValueError("warm_start=True is incompatible with n_jobs>1 (sequential dependency).")

    if not isinstance(X_full.index, pd.DatetimeIndex):
        raise TypeError("X_full must be indexed by DatetimeIndex.")
    if not isinstance(y_full.index, pd.DatetimeIndex):
        raise TypeError("y_full must be indexed by DatetimeIndex.")

    X_full = X_full.sort_index()
    y_full = y_full.sort_index()

    eval_start = pd.to_datetime(eval_cfg.eval_start)
    eval_end = pd.to_datetime(eval_cfg.eval_end)

    idx_full = X_full.index
    in_window = (idx_full >= eval_start) & (idx_full <= eval_end)
    eval_dates = idx_full[in_window]
    if eval_dates.empty:
        raise ValueError("Evaluation window produced no dates on the panel monthly grid.")

    # Resolve delays
    if eval_cfg.delay_style == "trailing_nan":
        delays = trailing_nan_delays(X_full)
        delay_vec = delays.reindex(X_full.columns).fillna(0).astype(int).to_numpy(dtype=int)
    elif eval_cfg.delay_style == "json_map":
        if getattr(eval_cfg, "delay_json", None) is None:
            raise ValueError("delay_style='json_map' requires delay_json path.")
        delays = pd.Series(pd.read_json(eval_cfg.delay_json, typ="series"))
        delay_vec = delays.reindex(X_full.columns).fillna(0).astype(int).to_numpy(dtype=int)
    elif eval_cfg.delay_style == "none":
        delay_vec = np.zeros((X_full.shape[1],), dtype=int)
    else:
        raise ValueError(f"Unknown delay_style: {eval_cfg.delay_style!r}")

    # Convert once to numpy
    X_arr = X_full.to_numpy(dtype=float, copy=False)

    # Align the quarterly series to the monthly index once
    y_aligned = y_full.reindex(idx_full)
    y_arr = y_aligned.to_numpy(dtype=float, copy=False)

    nM = int(X_arr.shape[1])

    # Fast lookup: timestamp -> integer position in full monthly grid
    pos_map: Dict[pd.Timestamp, int] = {ts: i for i, ts in enumerate(idx_full)}

    # Build EM cache once and reuse across all eval months
    r_by_block = tuple(int(x) for x in model_config.r_by_block)
    em_cache = build_em_cache(
        nM=nM,
        nQ=1,
        r_by_block=r_by_block,
        blocks=getattr(model_config, "blocks", None),
        enforce_q_loading_constraint=bool(getattr(model_config, "enforce_quarterly_loading_constraint", True)),
    )

    def _one_eval(t: pd.Timestamp, init_params=None) -> list[dict]:
        end_idx = pos_map[pd.Timestamp(t)]
        Xrt = X_arr[: end_idx + 1, :].copy()

        if eval_cfg.delay_style != "none":
            _mask_ragged_edge_numpy(Xrt, delay_vec)

        yrt = y_arr[: end_idx + 1].copy()
        d = int(getattr(eval_cfg, "gdp_rel", 0))
        if d > 0 and yrt.size >= d:
            yrt[-d:] = np.nan

        res = fit_fn(
            Y_monthly=Xrt,
            y_quarterly=yrt,
            config=model_config,
            init_params=init_params,
            em_cache=em_cache,
        )

        # State-space for running smoother on the pseudo-vintage
        Y_stack = np.column_stack([Xrt, yrt.reshape(-1, 1)]).astype(float)
        ss = StateSpaceParams(T=res.T, Q=res.Q, C=res.C, R=res.R, a0=res.a0, P0=res.P0)
        ks = kalman_filter_smoother(Y_stack, ss)

        Cq = res.C[nM, :].astype(float)

        a_t_filt = ks.a_filt[-1, :].astype(float)
        P_t_filt = ks.P_filt[-1, :, :].astype(float)

        t_moq = month_of_quarter(pd.Timestamp(t))
        iQ = quarter_end_stamp(pd.Timestamp(t))

        targets: dict[Horizon, pd.Timestamp] = {
            "bac": add_months(iQ, -3),
            "now": iQ,
            "for": add_months(iQ, 3),
        }

        out_rows: list[dict] = []

        for h in eval_cfg.horizons:
            target_date = targets[h]

            if target_date not in y_full.index:
                pred = np.nan
                actual = np.nan
            else:
                actual = float(y_full.loc[target_date])

                pos_td = pos_map.get(pd.Timestamp(target_date), None)

                if pos_td is not None and pos_td <= end_idx:
                    idx_td = int(pos_td)

                    # Quarter-end leakage fix (same logic as original fast evaluator)
                    if h == "now" and pd.Timestamp(target_date) == pd.Timestamp(t):
                        a_pr = ks.a_pred[idx_td, :].astype(float)
                        P_pr = ks.P_pred[idx_td, :, :].astype(float)

                        x_row = Xrt[idx_td, :]
                        obs_idx = np.where(np.isfinite(x_row))[0]

                        if obs_idx.size == 0:
                            a_upd = a_pr
                        else:
                            C_m = res.C[:nM, :]
                            R_m = res.R[:nM, :nM]
                            C_sub = C_m[obs_idx, :]
                            R_sub = R_m[np.ix_(obs_idx, obs_idx)]
                            y_obs = x_row[obs_idx]
                            a_upd, _P_upd = _kalman_update_subset(a_pr, P_pr, y_obs, C_sub, R_sub)

                        pred = float(Cq @ a_upd)
                    else:
                        a_td = ks.a_smooth[idx_td, :].astype(float)
                        pred = float(Cq @ a_td)

                else:
                    steps = months_diff(pd.Timestamp(t), pd.Timestamp(target_date))
                    if steps < 0:
                        pred = np.nan
                    else:
                        a_f, _P_f = forecast_from_last(res.T, res.Q, a_t_filt, P_t_filt, steps=steps)
                        pred = float(Cq @ a_f)

            out_rows.append(
                {
                    "eval_date": pd.Timestamp(t),
                    "moq": int(t_moq),
                    "horizon": h,
                    "target_date": pd.Timestamp(target_date),
                    "pred": pred,
                    "actual": actual,
                }
            )

        # Return both output rows and the last fitted params (for warm-start chaining)
        return out_rows, getattr(res, "params_final", None)

    rows: list[dict] = []

    # Sequential mode (supports warm_start)
    if int(n_jobs) <= 1:
        last_params = None
        for t in eval_dates:
            out_rows, params_out = _one_eval(t, init_params=(last_params if warm_start else None))
            rows.extend(out_rows)
            if warm_start and params_out is not None:
                last_params = params_out

    # Parallel mode (no warm-start)
    else:
        try:
            from joblib import Parallel, delayed
        except ImportError as e:
            raise ImportError("joblib is required for n_jobs > 1 parallel evaluation.") from e

        def _worker(t):
            _set_blas_threads(blas_threads)
            out_rows, _ = _one_eval(t, init_params=None)
            return out_rows

        chunks = Parallel(n_jobs=int(n_jobs), backend="loky")(
            delayed(_worker)(t) for t in eval_dates
        )
        for ch in chunks:
            rows.extend(ch)

    pred_df = pd.DataFrame(rows)
    pred_df["eval_date"] = pd.to_datetime(pred_df["eval_date"])
    pred_df["target_date"] = pd.to_datetime(pred_df["target_date"])

    scores = compute_scores(pred_df)
    return pred_df, scores
