# src/dfm_pipeline/dfm_dyn/backtest.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Tuple, Dict, Any, Optional

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_simple.backtest import (
    load_X_y_simple,
    build_quarterly_target_from_monthly,
    quarter_id_from_timestamp,
    apply_ragged_mask_to_X,
    load_mask_simple,
)
from dfm_pipeline.dfm_dyn.em_dfm import em_dfm_full

# Optional progress bars
try:
    from tqdm.auto import tqdm  # type: ignore[import]
except ImportError:  # pragma: no cover
    tqdm = None


@dataclass(frozen=True)
class DFMBacktestConfigDyn:
    """
    Config for dynamic-factor DFM recursive pseudo-real-time backtest.

    Differences vs DFMBacktestConfigSimple:
      - Uses EM-based state-space estimation (dynamic DFM).
      - Grid is over (q, r, p), but currently only q is used inside em_dfm_full.
    """

    panel_id: str
    X_path: Path
    y_path: Path
    train_start: str
    eval_start: str
    eval_end: str
    monthly_freq: str = "MS"
    grid: Sequence[Tuple[int, int, int]] = ()
    mask_path: Path | None = None
    ragged_mode: str = "none"  # "none" or "mask"


def build_eval_vintages_dyn(
    cfg: DFMBacktestConfigDyn,
    idx: pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    """
    Build the sequence of evaluation months [eval_start, eval_end] that
    are present in the X index, with monthly frequency "MS".
    """
    all_months = pd.date_range(
        start=pd.to_datetime(cfg.train_start),
        end=pd.to_datetime(cfg.eval_end),
        freq="MS",
    )
    eval_mask = (
        (all_months >= pd.to_datetime(cfg.eval_start))
        & (all_months <= pd.to_datetime(cfg.eval_end))
    )
    eval_months = all_months[eval_mask]
    eval_months = eval_months[eval_months.isin(idx)]
    return eval_months


def estimate_dfm_dyn_and_nowcast_single(
    X: pd.DataFrame,
    y: pd.Series,
    cfg: DFMBacktestConfigDyn,
    mask: Optional[pd.DataFrame],
    T: pd.Timestamp,
    q: int,
    r: int,
    p: int,
    n_em_iter: int,
    tol: float,
    min_obs: int = 60,
) -> float:
    """
    For a given vintage T and (q,r,p), estimate dynamic DFM on [train_start, T]
    and produce a nowcast of the current quarter y_Q(T).

    Currently:
      - em_dfm_full is called with q factors; r and p are not used inside EM.
      - GDP equation: OLS of quarterly y_Q on smoothed factors f_t (post-EM).
    """
    train_start_ts = pd.to_datetime(cfg.train_start)

    # Build X_T with optional ragged mask
    if cfg.ragged_mode == "mask" and mask is not None:
        X_T = apply_ragged_mask_to_X(X, mask, T, train_start_ts)
    else:
        X_T = X.loc[train_start_ts:T]

    y_T = y.loc[train_start_ts:T]

    if X_T.shape[0] < min_obs:
        return np.nan

    # Quarterly reference for the current quarter
    y_q_full = build_quarterly_target_from_monthly(y_T)
    q_id = quarter_id_from_timestamp(T)
    if q_id not in y_q_full.index:
        return np.nan
    y_real_Q = float(y_q_full.loc[q_id])
    if np.isnan(y_real_Q):
        return np.nan

    # Fit dynamic DFM via full EM on X_T
    # Type as Any so static checker doesn't complain about attributes like f_smooth.
    params: Any = em_dfm_full(
        X=X_T,
        q=q,
        n_iter=n_em_iter,
        tol=tol,
        verbose=False,
    )

    # Smoothed factors from EM (T_eff x q)
    f_sm = getattr(params, "f_smooth", None)
    if f_sm is None:
        return np.nan

    f_sm = np.asarray(f_sm, dtype=float)
    if f_sm.shape[0] != X_T.shape[0]:
        return np.nan

    dates_T = list(X_T.index)

    # Build regression sample: map months to their quarter's y_Q
    y_q_T = build_quarterly_target_from_monthly(y_T)

    y_reg: list[float] = []
    F_reg: list[np.ndarray] = []

    for idx_t, date in enumerate(dates_T):
        qid = quarter_id_from_timestamp(date)
        if qid not in y_q_T.index:
            continue
        y_val = float(y_q_T.loc[qid])
        if np.isnan(y_val):
            continue
        y_reg.append(y_val)
        F_reg.append(f_sm[idx_t, :])

    if len(y_reg) < q + 2:
        return np.nan

    y_reg_arr = np.asarray(y_reg, dtype=float)
    F_reg_arr = np.asarray(F_reg, dtype=float)

    # OLS: y = beta0 + beta' f_t
    X_reg = np.column_stack([np.ones(len(y_reg_arr)), F_reg_arr])  # (n, 1+q)
    beta, *_ = np.linalg.lstsq(X_reg, y_reg_arr, rcond=None)

    # Factor for current vintage T
    try:
        idx_T = dates_T.index(T)
    except ValueError:
        return np.nan

    f_T = f_sm[idx_T, :]
    y_hat_T = beta[0] + f_T @ beta[1:]

    return float(y_hat_T)


def run_backtest_single_spec_dyn(
    cfg: DFMBacktestConfigDyn,
    X: pd.DataFrame,
    y: pd.Series,
    y_q: pd.Series,
    mask: Optional[pd.DataFrame],
    q: int,
    r: int,
    p: int,
    n_em_iter: int,
    tol: float,
    min_obs: int = 60,
) -> Dict[str, Any]:
    """
    Run dynamic DFM backtest for one (q,r,p) over the evaluation window.

    Returns a single summary row with RMSE and n_eval.
    """
    X_index = pd.DatetimeIndex(X.index)
    eval_months = build_eval_vintages_dyn(cfg, X_index)

    forecasts: list[float] = []
    reals: list[float] = []
    months: list[pd.Timestamp] = []

    # Inner progress bar over evaluation months
    if tqdm is not None:
        iterator = tqdm(
            eval_months,
            desc=f"dyn DFM q={q}, r={r}, p={p}",
            unit="month",
            leave=False,
        )
    else:
        iterator = eval_months

    for T in iterator:
        y_hat_T = estimate_dfm_dyn_and_nowcast_single(
            X=X,
            y=y,
            cfg=cfg,
            mask=mask,
            T=T,
            q=q,
            r=r,
            p=p,
            n_em_iter=n_em_iter,
            tol=tol,
            min_obs=min_obs,
        )
        if np.isnan(y_hat_T):
            continue

        q_id = quarter_id_from_timestamp(T)
        if q_id not in y_q.index:
            continue
        y_real_Q = float(y_q.loc[q_id])
        if np.isnan(y_real_Q):
            continue

        forecasts.append(float(y_hat_T))
        reals.append(y_real_Q)
        months.append(T)

    if not forecasts:
        return {
            "panel_id": cfg.panel_id,
            "X_path": str(cfg.X_path),
            "y_path": str(cfg.y_path),
            "train_start": cfg.train_start,
            "eval_start": cfg.eval_start,
            "eval_end": cfg.eval_end,
            "ragged_mode": cfg.ragged_mode,
            "mask_path": str(cfg.mask_path) if cfg.mask_path is not None else "",
            "q": q,
            "r": r,
            "p": p,
            "rmse_nowcast": np.nan,
            "n_eval": 0,
        }

    f = np.asarray(forecasts, dtype=float)
    rvals = np.asarray(reals, dtype=float)
    err = f - rvals
    rmse_global = float(np.sqrt(np.mean(err**2)))

    return {
        "panel_id": cfg.panel_id,
        "X_path": str(cfg.X_path),
        "y_path": str(cfg.y_path),
        "train_start": cfg.train_start,
        "eval_start": cfg.eval_start,
        "eval_end": cfg.eval_end,
        "ragged_mode": cfg.ragged_mode,
        "mask_path": str(cfg.mask_path) if cfg.mask_path is not None else "",
        "q": q,
        "r": r,
        "p": p,
        "rmse_nowcast": rmse_global,
        "n_eval": len(forecasts),
    }


def run_dfm_grid_dyn(
    cfg: DFMBacktestConfigDyn,
    n_em_iter: int = 30,
    tol: float = 1e-4,
    min_obs: int = 60,
) -> pd.DataFrame:
    """
    Load X,y, optionally mask, and run dynamic DFM backtest over cfg.grid.

    Returns a DataFrame with one row per (q,r,p).

    Progress:
      - Outer tqdm over specs (q,r,p).
      - Inner tqdm (in run_backtest_single_spec_dyn) over evaluation months.
    """
    X, y = load_X_y_simple(cfg.X_path, cfg.y_path, monthly_freq=cfg.monthly_freq)
    y_q = build_quarterly_target_from_monthly(y)

    mask: Optional[pd.DataFrame] = None
    if cfg.ragged_mode == "mask" and cfg.mask_path is not None:
        mask = load_mask_simple(cfg.mask_path, monthly_freq=cfg.monthly_freq)

    records: list[Dict[str, Any]] = []
    grid_list = list(cfg.grid)

    # Outer progress bar over specs
    if tqdm is not None:
        iterator = tqdm(
            grid_list,
            desc=f"DFM dyn grid {cfg.panel_id}",
            total=len(grid_list),
            unit="spec",
        )
    else:
        iterator = grid_list

    for (q, r, p) in iterator:
        # Optional constraint: dynamic dimension r <= q
        if r > q:
            continue

        rec = run_backtest_single_spec_dyn(
            cfg=cfg,
            X=X,
            y=y,
            y_q=y_q,
            mask=mask,
            q=q,
            r=r,
            p=p,
            n_em_iter=n_em_iter,
            tol=tol,
            min_obs=min_obs,
        )
        records.append(rec)

    return pd.DataFrame.from_records(records)
