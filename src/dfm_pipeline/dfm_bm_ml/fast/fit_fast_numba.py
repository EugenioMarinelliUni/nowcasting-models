from __future__ import annotations

from typing import Optional

import numpy as np
from tqdm.auto import tqdm

from .em_fast_numba import EMStepCache, build_em_cache, em_step_ml_fast_numba
from .init import init_params_pca
from ..scaling import scale_panel
from ..spec import BMDfmConfig
from ..state_builder import BMParams, build_state_space
from ..types import BMDfmResult


def _as_blocks_array(blocks: Optional[list[int]], nM: int) -> Optional[np.ndarray]:
    if blocks is None:
        return None
    arr = np.asarray(blocks, dtype=int)
    if arr.ndim != 1 or arr.shape[0] != nM:
        raise ValueError("config.blocks must be a 1D list/array of length n_monthly.")
    return arr


def _converged(loglik_trace: list[float], tol: float, mode: str) -> bool:
    """
    Additional stop criterion (in addition to max_iter):
      - absolute_ll: |ll_k - ll_{k-1}| < tol
      - toolbox_rel: |ll_k - ll_{k-1}| / max(1, |ll_{k-1}|) < tol
    """
    if len(loglik_trace) < 2:
        return False

    ll_new = float(loglik_trace[-1])
    ll_old = float(loglik_trace[-2])
    if (not np.isfinite(ll_new)) or (not np.isfinite(ll_old)):
        return False

    d = ll_new - ll_old

    if mode == "absolute_ll":
        return abs(d) < tol

    # toolbox_rel (default)
    denom = max(1.0, abs(ll_old))
    return abs(d) / denom < tol


def fit_bm_dfm_fast_numba(
    Y_monthly: Optional[np.ndarray] = None,
    y_quarterly: Optional[np.ndarray] = None,
    config: Optional[BMDfmConfig] = None,
    *,
    X_monthly: Optional[np.ndarray] = None,  # alias
    init_params: Optional[BMParams] = None,
    em_cache: Optional[EMStepCache] = None,
    verbose: bool = False,
) -> BMDfmResult:
    if config is None:
        raise ValueError("config is required.")
    config.validate()

    if Y_monthly is None:
        Y_monthly = X_monthly
    if Y_monthly is None:
        raise ValueError("Y_monthly (or X_monthly) is required.")
    if y_quarterly is None:
        raise ValueError("y_quarterly is required.")

    Y_monthly = np.asarray(Y_monthly, dtype=float)
    y_quarterly = np.asarray(y_quarterly, dtype=float).reshape(-1)

    if Y_monthly.ndim != 2:
        raise ValueError("Y_monthly must be 2D (T, nM).")
    if y_quarterly.ndim != 1:
        raise ValueError("y_quarterly must be 1D (T,).")
    if Y_monthly.shape[0] != y_quarterly.shape[0]:
        raise ValueError("Y_monthly and y_quarterly must share the same T index.")

    Tn, nM = Y_monthly.shape
    nQ = int(config.n_quarterly)
    if nQ != 1:
        raise ValueError("This BM-DFM variant expects a single quarterly target (n_quarterly=1).")

    Y_raw = np.concatenate([Y_monthly, y_quarterly[:, None]], axis=1)

    scale_mode = str(config.scaling_mode)
    if scale_mode == "toolbox_vintage":
        scale_mode = "internal_per_run"
    Y, scaler = scale_panel(Y_raw, mode=scale_mode)

    blocks_arr = _as_blocks_array(config.blocks, nM)

    if em_cache is None:
        em_cache = build_em_cache(
            nM=nM,
            nQ=nQ,
            r_by_block=tuple(int(x) for x in config.r_by_block),
            blocks=blocks_arr,
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        )

    params = init_params
    if params is None:
        params = init_params_pca(Y, nM=nM, config=config)

    P0_mode = getattr(config, "P0_mode", "diffuse")
    update_initial_state = bool(getattr(config, "update_initial_state_each_iter", False))

    a0_in = None
    P0_in = None

    loglik_trace: list[float] = []
    converged = False

    it_iter = tqdm(
        range(int(config.max_iter)),
        desc="BM-DFM EM (numba)",
        unit="iter",
        dynamic_ncols=True,
        disable=not bool(verbose),
    )

    for _it in it_iter:
        (
            params,
            loglik,
            a_smooth,
            P_smooth,
            P_lag_smooth,
            a0_next,
            P0_next,
        ) = em_step_ml_fast_numba(
            Y=Y,
            params=params,
            nM=nM,
            nQ=nQ,
            r_by_block=tuple(int(x) for x in config.r_by_block),
            p=int(config.p),
            ppC=5,
            mm_style=str(config.mm_weight_style),
            quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
            monthly_meas_var_floor=float(config.monthly_meas_var_floor),
            idio_ar1=bool(config.idio_ar1),
            force_var_stability=bool(config.force_var_stability),
            var_stability_shrink=float(config.var_stability_shrink),
            P0_mode=str(P0_mode),
            a0_in=a0_in,
            P0_in=P0_in,
            update_initial_state=update_initial_state,
            min_var=float(config.min_var),
            jitter=float(config.jitter),
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
            fix_quarterly_R=bool(config.fix_quarterly_R),
            blocks=blocks_arr,
            cache=em_cache,
        )

        loglik_trace.append(float(loglik))

        if verbose:
            it_iter.set_postfix(ll=float(loglik), refresh=False)

        if update_initial_state:
            a0_in = a0_next
            P0_in = P0_next

        # Additional stop criterion: relative/absolute LL improvement
        if _converged(loglik_trace, tol=float(config.tol), mode=str(config.convergence_mode)):
            converged = True
            break

    C, R, A, Q, a0, P0, state_index = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=tuple(int(x) for x in config.r_by_block),
        p=int(config.p),
        ppC=5,
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
        P0_mode=str(P0_mode),
        a0_override=a0_in,
        P0_override=P0_in,
    )

    return BMDfmResult(
        params=params,
        loglik_trace=loglik_trace,
        a_smooth=a_smooth,
        P_smooth=P_smooth,
        P_lag_smooth=P_lag_smooth,
        C=C,
        R=R,
        A=A,
        Q=Q,
        a0=a0,
        P0=P0,
        state_index=state_index,
        scaler=scaler,
        config=config,
        converged=converged,
        em_cache=em_cache,
    )