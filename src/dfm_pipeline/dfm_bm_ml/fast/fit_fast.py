from __future__ import annotations

import warnings
from typing import Optional

import numpy as np
from tqdm import tqdm

from .em_fast import EMStepCache, build_em_cache, em_step_ml_fast
from .init import init_params_pca
from ..scaling import PanelScaler, scale_panel
from ..spec import BMDfmConfig
from ..state_builder import BMParams, build_state_space

try:
    # Newer layout: result types live in types.py
    from ..types import BMDfmResult  # type: ignore
except Exception:  # pragma: no cover
    # Backward compatible: BMDfmResult lives in spec.py
    from ..spec import BMDfmResult  # type: ignore


def fit_bm_dfm_fast(
    Y_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    config: BMDfmConfig,
    *,
    init_params: Optional[BMParams] = None,
    em_cache: Optional[EMStepCache] = None,
) -> BMDfmResult:
    """
    Fast (non-numba) BM-DFM ML-EM fit.

    Inputs:
      Y_monthly: (T, nM) monthly panel (already transformed; may be raw or standardized depending on scaling_mode)
      y_quarterly: (T,) quarterly target placed on quarter-end months (NaN elsewhere)

    scaling_mode:
      - external_frozen: inputs are already standardized; do not rescale
      - internal_per_run / toolbox_vintage: compute nanmean/nanstd once per fit call and standardize internally
    """
    config.validate()

    Y_monthly = np.asarray(Y_monthly, dtype=float)
    y_quarterly = np.asarray(y_quarterly, dtype=float).reshape(-1)
    if Y_monthly.ndim != 2:
        raise ValueError("Y_monthly must be 2D (T, nM).")
    if y_quarterly.ndim != 1:
        raise ValueError("y_quarterly must be 1D (T,).")
    if Y_monthly.shape[0] != y_quarterly.shape[0]:
        raise ValueError("Y_monthly and y_quarterly must share the same T index.")

    Tn, nM = Y_monthly.shape
    nQ = 1  # single quarterly target

    # Stack into a single observation matrix (T, nM+nQ)
    Y_raw = np.concatenate([Y_monthly, y_quarterly[:, None]], axis=1)

    # Accept toolbox_vintage as an alias for internal_per_run
    scale_mode = str(config.scaling_mode)
    if scale_mode == "toolbox_vintage":
        scale_mode = "internal_per_run"

    Y_scaled, scaler = scale_panel(Y_raw, mode=scale_mode, min_std=config.min_std)
    if not isinstance(scaler, PanelScaler):
        raise TypeError("scale_panel must return a PanelScaler.")

    # Initialization
    params = init_params
    if params is None:
        params = init_params_pca(Y_scaled, nM=nM, config=config)

    # Build (or reuse) EM cache
    if em_cache is None:
        em_cache = build_em_cache(
            nM=nM,
            nQ=nQ,
            r_by_block=tuple(int(x) for x in config.r_by_block),
            p=int(config.p),
            idio_ar1=bool(config.idio_ar1),
            enforce_quarterly_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
            fix_quarterly_R=bool(config.fix_quarterly_R),
            mm_weight_style=str(config.mm_weight_style),
        )

    # Initial state (diffuse)
    a0_in = np.zeros(em_cache.n_state, dtype=float)
    P0_in = 1e4 * np.eye(em_cache.n_state, dtype=float)

    loglik_trace: list[float] = []
    converged = False

    # Main EM loop
    for it in tqdm(range(1, int(config.max_iter) + 1), desc="BM-DFM EM (fast)"):
        try:
            (
                params,
                loglik,
                a_smooth,
                P_smooth,
                P_lag_smooth,
                a0_next,
                P0_next,
            ) = em_step_ml_fast(
                Y_m=Y_scaled[:, :nM],
                y_q=Y_scaled[:, nM],
                r_by_block=tuple(int(x) for x in config.r_by_block),
                p=int(config.p),
                idio_ar1=bool(config.idio_ar1),
                enforce_quarterly_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
                fix_quarterly_R=bool(config.fix_quarterly_R),
                mm_weight_style=str(config.mm_weight_style),
                rho_idio_floor=float(config.rho_idio_floor),
                rho_idio_ceiling=float(config.rho_idio_ceiling),
                min_var=float(config.min_var),
                jitter=float(config.jitter),
                monthly_meas_var_floor=float(config.monthly_meas_var_floor),
                quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
                params=params,
                cache=em_cache,
                a0_in=a0_in,
                P0_in=P0_in,
            )
        except np.linalg.LinAlgError as e:
            warnings.warn(f"LinAlgError at iter {it}: {e}; stopping early.", RuntimeWarning)
            break

        loglik_trace.append(float(loglik))

        # Optionally update initial state for the next E-step
        if bool(config.update_initial_state_each_iter):
            a0_in = a0_next
            P0_in = P0_next

        # Convergence check
        if len(loglik_trace) >= 2:
            dLL = loglik_trace[-1] - loglik_trace[-2]
            if str(config.convergence_mode) == "abs":
                crit = abs(dLL)
                postfix = {"loglik": loglik_trace[-1], "dLL": dLL}
            else:
                rel = abs(dLL) / (abs(loglik_trace[-1]) + 1e-12)
                crit = rel
                postfix = {"loglik": loglik_trace[-1], "dLL": dLL, "rel": rel}

            tqdm.write("") if False else None
            tqdm.set_lock(None)
            # tqdm postfix
            try:
                tqdm_obj = tqdm._instances.pop()  # type: ignore[attr-defined]
                tqdm._instances.add(tqdm_obj)  # type: ignore[attr-defined]
                tqdm_obj.set_postfix(postfix)
            except Exception:
                pass

            if it >= 2 and crit < float(config.tol):
                converged = True
                break
        else:
            # First iteration: show loglik only
            try:
                tqdm_obj = tqdm._instances.pop()  # type: ignore[attr-defined]
                tqdm._instances.add(tqdm_obj)  # type: ignore[attr-defined]
                tqdm_obj.set_postfix({"loglik": loglik_trace[-1]})
            except Exception:
                pass

    # Ensure final state-space matrices are consistent with last params
    _T, _Q, _C, _R, _, _, _idx = build_state_space(params, config, a0=np.zeros(em_cache.n_state), P0=P0_in)

    return BMDfmResult(
        params=params,
        loglik=float(loglik_trace[-1]) if loglik_trace else float("nan"),
        converged=converged,
        n_iter=len(loglik_trace),
        loglik_trace=loglik_trace,
        a_smooth=a_smooth if loglik_trace else None,
        P_smooth=P_smooth if loglik_trace else None,
        P_lag_smooth=P_lag_smooth if loglik_trace else None,
        scaler=scaler,
    )
