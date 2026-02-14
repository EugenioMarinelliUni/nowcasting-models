from __future__ import annotations

from dataclasses import replace

import numpy as np
from tqdm.auto import tqdm

from dfm_pipeline.dfm_bm_ml.fast.em_step_cache import EMStepCache
from dfm_pipeline.dfm_bm_ml.fast.init import init_params_pca
from dfm_pipeline.dfm_bm_ml.fast.m_step_numba import m_step_numba
from dfm_pipeline.dfm_bm_ml.fast.state_space import build_state_space
from dfm_pipeline.dfm_bm_ml.scaling import scale_panel
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.types import BMDfmResult, BMParams


def _convergence_stat(
    config: BMDfmConfig, ll_new: float, ll_old: float, ll0: float | None
) -> tuple[float, float, float]:
    dLL = float(ll_new - ll_old)
    rel = float(abs(dLL) / (abs(ll_old) + 1.0))
    if config.convergence_mode == "absolute_ll":
        crit = abs(dLL)
    elif config.convergence_mode == "toolbox_rel":
        crit = rel
    else:
        raise ValueError(f"Unknown convergence_mode={config.convergence_mode!r}")
    return dLL, rel, crit


def fit_bm_dfm_fast_numba(
    X_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    config: BMDfmConfig,
    init_params: BMParams | None = None,
    em_cache: EMStepCache | None = None,
) -> BMDfmResult:
    config.validate()

    if int(config.n_quarterly) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    T = X_monthly.shape[0]
    nM = X_monthly.shape[1]
    nQ = int(config.n_quarterly)

    Y_raw = np.column_stack([X_monthly, y_quarterly.reshape(T, nQ)])

    Y, scaler = scale_panel(Y_raw, mode=str(config.scaling_mode), min_std=float(config.min_var))

    Xs = Y[:, :nM]
    yq = Y[:, nM : nM + nQ]

    if init_params is None:
        params = init_params_pca(Xs, yq, config=config)
    else:
        params = init_params

    ss = build_state_space(params=params, config=config)

    loglik_trace: list[float] = []

    pbar = tqdm(range(int(config.max_iter)), desc="BM-DFM EM (fast+numba)", leave=False)
    ll0: float | None = None
    converged = False
    for it in pbar:
        new_params, ll, em_cache = m_step_numba(
            Y=Y,
            params=params,
            ss=ss,
            config=config,
            em_cache=em_cache,
        )
        loglik_trace.append(float(ll))
        if ll0 is None:
            ll0 = float(ll)

        params = new_params
        ss = build_state_space(params=params, config=config)

        if len(loglik_trace) == 1:
            pbar.set_postfix_str(f"loglik={ll:.2f}")
            continue

        dLL, rel, crit = _convergence_stat(config, loglik_trace[-1], loglik_trace[-2], ll0)
        pbar.set_postfix_str(f"loglik={ll:.2f}, dLL={dLL:.3e}, rel={rel:.3e}")

        if it >= 2 and crit < float(config.tol):
            converged = True
            break

    ss = build_state_space(params=params, config=config)

    return BMDfmResult(
        params=params,
        state_space=ss,
        loglik_trace=loglik_trace,
        scaler=scaler,
        em_cache=em_cache,
        converged=bool(converged),
        config=replace(config),
    )