from __future__ import annotations

from dataclasses import replace

import numpy as np
from tqdm.auto import tqdm

from dfm_pipeline.dfm_bm_ml.fast.em_step_cache import EMStepCache
from dfm_pipeline.dfm_bm_ml.fast.init import init_params_pca
from dfm_pipeline.dfm_bm_ml.fast.m_step import m_step
from dfm_pipeline.dfm_bm_ml.fast.state_space import build_state_space
from dfm_pipeline.dfm_bm_ml.scaling import scale_panel
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.types import BMDfmResult, BMParams


def _convergence_stat(
    config: BMDfmConfig, ll_new: float, ll_old: float, ll0: float | None
) -> tuple[float, float, float]:
    """
    Returns (dLL, rel, crit) where crit is the stopping statistic used.
    - dLL = ll_new - ll_old
    - rel = abs(dLL) / (abs(ll_old) + 1) (toolbox-style normalization)
    - crit = abs(dLL) or rel depending on convergence_mode
    """
    dLL = float(ll_new - ll_old)
    rel = float(abs(dLL) / (abs(ll_old) + 1.0))
    if config.convergence_mode == "absolute_ll":
        crit = abs(dLL)
    elif config.convergence_mode == "toolbox_rel":
        crit = rel
    else:
        raise ValueError(f"Unknown convergence_mode={config.convergence_mode!r}")
    return dLL, rel, crit


def fit_bm_dfm_fast(
    X_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    config: BMDfmConfig,
    init_params: BMParams | None = None,
    em_cache: EMStepCache | None = None,
) -> BMDfmResult:
    # Validate invariants early so mistakes fail loudly.
    config.validate()

    if int(config.n_quarterly) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    T = X_monthly.shape[0]
    nM = X_monthly.shape[1]
    nQ = int(config.n_quarterly)

    # Stack observation matrix as [monthly panel..., quarterly target]
    Y_raw = np.column_stack([X_monthly, y_quarterly.reshape(T, nQ)])

    # Standardize per chosen mode
    Y, scaler = scale_panel(Y_raw, mode=str(config.scaling_mode), min_std=float(config.min_var))

    # Split standardized Y back out
    Xs = Y[:, :nM]
    yq = Y[:, nM : nM + nQ]

    # PCA init / initial parameters
    if init_params is None:
        params = init_params_pca(Xs, yq, config=config)
    else:
        params = init_params

    # Build state space (depends on parameters)
    ss = build_state_space(params=params, config=config)

    loglik_trace: list[float] = []

    pbar = tqdm(range(int(config.max_iter)), desc="BM-DFM EM (fast)", leave=False)
    ll0: float | None = None
    converged = False
    for it in pbar:
        # E-step: Kalman filter/smoother is inside the M-step builder via cached stats.
        new_params, ll, em_cache = m_step(
            Y=Y,
            params=params,
            ss=ss,
            config=config,
            em_cache=em_cache,
        )

        loglik_trace.append(float(ll))
        if ll0 is None:
            ll0 = float(ll)

        # Update params and state space for next iter
        params = new_params
        ss = build_state_space(params=params, config=config)

        # Display
        if len(loglik_trace) == 1:
            pbar.set_postfix_str(f"loglik={ll:.2f}")
            continue

        dLL, rel, crit = _convergence_stat(config, loglik_trace[-1], loglik_trace[-2], ll0)
        pbar.set_postfix_str(f"loglik={ll:.2f}, dLL={dLL:.3e}, rel={rel:.3e}")

        # Toolbox-style: allow a couple of iterations before checking
        if it >= 2 and crit < float(config.tol):
            converged = True
            break

    # One last rebuild for returning result consistency
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