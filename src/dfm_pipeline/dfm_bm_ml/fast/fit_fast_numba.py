from __future__ import annotations

"""Numba-accelerated fast fitter for the Banbura–Modugno (2014) mixed-frequency DFM.

This module is identical to :mod:`dfm_pipeline.dfm_bm_ml.fast.fit_fast` except
that it uses the Numba-accelerated EM step from :mod:`em_fast_numba`.

The original fast fitter is preserved for A/B comparison.
"""

import numpy as np
from tqdm.auto import tqdm

from ..spec import BMDfmConfig, BMDfmResult
from .init_fast import standardize_panel, pca_init_factors
from ..constraints import mm_sum_sq
from ..state_builder import BMParams, build_state_space
from .em_fast_numba import em_step_ml_fast_numba, build_em_cache


def _fit_var_ols(F: np.ndarray, p: int) -> tuple[list[np.ndarray], np.ndarray]:
    """Fit VAR(p) by OLS and return Phi list and innovation covariance Q_f.

    Important: ensure Q_f is always 2D. For r=1, np.cov returns a scalar.
    """
    Tn, r = F.shape
    if p == 0:
        Q_f = np.eye(r, dtype=float) * 0.1
        return [], Q_f

    X_lags = []
    for lag in range(1, p + 1):
        X_lags.append(F[p - lag : Tn - lag, :])
    Z = np.concatenate(X_lags, axis=1)
    Y = F[p:, :]

    B = np.linalg.lstsq(Z, Y, rcond=None)[0].T
    Phi = [B[:, lag * r : (lag + 1) * r] for lag in range(p)]
    resid = Y - Z @ B.T

    Q_f = np.cov(resid.T, bias=True)
    Q_f = np.atleast_2d(Q_f)  # <-- critical for r=1
    return Phi, Q_f


def fit_bm_dfm_fast_numba(
    Y_monthly: np.ndarray,  # (T, nM) with NaNs
    y_quarterly: np.ndarray,  # (T,) with NaNs except quarter-end months
    config: BMDfmConfig,
    *,
    init_params: BMParams | None = None,
    em_cache=None,
) -> BMDfmResult:
    """Fit the BM-DFM using the same logic as fit_bm_dfm_fast, but with Numba EM.

    Parameters
    ----------
    init_params:
        Optional warm-start initialization. If provided, the EM loop starts from
        these parameters (and skips PCA-based initialization).
    em_cache:
        Optional EM invariant cache (provided by pseudo-RT engine).
    """
    if config.p > 5:
        raise ValueError("Toolbox parity requires p <= 5 (MF constraints).")
    if int(getattr(config, "n_quarterly", 1)) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    Tn, nM = Y_monthly.shape
    nQ = 1
    p = int(config.p)
    ppC = 5

    # Combine into a single observation matrix: [monthly panel | quarterly target]
    Y = np.column_stack([Y_monthly, y_quarterly.reshape(-1, 1)]).astype(float)

    # Internal standardization (disable when inputs are already z-scored)
    if bool(config.standardize):
        Y, _, _ = standardize_panel(Y)

    r_by_block = tuple(int(x) for x in config.r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")

    # ------------------------------------------------------------
    # Initialization:
    #   - If init_params is provided, use it directly (warm-start)
    #   - Otherwise do PCA init + OLS VAR init like baseline
    # ------------------------------------------------------------
    if init_params is not None:
        params = init_params
    else:
        fill_mode = getattr(config, "pca_fill", "interp")
        F0, Lambda0 = pca_init_factors(Y_monthly, r_total, fill_mode=fill_mode)

        # Split factors into blocks and fit VAR(p) per block
        Phi_blocks: list[list[np.ndarray]] = []
        Q_f_blocks: list[np.ndarray] = []

        cursor = 0
        for rb in r_by_block:
            rb = int(rb)
            Fb = F0[:, cursor : cursor + rb]
            Phi_b, Q_b = _fit_var_ols(Fb, p=p)
            Q_b = np.atleast_2d(Q_b)  # <-- safety
            Phi_blocks.append(Phi_b)
            Q_f_blocks.append(Q_b)
            cursor += rb

        rho_idio_init = float(getattr(config, "rho_idio_init", 0.5))

        rho_m = np.full((nM,), rho_idio_init, dtype=float)
        sig2_m = np.full((nM,), 1.0, dtype=float)
        rho_q = np.full((nQ,), rho_idio_init, dtype=float)
        sig2_q = np.full((nQ,), 1.0, dtype=float)

        Lambda_m = Lambda0.copy().astype(float)

        # Quarterly loadings
        Lambda_q = np.zeros((nQ, r_total), dtype=float)
        obs_q = ~np.isnan(Y[:, nM])
        if np.any(obs_q):
            Lambda_q[0, :] = np.linalg.lstsq(F0[obs_q, :], Y[obs_q, nM], rcond=None)[0]

        quarterly_meas_var_floor = float(getattr(config, "quarterly_meas_var_floor", 1e-4))
        min_var = float(getattr(config, "min_var", 1e-8))

        R_diag_m = np.full((nM,), 0.5, dtype=float)
        R_diag_q = np.full((nQ,), quarterly_meas_var_floor, dtype=float)

        if np.any(obs_q):
            yq = Y[obs_q, nM]
            yq_hat = (F0[obs_q, :] @ Lambda_q[0, :].reshape(-1, 1)).reshape(-1)
            resid = yq - yq_hat
            var_q = float(np.nanvar(resid, ddof=0))
            denom = mm_sum_sq(config.mm_weight_style)
            sig2_q[0] = max(var_q / denom, min_var)
            if bool(config.fix_quarterly_R):
                R_diag_q[0] = quarterly_meas_var_floor

        params = BMParams(
            Phi_blocks=Phi_blocks,
            Q_f_blocks=Q_f_blocks,
            rho_m=rho_m,
            sig2_m=sig2_m,
            rho_q=rho_q,
            sig2_q=sig2_q,
            Lambda_m=Lambda_m,
            Lambda_q=Lambda_q,
            R_diag_m=R_diag_m,
            R_diag_q=R_diag_q,
        )

    # Cache invariants for EM steps (reuse if provided by pseudo-RT engine)
    if em_cache is None:
        em_cache = build_em_cache(
            nM=nM,
            nQ=nQ,
            r_by_block=r_by_block,
            blocks=config.blocks,
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        )

    loglik_trace: list[float] = []
    a_last = None
    P_last = None
    P_lag_last = None

    quarterly_meas_var_floor = float(getattr(config, "quarterly_meas_var_floor", 1e-4))
    min_var = float(getattr(config, "min_var", 1e-8))
    jitter = float(getattr(config, "jitter", 1e-12))

    pbar = tqdm(range(int(config.max_iter)), desc="BM-DFM EM (fast+numba)", unit="iter", leave=True)
    for it in pbar:
        params, ll, a_last, P_last, P_lag_last = em_step_ml_fast_numba(
            Y=Y,
            params=params,
            nM=nM,
            nQ=nQ,
            r_by_block=r_by_block,
            p=p,
            ppC=ppC,
            mm_style=config.mm_weight_style,
            quarterly_meas_var_floor=quarterly_meas_var_floor,
            min_var=min_var,
            jitter=jitter,
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
            fix_quarterly_R=bool(config.fix_quarterly_R),
            blocks=config.blocks,
            cache=em_cache,
        )
        loglik_trace.append(float(ll))

        if it == 0:
            pbar.set_postfix_str(f"loglik={ll:.2f}")
            continue

        delta = loglik_trace[-1] - loglik_trace[-2]
        pbar.set_postfix_str(f"loglik={ll:.2f}, dLL={delta:.3e}")
        if it >= 2 and abs(delta) < float(config.tol):
            pbar.set_postfix_str(f"loglik={ll:.2f}, converged")
            break

    # Final state-space for output
    Tm, Qm, Cm, Rm, a0, P0, idx = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p,
        ppC=ppC,
        mm_style=config.mm_weight_style,
        quarterly_meas_var_floor=quarterly_meas_var_floor,
        jitter=jitter,
    )

    return BMDfmResult(
        config=config,
        loglik_trace=loglik_trace,
        T=Tm,
        Q=Qm,
        C=Cm,
        R=Rm,
        a0=a0,
        P0=P0,
        a_smooth=a_last,
        P_smooth=P_last,
        P_lag_smooth=P_lag_last,
        idx_factors=idx.idx_factors,
        idx_idio_monthly=idx.idx_idio_monthly,
        idx_idio_quarterly=idx.idx_idio_quarterly,
        f_t_idx=idx.f_t_idx,
        f_stack_idx=idx.f_stack_idx,
    )