from __future__ import annotations

"""
Fast fitter for the Banbura–Modugno (2014) mixed-frequency DFM.

Follows dfm_pipeline.dfm_bm_ml.fit.fit_bm_dfm but uses:
- fast PCA helpers (dfm_pipeline.dfm_bm_ml.fast.init_fast)
- cached invariants + constrained_ls_fast in fast EM step (dfm_pipeline.dfm_bm_ml.fast.em_fast)

Intended to share the same config surface and behaviors as the slow path.
"""

import warnings
import numpy as np
from tqdm.auto import tqdm

from ..spec import BMDfmConfig, BMDfmResult
from ..scaling import scale_panel
from .init_fast import pca_init_factors
from ..constraints import mm_sum_sq
from ..state_builder import BMParams, build_state_space
from ..stability import enforce_var_stability
from .em_fast import em_step_ml_fast, build_em_cache, EMStepCache


def _fit_var_ols(F: np.ndarray, p: int) -> tuple[list[np.ndarray], np.ndarray]:
    """
    Fit VAR(p) by OLS on factor matrix F (T, r).
    Returns (Phi_list, Q_f) with Phi_list length p and Q_f (r,r).
    """
    Tn, r = F.shape
    if p == 0:
        return [], np.eye(r, dtype=float) * 0.1

    X_lags = []
    for lag in range(1, p + 1):
        X_lags.append(F[p - lag:Tn - lag, :])
    Z = np.concatenate(X_lags, axis=1)  # (T-p, r*p)
    Y = F[p:, :]                        # (T-p, r)

    B = np.linalg.lstsq(Z, Y, rcond=None)[0].T  # (r, r*p)
    Phi = [B[:, lag * r:(lag + 1) * r] for lag in range(p)]

    resid = Y - Z @ B.T
    Q_f = np.cov(resid.T, bias=True)
    return Phi, Q_f


def fit_bm_dfm_fast(
    Y_monthly: np.ndarray,  # (T, nM) with NaNs
    y_quarterly: np.ndarray,  # (T,) with NaNs except quarter-end months
    config: BMDfmConfig,
    *,
    init_params: BMParams | None = None,
    em_cache: EMStepCache | None = None,
) -> BMDfmResult:
    if int(config.n_quarterly) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    if int(config.p) > 5:
        warnings.warn("Toolbox caps p at 5; using p=5.", RuntimeWarning)
        p = 5
    else:
        p = int(config.p)

    ppC = 5
    _, nM = Y_monthly.shape
    nQ = 1

    # Scaling (single source of truth for both PCA init and EM/Kalman)
    Y_raw = np.column_stack([Y_monthly, y_quarterly.reshape(-1, 1)]).astype(float)
    Y_scaled, scaler = scale_panel(Y_raw, mode=str(config.scaling_mode))

    r_by_block = tuple(int(x) for x in config.r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")

    # Initialization
    if init_params is None:
        F0, Lambda0 = pca_init_factors(Y_scaled[:, :nM], r_total, fill_mode=config.pca_fill)

        Phi_blocks: list[list[np.ndarray]] = []
        Q_f_blocks: list[np.ndarray] = []

        cursor = 0
        for rb in r_by_block:
            rb = int(rb)
            Fb = F0[:, cursor:cursor + rb]
            Phi_b, Q_b = _fit_var_ols(Fb, p=p)
            if bool(config.force_var_stability) and p > 0 and rb > 0:
                Phi_b = enforce_var_stability(Phi_b, ppC=int(ppC), shrink=float(config.var_stability_shrink))
            Phi_blocks.append([x.copy() for x in Phi_b])
            Q_f_blocks.append(Q_b.copy())
            cursor += rb

        rho_m = np.full((nM,), float(config.rho_idio_init), dtype=float)
        sig2_m = np.full((nM,), 1.0, dtype=float)
        rho_q = np.full((nQ,), float(config.rho_idio_init), dtype=float)
        sig2_q = np.full((nQ,), 1.0, dtype=float)

        Lambda_m = Lambda0.copy().astype(float)
        Lambda_q = np.zeros((nQ, r_total), dtype=float)

        obs_q = ~np.isnan(Y_scaled[:, nM])
        if np.any(obs_q):
            Lambda_q[0, :] = np.linalg.lstsq(F0[obs_q, :], Y_scaled[obs_q, nM], rcond=None)[0]

        if bool(config.idio_ar1):
            R_diag_m = np.full((nM,), float(config.monthly_meas_var_floor), dtype=float)
        else:
            R_diag_m = np.full((nM,), 0.5, dtype=float)
        R_diag_q = np.full((nQ,), float(config.quarterly_meas_var_floor), dtype=float)

        if np.any(obs_q):
            yq = Y_scaled[obs_q, nM]
            yq_hat = (F0[obs_q, :] @ Lambda_q[0, :].reshape(-1, 1)).reshape(-1)
            resid = yq - yq_hat
            var_q = float(np.nanvar(resid, ddof=0))
            denom = mm_sum_sq(config.mm_weight_style)
            sig2_q[0] = max(var_q / denom, float(config.min_var))
            if bool(config.fix_quarterly_R):
                R_diag_q[0] = float(config.quarterly_meas_var_floor)

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
    else:
        params = init_params

    # Cache invariants for EM
    if em_cache is None:
        em_cache = build_em_cache(
            nM=nM,
            nQ=nQ,
            r_by_block=r_by_block,
            blocks=config.blocks,
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        )

    loglik_trace: list[float] = []
    a_last: np.ndarray | None = None
    P_last: np.ndarray | None = None
    P_lag_last: np.ndarray | None = None

    a0_in: np.ndarray | None = None
    P0_in: np.ndarray | None = None

    pbar = tqdm(range(int(config.max_iter)), desc="BM-DFM EM (fast)", unit="iter", leave=True)
    for it in pbar:
        params, ll, a_last, P_last, P_lag_last, a0_in, P0_in = em_step_ml_fast(
            Y=Y_scaled,
            params=params,
            nM=nM,
            nQ=nQ,
            r_by_block=r_by_block,
            p=p,
            ppC=ppC,
            mm_style=str(config.mm_weight_style),
            quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
            monthly_meas_var_floor=float(config.monthly_meas_var_floor),
            idio_ar1=bool(config.idio_ar1),
            force_var_stability=bool(config.force_var_stability),
            var_stability_shrink=float(config.var_stability_shrink),
            P0_mode=str(config.P0_mode),
            a0_in=a0_in,
            P0_in=P0_in,
            update_initial_state=bool(config.update_initial_state_each_iter),
            min_var=float(config.min_var),
            jitter=float(config.jitter),
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

    if a_last is None or P_last is None or P_lag_last is None:
        raise RuntimeError("EM did not produce smoother outputs.")

    Tm, Qm, Cm, Rm, a0, P0, idx = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p,
        ppC=ppC,
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
        P0_mode=str(config.P0_mode),
        a0_override=a0_in,
        P0_override=P0_in,
    )

    return BMDfmResult(
        config=config,
        loglik_trace=loglik_trace,
        scaler=scaler,
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
        params_final=params,
    )
