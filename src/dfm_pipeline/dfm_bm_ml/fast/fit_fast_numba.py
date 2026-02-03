from __future__ import annotations

import numpy as np
from tqdm.auto import tqdm

from ..spec import BMDfmConfig, BMDfmResult
from ..scaling import scale_panel
from ..init import pca_init_factors
from ..constraints import mm_sum_sq
from ..stability import enforce_var_stability
from ..state_builder import BMParams, build_state_space
from .em_fast_numba import em_step_ml_fast_numba


def _fit_var_ols(F: np.ndarray, p: int) -> tuple[list[np.ndarray], np.ndarray]:
    Tn, r = F.shape
    if p == 0:
        return [], np.eye(r, dtype=float) * 0.1

    X_lags = []
    for lag in range(1, p + 1):
        X_lags.append(F[p - lag:Tn - lag, :])
    Z = np.concatenate(X_lags, axis=1)
    Y = F[p:, :]

    B = np.linalg.lstsq(Z, Y, rcond=None)[0].T
    Phi = [B[:, lag * r:(lag + 1) * r] for lag in range(p)]
    resid = Y - Z @ B.T
    Q_f = np.cov(resid.T, bias=True)
    return Phi, Q_f


def fit_bm_dfm_fast_numba(
    Y_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    config: BMDfmConfig,
    *,
    init_params: BMParams | None = None,
) -> BMDfmResult:
    if config.p > 5:
        raise ValueError("Toolbox parity requires p <= 5 (MF constraints).")
    if int(config.n_quarterly) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    Tn, nM = Y_monthly.shape
    nQ = 1
    p = int(config.p)
    ppC = 5

    Y = np.column_stack([Y_monthly, y_quarterly.reshape(-1, 1)]).astype(float)

    # Scaling
    Y, scaler = scale_panel(Y, mode=config.scaling_mode)

    r_by_block = tuple(int(x) for x in config.r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")

    if init_params is None:
        F0, Lambda0 = pca_init_factors(Y[:, :nM], r_total, fill_mode=config.pca_fill)

        Phi_blocks: list[list[np.ndarray]] = []
        Q_f_blocks: list[np.ndarray] = []
        cursor = 0
        for rb in r_by_block:
            Fb = F0[:, cursor:cursor + rb]
            Phi_b, Q_b = _fit_var_ols(Fb, p=p)
            if bool(config.force_var_stability) and p > 0:
                Phi_b = enforce_var_stability(
                    Phi_b,
                    ppC=int(ppC),
                    shrink=float(config.var_stability_shrink),
                )
            Phi_blocks.append(Phi_b)
            Q_f_blocks.append(Q_b)
            cursor += rb

        rho_m = np.full((nM,), float(config.rho_idio_init), dtype=float)
        sig2_m = np.full((nM,), 1.0, dtype=float)

        rho_q = np.full((nQ,), float(config.rho_idio_init), dtype=float)
        sig2_q = np.full((nQ,), 1.0, dtype=float)

        Lambda_m = Lambda0.copy().astype(float)

        Lambda_q = np.zeros((nQ, r_total), dtype=float)
        obs_q = ~np.isnan(Y[:, nM])
        if np.any(obs_q):
            Lambda_q[0, :] = np.linalg.lstsq(F0[obs_q, :], Y[obs_q, nM], rcond=None)[0]

        if bool(config.idio_ar1):
            R_diag_m = np.full((nM,), float(config.monthly_meas_var_floor), dtype=float)
        else:
            R_diag_m = np.full((nM,), 0.5, dtype=float)

        R_diag_q = np.full((nQ,), float(config.quarterly_meas_var_floor), dtype=float)

        if np.any(obs_q):
            yq = Y[obs_q, nM]
            yq_hat = (F0[obs_q, :] @ Lambda_q[0, :].reshape(-1, 1)).reshape(-1)
            resid = yq - yq_hat
            var_q = float(np.nanvar(resid, ddof=0))
            denom = mm_sum_sq(config.mm_weight_style)
            sig2_q[0] = max(var_q / denom, float(config.min_var))
            if config.fix_quarterly_R:
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

    loglik_trace: list[float] = []
    a_last = None
    P_last = None
    P_lag_last = None

    pbar = tqdm(range(int(config.max_iter)), desc="BM-DFM EM (fast-numba)", unit="iter", leave=True)
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
            quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
            monthly_meas_var_floor=float(config.monthly_meas_var_floor),
            idio_ar1=bool(config.idio_ar1),
            force_var_stability=bool(config.force_var_stability),
            var_stability_shrink=float(config.var_stability_shrink),
            min_var=float(config.min_var),
            jitter=float(config.jitter),
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
            fix_quarterly_R=bool(config.fix_quarterly_R),
            blocks=config.blocks,
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

    Tm, Qm, Cm, Rm, a0, P0, idx = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p,
        ppC=ppC,
        mm_style=config.mm_weight_style,
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        monthly_meas_var_floor=float(config.monthly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
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
