from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, Dict, Tuple

import numpy as np
from tqdm.auto import tqdm

from .spec import BMDfmConfig, BMDfmResult
from .init import pca_init_factors
from .constraints import mm_sum_sq
from .state_builder import BMParams, build_state_space
from .em import em_step_ml

from .scaling import scale_panel
from .stability import enforce_var_stability


def _fit_var_ols(F: np.ndarray, p: int) -> tuple[list[np.ndarray], np.ndarray]:
    """
    Fit VAR(p) by OLS on factor matrix F (T, r).
    Returns (Phi_list, Q_f) with Phi_list length p and Q_f (r,r).
    """
    Tn, r = F.shape
    if p <= 0:
        return [], np.eye(r, dtype=float) * 0.1

    X_lags = []
    for lag in range(1, p + 1):
        X_lags.append(F[p - lag : Tn - lag, :])
    Z = np.concatenate(X_lags, axis=1)  # (T-p, r*p)
    Y = F[p:, :]  # (T-p, r)

    B = np.linalg.lstsq(Z, Y, rcond=None)[0].T  # (r, r*p)
    Phi = [B[:, lag * r : (lag + 1) * r] for lag in range(p)]
    resid = Y - Z @ B.T
    Q_f = np.cov(resid.T, bias=True)
    Q_f = np.atleast_2d(Q_f)
    return Phi, Q_f


def _filter_kwargs_for_dataclass(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not is_dataclass(cls):
        return kwargs
    valid = {f.name for f in fields(cls)}
    return {k: v for k, v in kwargs.items() if k in valid}


def fit_bm_dfm(
    Y_monthly: np.ndarray,  # (T, nM) with NaNs
    y_quarterly: np.ndarray,  # (T,) with NaNs except quarter-end months
    config: BMDfmConfig,
) -> BMDfmResult:
    """
    Mixed-frequency DFM estimated by ML-EM (Banbura–Modugno style).

    Toolbox constraint: MF aggregation uses a 5-month stack (ppC=5).
    This fitter caps VAR lag order at p_use = min(p, 5) for internal consistency.
    """
    if int(getattr(config, "n_quarterly", 1)) != 1:
        raise ValueError("This implementation supports n_quarterly=1 (single quarterly target).")

    Tn, nM = Y_monthly.shape
    nQ = 1

    p_in = int(getattr(config, "p", 0))
    p_use = int(min(p_in, 5))
    ppC = 5

    # Assemble Y = [X, y_q] and apply scaling ONCE for both PCA and EM
    Y_raw = np.column_stack([Y_monthly, y_quarterly.reshape(-1, 1)]).astype(float)

    scaling_mode = getattr(config, "scaling_mode", None)
    if scaling_mode is None:
        scaling_mode = "internal_per_run" if bool(getattr(config, "standardize", False)) else "external_frozen"

    Y, scaler = scale_panel(Y_raw, mode=str(scaling_mode))

    r_by_block = tuple(int(x) for x in getattr(config, "r_by_block"))
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")

    blocks = getattr(config, "blocks", None)

    # PCA init on the SCALED monthly panel only
    F0, Lambda0 = pca_init_factors(Y[:, :nM], r_total, fill_mode=str(getattr(config, "pca_fill", "mean")))

    # Split factors into blocks and fit VAR(p_use) per block
    Phi_blocks: list[list[np.ndarray]] = []
    Q_f_blocks: list[np.ndarray] = []

    cursor = 0
    for rb in r_by_block:
        Fb = F0[:, cursor : cursor + rb]
        Phi_b, Q_b = _fit_var_ols(Fb, p=p_use)

        if bool(getattr(config, "force_var_stability", True)) and p_use > 0:
            shrink = float(getattr(config, "var_stability_shrink", 0.98))
            Phi_b, _ = enforce_var_stability(Phi_b, shrink=shrink)

        Phi_blocks.append(Phi_b)
        Q_f_blocks.append(Q_b)
        cursor += rb

    # Idiosyncratic initial params
    rho_init = float(getattr(config, "rho_idio_init", 0.10))
    rho_m = np.full((nM,), rho_init, dtype=float)
    sig2_m = np.full((nM,), 1.0, dtype=float)

    rho_q = np.full((nQ,), rho_init, dtype=float)
    sig2_q = np.full((nQ,), 1.0, dtype=float)

    # Loadings init (monthly)
    Lambda_m = Lambda0.copy().astype(float)

    # Quarterly base loadings init
    Lambda_q = np.zeros((nQ, r_total), dtype=float)
    obs_q = ~np.isnan(Y[:, nM])
    if np.any(obs_q):
        Lambda_q[0, :] = np.linalg.lstsq(F0[obs_q, :], Y[obs_q, nM], rcond=None)[0]

    # Measurement noise init
    monthly_floor = float(getattr(config, "monthly_meas_var_floor", 1e-4))
    quarterly_floor = float(getattr(config, "quarterly_meas_var_floor", 1e-4))

    idio_ar1 = bool(getattr(config, "idio_ar1", True))
    R_diag_m = np.full((nM,), monthly_floor if idio_ar1 else 0.5, dtype=float)
    R_diag_q = np.full((nQ,), quarterly_floor, dtype=float)

    # Quarterly idio scaling via /sum(w^2)
    if np.any(obs_q):
        yq = Y[obs_q, nM]
        yq_hat = (F0[obs_q, :] @ Lambda_q[0, :].reshape(-1, 1)).reshape(-1)
        resid = yq - yq_hat
        var_q = float(np.nanvar(resid, ddof=0))
        denom = float(mm_sum_sq(str(getattr(config, "mm_weight_style", "toolbox"))))
        sig2_q[0] = max(var_q / denom, float(getattr(config, "min_var", 1e-8)))

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

    # EM loop
    loglik_trace: list[float] = []
    max_iter = int(getattr(config, "max_iter", 200))
    tol = float(getattr(config, "tol", 1e-6))

    for it in tqdm(range(max_iter), desc="BM-DFM EM", leave=False):
        params, ll, a_smooth, P_smooth, P_lag_smooth = em_step_ml(
            Y=Y,
            params=params,
            nM=nM,
            nQ=nQ,
            r_by_block=r_by_block,
            p=p_use,
            ppC=ppC,
            mm_style=str(getattr(config, "mm_weight_style", "toolbox")),
            quarterly_meas_var_floor=quarterly_floor,
            min_var=float(getattr(config, "min_var", 1e-8)),
            jitter=float(getattr(config, "jitter", 1e-12)),
            enforce_q_loading_constraint=bool(getattr(config, "enforce_quarterly_loading_constraint", True)),
            fix_quarterly_R=bool(getattr(config, "fix_quarterly_R", True)),
            blocks=blocks,
        )
        loglik_trace.append(float(ll))

        if it >= 1 and abs(loglik_trace[-1] - loglik_trace[-2]) < tol:
            break

    # Final state space + indices
    Tm, Qm, C, R, a0, P0, idx = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p_use,
        ppC=ppC,
        mm_style=str(getattr(config, "mm_weight_style", "toolbox")),
        quarterly_meas_var_floor=quarterly_floor,
        jitter=float(getattr(config, "jitter", 1e-12)),
    )

    res_kwargs: Dict[str, Any] = dict(
        config=config,
        loglik_trace=loglik_trace,
        T=Tm,
        Q=Qm,
        C=C,
        R=R,
        a0=a0,
        P0=P0,
        a_smooth=a_smooth,
        P_smooth=P_smooth,
        P_lag_smooth=P_lag_smooth,
        idx_factors=idx.idx_factors,
        idx_idio_monthly=idx.idx_idio_monthly,
        idx_idio_quarterly=idx.idx_idio_quarterly,
        f_t_idx=idx.f_t_idx,
        f_stack_idx=idx.f_stack_idx,
        params_final=params,
        scaler=scaler,
        p_use=p_use,
        ppC=ppC,
    )
    res_kwargs = _filter_kwargs_for_dataclass(BMDfmResult, res_kwargs)
    return BMDfmResult(**res_kwargs)
