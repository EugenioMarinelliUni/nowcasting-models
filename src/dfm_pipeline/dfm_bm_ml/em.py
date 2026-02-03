from __future__ import annotations

from typing import Tuple, Optional, Sequence

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother
from .constraints import toolbox_R_mat, kron_quarterly_constraints, constrained_ls, mm_weights
from .state_builder import BMParams, build_state_space
from .steady_state import safe_sym
from .stability import enforce_var_stability


def _clip_rho(rho: np.ndarray, cap: float = 0.999) -> np.ndarray:
    return np.clip(rho, -cap, cap)


def _floor(x: np.ndarray, floor: float) -> np.ndarray:
    return np.maximum(x, floor)


def _compute_Ezz(a_smooth: np.ndarray, P_smooth: np.ndarray) -> np.ndarray:
    Tn, m = a_smooth.shape
    E = np.zeros((Tn, m, m), dtype=float)
    for t in range(Tn):
        E[t] = P_smooth[t] + np.outer(a_smooth[t], a_smooth[t])
    return E


def em_step_ml(
    Y: np.ndarray,                   # (T, nM+nQ) with NaNs
    params: BMParams,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    p: int,
    ppC: int,
    mm_style: str,
    quarterly_meas_var_floor: float,
    monthly_meas_var_floor: float,
    idio_ar1: bool,
    force_var_stability: bool,
    var_stability_shrink: float,
    min_var: float,
    jitter: float,
    enforce_q_loading_constraint: bool,
    fix_quarterly_R: bool,
    blocks: Optional[np.ndarray],     # (n_obs, n_blocks) or None
) -> Tuple[BMParams, float, np.ndarray, np.ndarray, np.ndarray]:
    """
    One ML-EM iteration with:
      - block-wise factor transitions (A_i, Q_i per block)
      - covariance-aware measurement updates
      - toolbox quarterly loading constraints (proportionality across 5 lags)
      - optional fixing quarterly R near zero (toolbox convention)
      - idio_ar1 switch:
          * True  -> monthly idio states present (AR1), monthly R forced near-zero
          * False -> no monthly idio states (iid), monthly R estimated
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))

    Tm, Qm, C, R, a0, P0, idx = build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p,
        ppC=ppC,
        mm_style=mm_style,
        quarterly_meas_var_floor=quarterly_meas_var_floor,
        monthly_meas_var_floor=monthly_meas_var_floor,
        idio_ar1=bool(idio_ar1),
        jitter=jitter,
    )

    ss = StateSpaceParams(T=Tm, Q=Qm, C=C, R=R, a0=a0, P0=P0)
    res = kalman_filter_smoother(Y, ss)
    loglik = float(res.loglik)

    a = res.a_smooth
    P = res.P_smooth
    P_lag = res.P_lag_smooth
    Ezz = _compute_Ezz(a, P)

    f_t_idx = idx.f_t_idx
    f_stack_idx = idx.f_stack_idx
    idx_m = idx.idx_idio_monthly
    idx_q = idx.idx_idio_quarterly

    # -----------------------------
    # Block-wise update of factor VAR(p) and Q_f per block
    # -----------------------------
    Phi_blocks_new: list[list[np.ndarray]] = []
    Q_f_blocks_new: list[np.ndarray] = []

    for b, rb in enumerate(r_by_block):
        if rb == 0:
            Phi_blocks_new.append([])
            Q_f_blocks_new.append(np.zeros((0, 0), dtype=float))
            continue

        sl = idx.factor_companion_slices[b]
        f0 = np.arange(sl.start, sl.start + rb, dtype=int)
        lag_stack = np.arange(sl.start, sl.start + rb * p, dtype=int) if p > 0 else np.array([], dtype=int)

        if p == 0:
            Phi_blocks_new.append([])
            Q_f_blocks_new.append(params.Q_f_blocks[b].copy())
            continue

        S_xx = np.zeros((rb * p, rb * p), dtype=float)
        S_yx = np.zeros((rb, rb * p), dtype=float)

        for t in range(1, Y.shape[0]):
            cross = P_lag[t] + np.outer(a[t], a[t - 1])
            S_yx += cross[np.ix_(f0, lag_stack)]
            S_xx += Ezz[t - 1][np.ix_(lag_stack, lag_stack)]

        S_xx = safe_sym(S_xx) + np.eye(S_xx.shape[0], dtype=float) * min_var
        B = S_yx @ np.linalg.inv(S_xx)  # (rb, rb*p)
        Phi_list = [B[:, lag * rb:(lag + 1) * rb].copy() for lag in range(p)]

        # innovation covariance on contemporaneous factors
        Qf = np.zeros((rb, rb), dtype=float)
        for t in range(1, Y.shape[0]):
            Ey = Ezz[t][np.ix_(f0, f0)]
            Eyx = (P_lag[t][np.ix_(f0, lag_stack)] + np.outer(a[t, f0], a[t - 1, lag_stack]))
            Exx = Ezz[t - 1][np.ix_(lag_stack, lag_stack)]
            Qf += Ey - Eyx @ B.T - B @ Eyx.T + B @ Exx @ B.T
        Qf = safe_sym(Qf / max(Y.shape[0] - 1, 1)) + np.eye(rb, dtype=float) * min_var

        if force_var_stability and p > 0:
            Phi_list = enforce_var_stability(
                Phi_list,
                ppC=int(ppC),
                shrink=float(var_stability_shrink),
            )

        Phi_blocks_new.append(Phi_list)
        Q_f_blocks_new.append(Qf)

    # -----------------------------
    # Update monthly idiosyncratic AR(1) and stationary variance (only if idio_ar1)
    # -----------------------------
    rho_m_new = params.rho_m.copy().astype(float)
    sig2_m_new = params.sig2_m.copy().astype(float)

    for i in (range(nM) if bool(idio_ar1) else range(0)):
        s_idx = idx_m.start + i
        num = 0.0
        den = 0.0
        for t in range(1, Y.shape[0]):
            cross = P_lag[t][s_idx, s_idx] + a[t, s_idx] * a[t - 1, s_idx]
            prev = Ezz[t - 1][s_idx, s_idx]
            num += float(cross)
            den += float(prev)
        if den > 0.0:
            rho_m_new[i] = num / den
        rho_m_new[i] = float(np.clip(rho_m_new[i], -0.999, 0.999))
        sig2_m_new[i] = float(max(np.mean(Ezz[:, s_idx, s_idx]), min_var))

    # -----------------------------
    # Update quarterly idiosyncratic AR(1) and stationary variance (first state only)
    # -----------------------------
    rho_q_new = params.rho_q.copy().astype(float)
    sig2_q_new = params.sig2_q.copy().astype(float)

    for j in range(nQ):
        s0 = idx_q.start + 5 * j
        num = 0.0
        den = 0.0
        for t in range(1, Y.shape[0]):
            cross = P_lag[t][s0, s0] + a[t, s0] * a[t - 1, s0]
            prev = Ezz[t - 1][s0, s0]
            num += float(cross)
            den += float(prev)
        if den > 0.0:
            rho_q_new[j] = num / den
        rho_q_new[j] = float(np.clip(rho_q_new[j], -0.999, 0.999))
        sig2_q_new[j] = float(max(np.mean(Ezz[:, s0, s0]), min_var))

    # -----------------------------
    # Update monthly loadings Lambda_m with block restrictions
    # -----------------------------
    Lambda_m_new = params.Lambda_m.copy().astype(float)

    if blocks is None:
        blocks_eff = np.ones((nM + nQ, 1), dtype=int)
        r_by_block_eff = (r_total,)
    else:
        blocks_eff = blocks.astype(int)
        r_by_block_eff = tuple(r_by_block)

    block_factor_ranges: list[tuple[int, int]] = []
    c = 0
    for rb in r_by_block_eff:
        block_factor_ranges.append((c, c + rb))
        c += rb

    for i in range(nM):
        y_i = Y[:, i]
        obs = ~np.isnan(y_i)
        if not np.any(obs):
            continue

        block_row = blocks_eff[i]
        allow = np.zeros((r_total,), dtype=bool)
        for b, (lo, hi) in enumerate(block_factor_ranges):
            if int(block_row[b]) == 1:
                allow[lo:hi] = True
        sel = np.where(allow)[0]
        if sel.size == 0:
            continue

        f_sel_idx = f_t_idx[sel]

        denom = np.zeros((sel.size, sel.size), dtype=float)
        nom = np.zeros((sel.size,), dtype=float)

        has_m_idio = bool(idio_ar1)
        s_idio = (idx_m.start + i) if has_m_idio else -1

        for t in np.where(obs)[0]:
            Eff = Ezz[t][np.ix_(f_sel_idx, f_sel_idx)]
            denom += Eff

            Ef = a[t, f_sel_idx]
            if has_m_idio:
                Eif = (P[t][s_idio, f_sel_idx] + a[t, s_idio] * a[t, f_sel_idx])
                nom += y_i[t] * Ef - Eif
            else:
                nom += y_i[t] * Ef

        denom = safe_sym(denom) + np.eye(sel.size, dtype=float) * min_var
        sol = np.linalg.solve(denom, nom)

        Lambda_m_new[i, :] = 0.0
        Lambda_m_new[i, sel] = sol

    # -----------------------------
    # Update quarterly loadings Lambda_q under toolbox proportional constraints
    # -----------------------------
    Lambda_q_new = params.Lambda_q.copy().astype(float)

    if nQ > 0:
        if ppC != 5:
            raise ValueError("Toolbox quarterly constraints assume ppC=5.")
        R_mat, _ = toolbox_R_mat()
        R_con = kron_quarterly_constraints(R_mat, r_total)  # (4*r, 5*r)
        q_con = np.zeros((R_con.shape[0],), dtype=float)

        for j in range(nQ):
            row = nM + j
            y_j = Y[:, row]
            obs = ~np.isnan(y_j)
            if not np.any(obs):
                continue

            denom = np.zeros((5 * r_total, 5 * r_total), dtype=float)
            nom = np.zeros((5 * r_total,), dtype=float)

            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)

            for t in np.where(obs)[0]:
                F_idx = f_stack_idx
                denom += Ezz[t][np.ix_(F_idx, F_idx)]

                Ef = a[t, F_idx]
                w = C[row, idio_idx]  # (5,)
                EidF = (
                    P[t][np.ix_(idio_idx, F_idx)] +
                    np.outer(a[t, idio_idx], a[t, F_idx])
                )  # (5, 5*r)
                EidioF = w @ EidF  # (5*r,)
                nom += y_j[t] * Ef - EidioF

            denom = safe_sym(denom) + np.eye(denom.shape[0], dtype=float) * min_var
            if not enforce_q_loading_constraint:
                raise ValueError(
                    "Unconstrained quarterly loadings require a 5-lag loading parameterization. "
                    "Set enforce_q_loading_constraint=True for toolbox parity."
                )

            C_con = constrained_ls(denom, nom, R_con, q_con)

            # Recover base Lambda_q so the measurement row uses w[lag] * Lambda_q.
            w0 = float(mm_weights(mm_style)[0])
            if w0 == 0.0:
                raise ValueError("MM weight w0 is zero; cannot recover base quarterly loading.")
            Lambda_q_new[j, :] = C_con[0:r_total] / w0

    # -----------------------------
    # Update diagonal measurement noise R
    # -----------------------------
    R_diag_m_new = params.R_diag_m.copy().astype(float)
    R_diag_q_new = params.R_diag_q.copy().astype(float)

    for i in range(nM + nQ):
        y_i = Y[:, i]
        obs_idx = np.where(~np.isnan(y_i))[0]
        if obs_idx.size == 0:
            continue

        if i >= nM and fix_quarterly_R:
            R_diag_q_new[i - nM] = float(quarterly_meas_var_floor)
            continue

        C_row = np.zeros((Tm.shape[0],), dtype=float)

        if i < nM:
            C_row[f_t_idx] = Lambda_m_new[i, :]
            if bool(idio_ar1):
                C_row[idx_m.start + i] = 1.0
        else:
            j = i - nM
            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)
            w = C[i, idio_idx].copy()  # (5,)
            pos = 0
            for lag in range(5):
                lag_idx = f_stack_idx[pos:pos + r_total]
                C_row[lag_idx] = w[lag] * Lambda_q_new[j, :]
                pos += r_total
            C_row[idio_idx] = w

        acc = 0.0
        for t in obs_idx:
            y = float(y_i[t])
            Es = a[t]
            Ess = Ezz[t]
            acc += y * y - 2.0 * y * float(C_row @ Es) + float(C_row @ Ess @ C_row.T)

        var = float(acc / obs_idx.size)
        if i < nM:
            R_diag_m_new[i] = max(var, min_var)
        else:
            R_diag_q_new[i - nM] = max(var, quarterly_meas_var_floor)

    # Toolbox behavior: when monthly idios are in the state, monthly measurement noise is forced near zero.
    if bool(idio_ar1):
        R_diag_m_new = np.full((nM,), float(monthly_meas_var_floor), dtype=float)

    new_params = BMParams(
        Phi_blocks=Phi_blocks_new,
        Q_f_blocks=Q_f_blocks_new,
        rho_m=_clip_rho(rho_m_new),
        sig2_m=_floor(sig2_m_new, min_var),
        rho_q=_clip_rho(rho_q_new),
        sig2_q=_floor(sig2_q_new, min_var),
        Lambda_m=Lambda_m_new,
        Lambda_q=Lambda_q_new,
        R_diag_m=_floor(R_diag_m_new, min_var if not bool(idio_ar1) else float(monthly_meas_var_floor)),
        R_diag_q=_floor(R_diag_q_new, float(quarterly_meas_var_floor)),
    )

    return new_params, loglik, a, P, P_lag
