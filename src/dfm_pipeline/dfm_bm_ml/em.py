# src/dfm_pipeline/dfm_bm_ml/em.py
from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother
from .ar1 import update_ar1_stationary_moments
from .constraints import (
    constrained_ls,
    kron_quarterly_constraints,
    mm_weights,
    toolbox_R_mat,
)
from .state_builder import BMParams, build_state_space
from .steady_state import project_psd, safe_sym
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
    Y: np.ndarray,  # (T, nM+nQ) with NaNs
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
    P0_mode: str,
    a0_in: Optional[np.ndarray],
    P0_in: Optional[np.ndarray],
    update_initial_state: bool,
    min_var: float,
    jitter: float,
    enforce_q_loading_constraint: bool,
    fix_quarterly_R: bool,
    blocks: Optional[np.ndarray],  # (n_obs, n_blocks) or None
) -> Tuple[BMParams, float, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    One ML-EM iteration (BM mixed-frequency DFM).

    Returns:
      (new_params, loglik, a_smooth, P_smooth, P_lag_smooth, a0_next, P0_next)

    Behavior:
    - If idio_ar1=True: monthly idios are states; monthly measurement variances are pinned
      to monthly_meas_var_floor (toolbox convention).
    - If enforce_q_loading_constraint=True: requires mm_style == "toolbox".
    - Optional VAR stability enforcement for each factor block via shrinkage.
    - Optional initial-state updates: (a0_next,P0_next) set to smoother t=0 moments.
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))

    if bool(enforce_q_loading_constraint) and mm_style != "toolbox":
        raise ValueError('enforce_q_loading_constraint requires mm_style="toolbox".')

    # Build state-space with chosen initial moments
    Tm, Qm, C, R, a0_used, P0_used, idx = build_state_space(
        params=params,
        nM=int(nM),
        nQ=int(nQ),
        r_by_block=r_by_block,
        p=int(p),
        ppC=int(ppC),
        mm_style=str(mm_style),
        quarterly_meas_var_floor=float(quarterly_meas_var_floor),
        idio_ar1=bool(idio_ar1),
        jitter=float(jitter),
        P0_mode=str(P0_mode),
        a0_override=a0_in,
        P0_override=P0_in,
    )

    # Correct API for dfm_dyn.state_space_new: fields are C and R (not Z/H)
    R_used = np.diag(R).copy() if getattr(R, "ndim", 0) == 2 else np.asarray(R, dtype=float).copy()
    ss = StateSpaceParams(T=Tm, Q=Qm, C=C, R=R_used, a0=a0_used, P0=P0_used)

    # Correct return contract: KalmanSmootherResult object
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
        rb = int(rb)
        if rb == 0:
            Phi_blocks_new.append([])
            Q_f_blocks_new.append(np.zeros((0, 0), dtype=float))
            continue

        sl = idx.factor_companion_slices[b]
        f0 = np.arange(sl.start, sl.start + rb, dtype=int)

        if int(p) == 0:
            Phi_blocks_new.append([])
            Q_f_blocks_new.append(params.Q_f_blocks[b].copy().astype(float))
            continue

        lag_stack = np.arange(sl.start, sl.start + rb * int(p), dtype=int)

        Effl = np.zeros((rb, rb * int(p)), dtype=float)
        Ell = np.zeros((rb * int(p), rb * int(p)), dtype=float)

        for t in range(1, Y.shape[0]):
            cross = P_lag[t] + np.outer(a[t], a[t - 1])
            Effl += cross[np.ix_(f0, lag_stack)]
            Ell += Ezz[t - 1][np.ix_(lag_stack, lag_stack)]

        Ell = safe_sym(Ell) + np.eye(Ell.shape[0], dtype=float) * float(min_var)
        Phi_stack = np.linalg.solve(Ell, Effl.T).T  # (rb, rb*p)

        Phi_b_new = [Phi_stack[:, lag * rb : (lag + 1) * rb].copy() for lag in range(int(p))]

        if bool(force_var_stability) and int(p) > 0:
            Phi_b_new = enforce_var_stability(
                Phi_b_new, r=rb, p=int(p), ppC=int(ppC), shrink=float(var_stability_shrink)
            )

        # Q must be updated for the same transition coefficients that are
        # returned. Stability projection changes Phi, so rebuild the stacked
        # matrix before evaluating the innovation covariance sufficient statistic.
        Phi_stack = np.hstack(Phi_b_new)

        Phi_blocks_new.append(Phi_b_new)

        Q_acc = np.zeros((rb, rb), dtype=float)
        count = 0
        for t in range(1, Y.shape[0]):
            Eff_tt = Ezz[t][np.ix_(f0, f0)]
            cross = P_lag[t] + np.outer(a[t], a[t - 1])
            Efl = cross[np.ix_(f0, lag_stack)]
            Ell_tt = Ezz[t - 1][np.ix_(lag_stack, lag_stack)]
            Q_acc += Eff_tt - Phi_stack @ Efl.T - Efl @ Phi_stack.T + Phi_stack @ Ell_tt @ Phi_stack.T
            count += 1

        Q_b = project_psd(Q_acc / max(count, 1), eps=float(min_var))
        Q_f_blocks_new.append(Q_b)

    # -----------------------------
    # Update monthly idios (only if present in the state)
    # -----------------------------
    rho_m_new = params.rho_m.copy().astype(float)
    sig2_m_new = params.sig2_m.copy().astype(float)

    if bool(idio_ar1):
        for i_m in range(int(nM)):
            s_idx = idx_m.start + i_m
            rho_m_new[i_m], sig2_m_new[i_m] = update_ar1_stationary_moments(
                Ezz[:, s_idx, s_idx],
                P_lag[:, s_idx, s_idx],
                a[:, s_idx],
                min_var=float(min_var),
            )

    # -----------------------------
    # Update quarterly idios
    # -----------------------------
    rho_q_new = params.rho_q.copy().astype(float)
    sig2_q_new = params.sig2_q.copy().astype(float)

    for j in range(int(nQ)):
        s0 = idx_q.start + 5 * j
        rho_q_new[j], sig2_q_new[j] = update_ar1_stationary_moments(
            Ezz[:, s0, s0],
            P_lag[:, s0, s0],
            a[:, s0],
            min_var=float(min_var),
        )

    # -----------------------------
    # Update monthly loadings Lambda_m with block restrictions
    # -----------------------------
    Lambda_m_new = params.Lambda_m.copy().astype(float)

    if blocks is None:
        blocks_eff = np.ones((int(nM) + int(nQ), 1), dtype=int)
        r_by_block_eff = (r_total,)
    else:
        blocks_eff = blocks.astype(int)
        r_by_block_eff = r_by_block

    block_factor_ranges: list[tuple[int, int]] = []
    c0 = 0
    for rb in r_by_block_eff:
        block_factor_ranges.append((c0, c0 + int(rb)))
        c0 += int(rb)

    for i in range(int(nM)):
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

        s_idio = (idx_m.start + i) if bool(idio_ar1) else None

        for t in np.where(obs)[0]:
            denom += Ezz[t][np.ix_(f_sel_idx, f_sel_idx)]
            Ef = a[t, f_sel_idx]

            if s_idio is None:
                nom += float(y_i[t]) * Ef
            else:
                Eif = (P[t][s_idio, f_sel_idx] + a[t, s_idio] * a[t, f_sel_idx])
                nom += float(y_i[t]) * Ef - Eif

        denom = safe_sym(denom) + np.eye(sel.size, dtype=float) * float(min_var)
        sol = np.linalg.solve(denom, nom)

        Lambda_m_new[i, :] = 0.0
        Lambda_m_new[i, sel] = sol

    # -----------------------------
    # Update quarterly loadings Lambda_q under toolbox constraints (optional)
    # -----------------------------
    Lambda_q_new = params.Lambda_q.copy().astype(float)

    if int(nQ) > 0:
        if int(ppC) != 5:
            raise ValueError("Quarterly loading updates assume ppC=5.")
        R_mat, _ = toolbox_R_mat()
        R_con = kron_quarterly_constraints(R_mat, r_total)  # (4*r, 5*r)
        q_con = np.zeros((R_con.shape[0],), dtype=float)

        for j in range(int(nQ)):
            row = int(nM) + j
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

                w_idio = C[row, idio_idx]  # (5,)
                EidF = P[t][np.ix_(idio_idx, F_idx)] + np.outer(a[t, idio_idx], a[t, F_idx])
                EidioF = w_idio @ EidF  # (5*r,)
                nom += float(y_j[t]) * Ef - EidioF

            denom = safe_sym(denom) + np.eye(denom.shape[0], dtype=float) * float(min_var)
            if bool(enforce_q_loading_constraint):
                C_con = constrained_ls(denom, nom, R_con, q_con)
            else:
                C_con = np.linalg.solve(denom, nom)

            w0 = float(mm_weights(mm_style)[0])
            if w0 == 0.0:
                raise ValueError("mm_weights[0] is zero; cannot normalize Lambda_q")
            Lambda_q_new[j, :] = C_con[0:r_total] / w0

    # -----------------------------
    # Update diagonal measurement noise R
    # -----------------------------
    R_diag_m_new = params.R_diag_m.copy().astype(float)
    R_diag_q_new = params.R_diag_q.copy().astype(float)

    for i in range(int(nM) + int(nQ)):
        y_i = Y[:, i]
        obs_idx = np.where(~np.isnan(y_i))[0]
        if obs_idx.size == 0:
            continue

        if i >= int(nM) and bool(fix_quarterly_R):
            R_diag_q_new[i - int(nM)] = float(quarterly_meas_var_floor)
            continue

        C_row = np.zeros((Tm.shape[0],), dtype=float)

        if i < int(nM):
            C_row[f_t_idx] = Lambda_m_new[i, :]
            if bool(idio_ar1):
                C_row[idx_m.start + i] = 1.0
        else:
            j = i - int(nM)
            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)
            w_idio = C[i, idio_idx].copy()
            pos = 0
            for lag in range(5):
                lag_idx = f_stack_idx[pos : pos + r_total]
                C_row[lag_idx] = w_idio[lag] * Lambda_q_new[j, :]
                pos += r_total
            C_row[idio_idx] = w_idio

        acc = 0.0
        for t in obs_idx:
            y = float(y_i[t])
            acc += y * y - 2.0 * y * float(C_row @ a[t]) + float(C_row @ Ezz[t] @ C_row.T)

        var = float(acc / obs_idx.size)
        if i < int(nM):
            R_diag_m_new[i] = max(var, float(min_var))
        else:
            R_diag_q_new[i - int(nM)] = max(var, float(quarterly_meas_var_floor))

    if bool(idio_ar1):
        R_diag_m_new[:] = float(monthly_meas_var_floor)

    new_params = BMParams(
        Phi_blocks=Phi_blocks_new,
        Q_f_blocks=Q_f_blocks_new,
        rho_m=_clip_rho(rho_m_new),
        sig2_m=_floor(sig2_m_new, float(min_var)),
        rho_q=_clip_rho(rho_q_new),
        sig2_q=_floor(sig2_q_new, float(min_var)),
        Lambda_m=Lambda_m_new,
        Lambda_q=Lambda_q_new,
        R_diag_m=_floor(R_diag_m_new, float(min_var)),
        R_diag_q=_floor(R_diag_q_new, float(quarterly_meas_var_floor)),
    )

    if bool(update_initial_state):
        a0_next = a[0].copy()
        P0_next = safe_sym(P[0].copy())
    else:
        a0_next = a0_used.copy()
        P0_next = safe_sym(P0_used.copy())

    return new_params, loglik, a, P, P_lag, a0_next, P0_next
