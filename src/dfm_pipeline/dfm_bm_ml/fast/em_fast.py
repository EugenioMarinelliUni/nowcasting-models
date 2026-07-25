from __future__ import annotations

"""
Fast ML-EM step for the Banbura–Modugno mixed-frequency DFM.

Mirrors dfm_pipeline.dfm_bm_ml.em.em_step_ml, but adds:
- EMStepCache for block-selection structures and quarterly constraints
- constrained_ls_fast (no explicit inverses)

Intended to be configuration-consistent with the slow path.
"""

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, List

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

from ..ar1 import update_ar1_stationary_moments
from ..constraints import toolbox_R_mat, kron_quarterly_constraints, mm_weights
from .constraints_fast import constrained_ls_fast
from ..state_builder import BMParams, build_state_space
from ..steady_state import project_psd, safe_sym
from ..stability import enforce_var_stability


def _clip_rho(rho: np.ndarray, cap: float = 0.999) -> np.ndarray:
    return np.clip(rho, -cap, cap)


def _floor(x: np.ndarray, floor: float) -> np.ndarray:
    return np.maximum(x, floor)


def _compute_Ezz(a_smooth: np.ndarray, P_smooth: np.ndarray) -> np.ndarray:
    # Ezz[t] = P[t] + a[t]a[t]'
    return P_smooth + a_smooth[:, :, None] * a_smooth[:, None, :]


@dataclass(frozen=True)
class EMStepCache:
    """
    monthly_sel_factors[i] contains factor indices (0..r_total-1) allowed for monthly series i.
    R_con/q_con are the stacked quarterly loading proportionality constraints (toolbox form).
    """
    monthly_sel_factors: Tuple[np.ndarray, ...]
    R_con: Optional[np.ndarray]
    q_con: Optional[np.ndarray]


def build_em_cache(
    *,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    blocks: Optional[np.ndarray],
    enforce_q_loading_constraint: bool,
) -> EMStepCache:
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))

    if blocks is None:
        monthly_sel = tuple(np.arange(r_total, dtype=int) for _ in range(int(nM)))
    else:
        blocks_eff = blocks.astype(int)
        block_factor_ranges: List[tuple[int, int]] = []
        c = 0
        for rb in r_by_block:
            block_factor_ranges.append((c, c + int(rb)))
            c += int(rb)

        monthly_sel_list: List[np.ndarray] = []
        for i in range(int(nM)):
            block_row = blocks_eff[i]
            allow = np.zeros((r_total,), dtype=bool)
            for b, (lo, hi) in enumerate(block_factor_ranges):
                if int(block_row[b]) == 1:
                    allow[lo:hi] = True
            monthly_sel_list.append(np.where(allow)[0].astype(int))
        monthly_sel = tuple(monthly_sel_list)

    if bool(enforce_q_loading_constraint) and int(nQ) > 0:
        R_mat, _ = toolbox_R_mat()
        R_con = kron_quarterly_constraints(R_mat, r_total)
        q_con = np.zeros((R_con.shape[0],), dtype=float)
    else:
        R_con = None
        q_con = None

    return EMStepCache(monthly_sel_factors=monthly_sel, R_con=R_con, q_con=q_con)


def em_step_ml_fast(
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
    blocks: Optional[np.ndarray],
    cache: EMStepCache,
) -> Tuple[BMParams, float, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    One fast ML-EM iteration.

    Returns:
      (new_params, loglik, a_smooth, P_smooth, P_lag_smooth, a0_next, P0_next)
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))

    if bool(enforce_q_loading_constraint) and mm_style != "toolbox":
        raise ValueError('enforce_q_loading_constraint requires mm_style="toolbox".')

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

    R_used = np.diag(R).copy() if getattr(R, "ndim", 0) == 2 else np.asarray(R, dtype=float).copy()
    ss = StateSpaceParams(T=Tm, Q=Qm, C=C, R=R_used, a0=a0_used, P0=P0_used)
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
    # Update factor VAR blocks
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
        Phi_stack = np.linalg.solve(Ell, Effl.T).T

        Phi_list = [Phi_stack[:, lag * rb:(lag + 1) * rb].copy() for lag in range(int(p))]
        if bool(force_var_stability):
            Phi_list = enforce_var_stability(
                Phi_list, ppC=int(ppC), shrink=float(var_stability_shrink)
            )

        # Keep the transition and innovation-covariance M-step coherent after
        # any stability projection of the VAR coefficients.
        Phi_stack = np.hstack(Phi_list)

        # Q update
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

        Phi_blocks_new.append(Phi_list)
        Q_f_blocks_new.append(Q_b)

    # -----------------------------
    # Update monthly idios (optional)
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
    # Update monthly loadings
    # -----------------------------
    Lambda_m_new = params.Lambda_m.copy().astype(float)

    for i in range(int(nM)):
        y_i = Y[:, i]
        obs_idx = np.where(~np.isnan(y_i))[0]
        if obs_idx.size == 0:
            continue

        sel = cache.monthly_sel_factors[i]
        if sel.size == 0:
            continue

        f_sel_idx = f_t_idx[sel]
        denom = np.zeros((sel.size, sel.size), dtype=float)
        nom = np.zeros((sel.size,), dtype=float)

        s_idio = (idx_m.start + i) if bool(idio_ar1) else None

        for t in obs_idx:
            denom += Ezz[t][np.ix_(f_sel_idx, f_sel_idx)]
            Ef = a[t, f_sel_idx]

            if s_idio is None:
                nom += float(y_i[t]) * Ef
            else:
                Eif = P[t][s_idio, f_sel_idx] + a[t, s_idio] * a[t, f_sel_idx]
                nom += float(y_i[t]) * Ef - Eif

        denom = safe_sym(denom) + np.eye(sel.size, dtype=float) * float(min_var)
        sol = np.linalg.solve(denom, nom)

        Lambda_m_new[i, :] = 0.0
        Lambda_m_new[i, sel] = sol

    # -----------------------------
    # Update quarterly loadings (base)
    # -----------------------------
    Lambda_q_new = params.Lambda_q.copy().astype(float)

    if int(nQ) > 0:
        w = mm_weights(mm_style).astype(float)
        if w.shape != (5,):
            raise ValueError("mm_weights must return shape (5,).")
        w0 = float(w[0])
        if w0 == 0.0:
            raise ValueError("mm_weights[0] is zero; cannot normalize quarterly base loading.")

        for j in range(int(nQ)):
            row = int(nM) + j
            y_j = Y[:, row]
            obs_idx = np.where(~np.isnan(y_j))[0]
            if obs_idx.size == 0:
                continue

            denom = np.zeros((5 * r_total, 5 * r_total), dtype=float)
            nom = np.zeros((5 * r_total,), dtype=float)

            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)
            F_idx = f_stack_idx

            for t in obs_idx:
                denom += Ezz[t][np.ix_(F_idx, F_idx)]
                Ef = a[t, F_idx]

                w_idio = C[row, idio_idx]  # (5,)
                EidF = P[t][np.ix_(idio_idx, F_idx)] + np.outer(a[t, idio_idx], a[t, F_idx])
                EidioF = w_idio @ EidF
                nom += float(y_j[t]) * Ef - EidioF

            denom = safe_sym(denom) + np.eye(denom.shape[0], dtype=float) * float(min_var)

            if bool(enforce_q_loading_constraint):
                if cache.R_con is None or cache.q_con is None:
                    raise RuntimeError("Quarterly constraint cache missing.")
                C_con = constrained_ls_fast(denom, nom, cache.R_con, cache.q_con)
            else:
                C_con = np.linalg.solve(denom, nom)

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
                lag_idx = f_stack_idx[pos:pos + r_total]
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
