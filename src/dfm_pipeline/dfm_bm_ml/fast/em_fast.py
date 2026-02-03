from __future__ import annotations

"""
Fast ML-EM step for the Banbura–Modugno mixed-frequency DFM.

Revisions included:
  - Re-introduce EMStepCache + build_em_cache (required by fit_fast imports)
  - Implement toolbox-aligned quarterly loading base recovery for scaled MM weights
  - Implement idio_ar1 switch:
      * True  -> monthly idio states present (AR1), monthly R forced near-zero
      * False -> no monthly idio states (iid), monthly R estimated
  - Fix VAR stability call: pass ppC
"""

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, List

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother
from ..constraints import toolbox_R_mat, kron_quarterly_constraints
from .constraints_fast import constrained_ls_fast
from ..state_builder import BMParams, build_state_space
from ..steady_state import safe_sym


def _clip_rho(rho: np.ndarray, cap: float = 0.999) -> np.ndarray:
    return np.clip(rho, -cap, cap)


def _floor(x: np.ndarray, floor: float) -> np.ndarray:
    return np.maximum(x, floor)


def _compute_Ezz_fast(a_smooth: np.ndarray, P_smooth: np.ndarray) -> np.ndarray:
    """Vectorized Ezz[t] = P[t] + a[t] a[t]'."""
    return P_smooth + a_smooth[:, :, None] * a_smooth[:, None, :]


@dataclass(frozen=True)
class EMStepCache:
    """
    monthly_sel_factors[i] contains the factor indices (0..r_total-1) that
    are allowed for monthly series i (based on blocks).
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
        monthly_sel = tuple(np.arange(r_total, dtype=int) for _ in range(nM))
    else:
        blocks_eff = blocks.astype(int)
        block_factor_ranges: List[tuple[int, int]] = []
        c = 0
        for rb in r_by_block:
            block_factor_ranges.append((c, c + rb))
            c += rb

        monthly_sel_list: List[np.ndarray] = []
        for i in range(nM):
            block_row = blocks_eff[i]
            allow = np.zeros((r_total,), dtype=bool)
            for b, (lo, hi) in enumerate(block_factor_ranges):
                if int(block_row[b]) == 1:
                    allow[lo:hi] = True
            sel = np.where(allow)[0]
            monthly_sel_list.append(sel.astype(int))
        monthly_sel = tuple(monthly_sel_list)

    if enforce_q_loading_constraint and nQ > 0:
        R_mat, _ = toolbox_R_mat()
        R_con = kron_quarterly_constraints(R_mat, r_total)
        q_con = np.zeros((R_con.shape[0],), dtype=float)
    else:
        R_con = None
        q_con = None

    return EMStepCache(monthly_sel_factors=monthly_sel, R_con=R_con, q_con=q_con)


def em_step_ml_fast(
    Y: np.ndarray,
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
    blocks: Optional[np.ndarray],
    cache: EMStepCache,
) -> Tuple[BMParams, float, np.ndarray, np.ndarray, np.ndarray]:

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
    Ezz = _compute_Ezz_fast(a, P)

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
        lag_stack = np.arange(sl.start, sl.start + rb * p, dtype=int) if p > 0 else np.array([], dtype=int)

        if p == 0:
            Phi_blocks_new.append([])
            Q_f_blocks_new.append(params.Q_f_blocks[b].copy())
            continue

        S_xx = np.zeros((rb * p, rb * p), dtype=float)
        S_yx = np.zeros((rb, rb * p), dtype=float)

        for t in range(1, Y.shape[0]):
            cross = P_lag[t] + np.outer(a[t], a[t - 1])
            S_yx += cross[f0][:, lag_stack_toggle(lag_stack)]
            S_xx += Ezz[t - 1][lag_stack_toggle(lag_stack)][:, lag_stack_toggle(lag_stack)]

        S_xx = safe_sym(S_xx) + np.eye(S_xx.shape[0], dtype=float) * min_var
        B = S_yx @ np.linalg.inv(S_xx)
        Phi_list = [B[:, lag * rb:(lag + 1) * rb].copy() for lag in range(p)]

        Qf = np.zeros((rb, rb), dtype=float)
        for t in range(1, Y.shape[0]):
            Ey = Ezz[t][f0][:, f0]
            Eyx = P_lag[t][f0][:, lag_stack] + np.outer(a[t, f0], a[t - 1, lag_stack])
            Exx = Ezz[t - 1][lag_stack][:, lag_stack]
            Qf += Ey - Eyx @ B.T - B @ Eyx.T + B @ Exx @ B.T
        Qf = safe_sym(Qf / max(Y.shape[0] - 1, 1)) + np.eye(rb, dtype=float) * min_var

        if force_var_stability and p > 0:
            from ..stability import enforce_var_stability
            Phi_list = enforce_var_stability(
                Phi_list,
                ppC=int(ppC),
                shrink=float(var_stability_shrink),
            )

        Phi_blocks_new.append(Phi_list)
        Q_f_blocks_new.append(Qf)

    # ------------------------------------------------------------
    # Update monthly idiosyncratic AR(1) (only if idio_ar1)
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # Update quarterly idio AR(1) (first state only)
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # Update monthly loadings Lambda_m
    # ------------------------------------------------------------
    Lambda_m_new = params.Lambda_m.copy().astype(float)

    for i in range(nM):
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

        has_m_idio = bool(idio_ar1)
        s_idio = (idx_m.start + i) if has_m_idio else -1

        for t in obs_idx:
            denom += Ezz[t][f_sel_idx][:, f_sel_idx]

            Ef = a[t, f_sel_idx]
            if has_m_idio:
                Eif = P[t][s_idio, f_sel_idx] + a[t, s_idio] * a[t, f_sel_idx]
                nom += y_i[t] * Ef - Eif
            else:
                nom += y_i[t] * Ef

        denom = safe_sym(denom) + np.eye(sel.size, dtype=float) * min_var
        sol = np.linalg.solve(denom, nom)

        Lambda_m_new[i, :] = 0.0
        Lambda_m_new[i, sel] = sol

    # ------------------------------------------------------------
    # Update quarterly loadings Lambda_q
    # ------------------------------------------------------------
    Lambda_q_new = params.Lambda_q.copy().astype(float)

    if nQ > 0:
        if ppC != 5:
            raise ValueError("Toolbox quarterly constraints assume ppC=5.")

        for j in range(nQ):
            row = nM + j
            y_j = Y[:, row]
            obs_idx = np.where(~np.isnan(y_j))[0]
            if obs_idx.size == 0:
                continue

            denom = np.zeros((5 * r_total, 5 * r_total), dtype=float)
            nom = np.zeros((5 * r_total,), dtype=float)

            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)
            F_idx = f_stack_idx

            for t in obs_idx:
                denom += Ezz[t][F_idx][:, F_idx]

                Ef = a[t, F_idx]
                w = C[row, idio_idx]
                EidF = P[t][idio_idx][:, F_idx] + np.outer(a[t, idio_idx], a[t, F_idx])
                EidioF = w @ EidF
                nom += y_j[t] * Ef - EidioF

            denom = safe_sym(denom) + np.eye(denom.shape[0], dtype=float) * min_var

            if not enforce_q_loading_constraint:
                raise ValueError(
                    "Unconstrained quarterly loadings require a 5-lag loading parameterization. "
                    "Set enforce_q_loading_constraint=True for toolbox parity."
                )
            if cache.R_con is None or cache.q_con is None:
                raise RuntimeError("Quarterly constraint cache missing.")
            C_con = constrained_ls_fast(denom, nom, cache.R_con, cache.q_con)

            w0 = float(C[row, idio_idx][0])
            if w0 == 0.0:
                raise ValueError("MM weight w0 is zero; cannot recover base quarterly loading.")
            Lambda_q_new[j, :] = C_con[0:r_total] / w0

    # ------------------------------------------------------------
    # Update diagonal measurement noise R
    # ------------------------------------------------------------
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
            w = C[i, idio_idx].copy()
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
