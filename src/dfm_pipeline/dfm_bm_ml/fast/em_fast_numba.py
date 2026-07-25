# src/dfm_pipeline/dfm_bm_ml/fast/em_fast_numba.py

from __future__ import annotations

"""
Numba-accelerated fast ML-EM step for the Banbura–Modugno mixed-frequency DFM.
"""

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, List
import os

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

from ..constraints import toolbox_R_mat, kron_quarterly_constraints, mm_weights
from .constraints_fast import constrained_ls_fast
from ..state_builder import BMParams, build_state_space
from ..steady_state import project_psd, safe_sym
from ..stability import enforce_var_stability

from .numba_kernels import (
    NUMBA_AVAILABLE,
    accumulate_Sxx_Syx,
    accumulate_Q_acc,
    update_rho_sig2_from_diag,
)

# --- line_profiler compat (removes "profile can be undefined" warnings) ---
try:  # pragma: no cover
    profile  # type: ignore[name-defined]
except NameError:  # pragma: no cover
    def profile(func):  # type: ignore[no-redef]
        return func


# Local numba helpers (for eliminating np.ix_ in loadings updates)
_HAVE_NUMBA_LOCAL = False
if bool(NUMBA_AVAILABLE):
    try:  # pragma: no cover
        from numba import njit  # type: ignore
        _HAVE_NUMBA_LOCAL = True
    except Exception:  # pragma: no cover
        njit = None  # type: ignore
        _HAVE_NUMBA_LOCAL = False
else:
    njit = None  # type: ignore
    _HAVE_NUMBA_LOCAL = False


def _clip_rho(rho: np.ndarray, cap: float = 0.999) -> np.ndarray:
    return np.clip(rho, -cap, cap)


def _floor(x: np.ndarray, floor: float) -> np.ndarray:
    return np.maximum(x, floor)


def _compute_Ezz(a_smooth: np.ndarray, P_smooth: np.ndarray) -> np.ndarray:
    return P_smooth + a_smooth[:, :, None] * a_smooth[:, None, :]


def _maybe_contiguous_slice(idxs: np.ndarray):
    """
    Return slice(lo, hi) if idxs are contiguous increasing ints; else return idxs.
    Useful to speed up a[t, idxs] / P[t][row, idxs] when possible.
    """
    idxs = np.asarray(idxs)
    if idxs.ndim != 1 or idxs.size == 0:
        return idxs
    if idxs.size == 1:
        i = int(idxs[0])
        return slice(i, i + 1)
    d = np.diff(idxs)
    if np.all(d == 1):
        lo = int(idxs[0])
        hi = int(idxs[-1]) + 1
        return slice(lo, hi)
    return idxs


# -----------------------------------------------------------------------------
# Hotspot fix A: eliminate np.ix_ in loadings updates
# -----------------------------------------------------------------------------
if _HAVE_NUMBA_LOCAL:

    @njit(cache=True, fastmath=False)
    def _accum_monthly_loading_stats(
        obs_idx: np.ndarray,
        f_sel_idx: np.ndarray,
        y_i: np.ndarray,
        a: np.ndarray,
        P: np.ndarray,
        s_idio: int,
    ):
        """
        Accumulate (denom, nom) for monthly loading regression:
          denom = sum_t E[f_t f_t'] for selected factors
          nom   = sum_t (y_t E[f_t] - E[idio_t f_t]) when idio exists, else y_t E[f_t]
        where E[f_i f_j] = P[t,fi,fj] + a[t,fi]*a[t,fj]
              E[idio f]  = P[t,s_idio,fi] + a[t,s_idio]*a[t,fi]
        """
        k = f_sel_idx.size
        denom = np.zeros((k, k), dtype=np.float64)
        nom = np.zeros((k,), dtype=np.float64)

        use_idio = s_idio >= 0

        for it in range(obs_idx.size):
            t = int(obs_idx[it])
            y = float(y_i[t])

            for ii in range(k):
                fi = int(f_sel_idx[ii])
                a_fi = float(a[t, fi])

                # denom row ii
                for jj in range(k):
                    fj = int(f_sel_idx[jj])
                    denom[ii, jj] += float(P[t, fi, fj]) + a_fi * float(a[t, fj])

                # nom[ii]
                val = y * a_fi
                if use_idio:
                    val -= float(P[t, s_idio, fi]) + float(a[t, s_idio]) * a_fi
                nom[ii] += val

        return denom, nom

    @njit(cache=True, fastmath=False)
    def _accum_quarterly_loading_stats(
        obs_idx: np.ndarray,
        F_idx: np.ndarray,
        idio_idx: np.ndarray,
        w_idio: np.ndarray,
        y_j: np.ndarray,
        a: np.ndarray,
        P: np.ndarray,
    ):
        """
        Accumulate (denom, nom) for quarterly loading regression (base loading):
          denom = sum_t E[F_t F_t']  for F_idx (factor-stack states)
          nom   = sum_t (y_t E[F_t] - E[idio_part_t * F_t]) where
                 idio_part = w_idio' * idio_state (5-shift)
                 E[idio_part * F] = sum_r w_idio[r] * (P[t, s_r, F] + a[t,s_r]*a[t,F])
        """
        kF = F_idx.size
        denom = np.zeros((kF, kF), dtype=np.float64)
        nom = np.zeros((kF,), dtype=np.float64)

        n_idio = idio_idx.size

        for it in range(obs_idx.size):
            t = int(obs_idx[it])
            y = float(y_j[t])

            # denom += E[F F'] and nom += y * E[F]
            for ii in range(kF):
                fi = int(F_idx[ii])
                a_fi = float(a[t, fi])
                nom[ii] += y * a_fi
                for jj in range(kF):
                    fj = int(F_idx[jj])
                    denom[ii, jj] += float(P[t, fi, fj]) + a_fi * float(a[t, fj])

            # nom -= E[idio_part * F]
            for r in range(n_idio):
                s = int(idio_idx[r])
                w = float(w_idio[r])
                if w == 0.0:
                    continue
                a_s = float(a[t, s])
                for ii in range(kF):
                    fi = int(F_idx[ii])
                    nom[ii] -= w * (float(P[t, s, fi]) + a_s * float(a[t, fi]))

        return denom, nom


@dataclass(frozen=True)
class EMStepCache:
    r_total: int
    monthly_sel_factors: Tuple[np.ndarray, ...]
    R_con: Optional[np.ndarray] = None
    q_con: Optional[np.ndarray] = None


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

    return EMStepCache(r_total=r_total, monthly_sel_factors=monthly_sel, R_con=R_con, q_con=q_con)


@profile
def em_step_ml_fast_numba(
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
    P0_mode: str,
    a0_in: Optional[np.ndarray],
    P0_in: Optional[np.ndarray],
    update_initial_state: bool,
    min_var: float,
    jitter: float,
    enforce_q_loading_constraint: bool,
    fix_quarterly_R: bool,
    blocks: Optional[np.ndarray],
    cache: Optional[EMStepCache] = None,
) -> Tuple[BMParams, float, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    One fast (Numba-accelerated) ML-EM iteration.
    Returns: (new_params, loglik, a_smooth, P_smooth, P_lag_smooth, a0_next, P0_next)
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))

    if bool(enforce_q_loading_constraint) and mm_style != "toolbox":
        raise ValueError('enforce_q_loading_constraint requires mm_style="toolbox".')

    if cache is None:
        cache = build_em_cache(
            nM=int(nM),
            nQ=int(nQ),
            r_by_block=r_by_block,
            blocks=blocks,
            enforce_q_loading_constraint=bool(enforce_q_loading_constraint),
        )

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

    # Force diagonal-R representation to avoid R[np.ix_(obs,obs)] in the Kalman code path.
    R_used = np.diag(R).copy() if getattr(R, "ndim", 0) == 2 else np.asarray(R, dtype=float).copy()
    ss = StateSpaceParams(T=Tm, Q=Qm, C=C, R=R_used, a0=a0_used, P0=P0_used)

    # Optional one-shot debug
    if os.getenv("DFM_DEBUG_R", "") and not hasattr(em_step_ml_fast_numba, "_printed_rinfo"):
        em_step_ml_fast_numba._printed_rinfo = True
        Rr = np.asarray(ss.R)
        print(
            "DEBUG ss.R ndim/shape:",
            getattr(Rr, "ndim", None),
            getattr(Rr, "shape", None),
            "nanmin:",
            float(np.nanmin(Rr)) if Rr.size else None,
            flush=True,
        )

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

        if bool(NUMBA_AVAILABLE):
            S_xx, S_yx = accumulate_Sxx_Syx(Ezz, P_lag, a, f0, lag_stack)
        else:
            S_xx = np.zeros((rb * int(p), rb * int(p)), dtype=float)
            S_yx = np.zeros((rb, rb * int(p)), dtype=float)
            for t in range(1, Y.shape[0]):
                cross = P_lag[t] + np.outer(a[t], a[t - 1])
                S_yx += cross[np.ix_(f0, lag_stack)]
                S_xx += Ezz[t - 1][np.ix_(lag_stack, lag_stack)]

        S_xx = safe_sym(S_xx) + np.eye(S_xx.shape[0], dtype=float) * float(min_var)
        Phi_stack = np.linalg.solve(S_xx, S_yx.T).T

        Phi_list = [Phi_stack[:, lag * rb:(lag + 1) * rb].copy() for lag in range(int(p))]
        if bool(force_var_stability):
            Phi_list = enforce_var_stability(Phi_list, ppC=int(ppC), shrink=float(var_stability_shrink))

        # Stability projection changes the transition coefficients. Rebuild the
        # stack so Q is computed for the exact Phi matrices returned to callers.
        Phi_stack = np.hstack(Phi_list)

        if bool(NUMBA_AVAILABLE):
            Q_acc, count = accumulate_Q_acc(Ezz, P_lag, a, f0, lag_stack, Phi_stack)
        else:
            Q_acc = np.zeros((rb, rb), dtype=float)
            count = 0
            for t in range(1, Y.shape[0]):
                Eff_tt = Ezz[t][np.ix_(f0, f0)]
                cross = P_lag[t] + np.outer(a[t], a[t - 1])
                Efl = cross[np.ix_(f0, lag_stack)]
                Ell_tt = Ezz[t - 1][np.ix_(lag_stack, lag_stack)]
                Q_acc += Eff_tt - Phi_stack @ Efl.T - Efl @ Phi_stack.T + Phi_stack @ Ell_tt @ Phi_stack.T
                count += 1

        Q_b = project_psd(Q_acc / max(int(count), 1), eps=float(min_var))

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
            if bool(NUMBA_AVAILABLE):
                rho, s2 = update_rho_sig2_from_diag(
                    Ezz[:, s_idx, s_idx],
                    P_lag[:, s_idx, s_idx],
                    a[:, s_idx],
                    min_var=float(min_var),
                )
                rho_m_new[i_m] = float(np.clip(rho, -0.999, 0.999))
                sig2_m_new[i_m] = float(max(s2, float(min_var)))

    # -----------------------------
    # Update quarterly idios
    # -----------------------------
    rho_q_new = params.rho_q.copy().astype(float)
    sig2_q_new = params.sig2_q.copy().astype(float)

    for j in range(int(nQ)):
        s0 = idx_q.start + 5 * j
        if bool(NUMBA_AVAILABLE):
            rho, s2 = update_rho_sig2_from_diag(
                Ezz[:, s0, s0],
                P_lag[:, s0, s0],
                a[:, s0],
                min_var=float(min_var),
            )
            rho_q_new[j] = float(np.clip(rho, -0.999, 0.999))
            sig2_q_new[j] = float(max(s2, float(min_var)))

    # -----------------------------
    # Update monthly loadings  (FIX A)
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

        f_sel_idx = np.asarray(f_t_idx[sel], dtype=np.int64)

        if _HAVE_NUMBA_LOCAL:
            s_idio_int = int(idx_m.start + i) if bool(idio_ar1) else -1
            denom, nom = _accum_monthly_loading_stats(
                obs_idx.astype(np.int64),
                f_sel_idx,
                y_i,
                a,
                P,
                s_idio_int,
            )
        else:
            # fallback python path (kept for safety)
            Fsel = _maybe_contiguous_slice(f_sel_idx)
            denom = np.zeros((sel.size, sel.size), dtype=float)
            nom = np.zeros((sel.size,), dtype=float)
            s_idio = (idx_m.start + i) if bool(idio_ar1) else None
            for t in obs_idx:
                denom += Ezz[t][np.ix_(f_sel_idx, f_sel_idx)]
                Ef = a[t, Fsel]
                if s_idio is None:
                    nom += float(y_i[t]) * Ef
                else:
                    Eif = P[t][s_idio, Fsel] + a[t, s_idio] * a[t, Fsel]
                    nom += float(y_i[t]) * Ef - Eif

        denom = safe_sym(denom) + np.eye(sel.size, dtype=float) * float(min_var)
        sol = np.linalg.solve(denom, nom)

        Lambda_m_new[i, :] = 0.0
        Lambda_m_new[i, sel] = sol

    # -----------------------------
    # Update quarterly loadings (base)  (FIX A)
    # -----------------------------
    Lambda_q_new = params.Lambda_q.copy().astype(float)

    if int(nQ) > 0:
        w = mm_weights(mm_style).astype(float)
        if w.shape != (5,):
            raise ValueError("mm_weights must return shape (5,).")
        w0 = float(w[0])
        if w0 == 0.0:
            raise ValueError("mm_weights[0] is zero; cannot normalize quarterly base loading.")

        F_idx = np.asarray(f_stack_idx, dtype=np.int64)

        for j in range(int(nQ)):
            row = int(nM) + j
            y_j = Y[:, row]
            obs_idx = np.where(~np.isnan(y_j))[0]
            if obs_idx.size == 0:
                continue

            idio_idx = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=np.int64)
            w_idio = np.asarray(C[row, idio_idx], dtype=np.float64)

            if _HAVE_NUMBA_LOCAL:
                denom, nom = _accum_quarterly_loading_stats(
                    obs_idx.astype(np.int64),
                    F_idx,
                    idio_idx,
                    w_idio,
                    y_j,
                    a,
                    P,
                )
            else:
                # fallback python path (kept for safety)
                denom = np.zeros((5 * r_total, 5 * r_total), dtype=float)
                nom = np.zeros((5 * r_total,), dtype=float)
                for t in obs_idx:
                    denom += Ezz[t][np.ix_(F_idx, F_idx)]
                    Ef = a[t, F_idx]
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
    # PRIORITY 2A: skip expensive estimation when R is fixed by config
    # -----------------------------
    R_diag_m_new = params.R_diag_m.copy().astype(float)
    R_diag_q_new = params.R_diag_q.copy().astype(float)

    need_estimate_monthly_R = (not bool(idio_ar1))
    need_estimate_quarterly_R = (int(nQ) > 0) and (not bool(fix_quarterly_R))

    if not need_estimate_monthly_R:
        R_diag_m_new[:] = float(monthly_meas_var_floor)

    if int(nQ) > 0 and bool(fix_quarterly_R):
        R_diag_q_new[:] = float(quarterly_meas_var_floor)

    if need_estimate_monthly_R or need_estimate_quarterly_R:
        for i_obs in range(int(nM) + int(nQ)):
            if i_obs < int(nM) and (not need_estimate_monthly_R):
                continue
            if i_obs >= int(nM) and (not need_estimate_quarterly_R):
                continue

            y_i2 = Y[:, i_obs]
            obs_idx2 = np.where(~np.isnan(y_i2))[0]
            if obs_idx2.size == 0:
                continue

            C_row = np.zeros((Tm.shape[0],), dtype=float)

            if i_obs < int(nM):
                C_row[f_t_idx] = Lambda_m_new[i_obs, :]
                if bool(idio_ar1):
                    C_row[idx_m.start + i_obs] = 1.0
            else:
                j = i_obs - int(nM)
                idio_idx2 = np.arange(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1), dtype=int)
                w_idio2 = C[i_obs, idio_idx2].copy()
                pos = 0
                for lag in range(5):
                    lag_idx = f_stack_idx[pos:pos + r_total]
                    C_row[lag_idx] = w_idio2[lag] * Lambda_q_new[j, :]
                    pos += r_total
                C_row[idio_idx2] = w_idio2

            acc = 0.0
            for t in obs_idx2:
                yv = float(y_i2[t])
                acc += yv * yv - 2.0 * yv * float(C_row @ a[t]) + float(C_row @ Ezz[t] @ C_row.T)

            var = float(acc / obs_idx2.size)
            if i_obs < int(nM):
                R_diag_m_new[i_obs] = max(var, float(min_var))
            else:
                R_diag_q_new[i_obs - int(nM)] = max(var, float(quarterly_meas_var_floor))

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