# src/dfm_pipeline/dfm_dyn/state_space_new_refactor.py

import numpy as np
from dataclasses import dataclass
from typing import Optional, List, Tuple

# --- line_profiler compat (removes "profile can be undefined" warnings) ---
try:  # pragma: no cover
    profile  # type: ignore[name-defined]
except NameError:  # pragma: no cover
    def profile(func):  # type: ignore[no-redef]
        return func


# SciPy Cholesky tools
try:
    import scipy.linalg as sla

    _HAVE_SCIPY = True
except Exception:  # pragma: no cover
    sla = None
    _HAVE_SCIPY = False


@dataclass
class StateSpaceParams:
    """Linear Gaussian state-space model parameters."""
    T: np.ndarray  # (m, m)
    Q: np.ndarray  # (m, m)
    C: np.ndarray  # (n, m)
    R: np.ndarray  # (n,) diagonal measurement variances OR (n, n) full covariance
    a0: np.ndarray  # (m,)
    P0: np.ndarray  # (m, m)


@dataclass
class KalmanSmootherResult:
    a_pred: np.ndarray          # (T, m)
    P_pred: np.ndarray          # (T, m, m)
    a_filt: np.ndarray          # (T, m)
    P_filt: np.ndarray          # (T, m, m)
    a_smooth: np.ndarray        # (T, m)
    P_smooth: np.ndarray        # (T, m, m)
    P_lag_smooth: np.ndarray    # (T, m, m) with P_lag_smooth[t]=Cov(alpha_t, alpha_{t-1}|Y), t>=1
    loglik: float


def _sym(A: np.ndarray) -> np.ndarray:
    return 0.5 * (A + A.T)


def _chol_factor_numpy(A: np.ndarray, jitter: float = 1e-9, max_tries: int = 12) -> np.ndarray:
    """
    Numpy Cholesky with diagonal jitter fallback (scale-aware).
    """
    A = _sym(A)
    n = A.shape[0]
    if n == 0:
        return A

    diag = np.diag(A)
    scale = float(np.max(diag)) if diag.size else 1.0
    if (not np.isfinite(scale)) or scale <= 0.0:
        scale = 1.0

    I = np.eye(n)
    j = 0.0
    for _ in range(max_tries):
        try:
            return np.linalg.cholesky(A + j * I)
        except np.linalg.LinAlgError:
            j = (jitter * scale) if j == 0.0 else (10.0 * j)

    # last attempt: eigenvalue floor (scale-aware)
    w, V = np.linalg.eigh(A)
    w = np.maximum(w, jitter * scale)
    A_pd = (V * w) @ V.T
    return np.linalg.cholesky(_sym(A_pd))


def _cho_factor_pd(A: np.ndarray, jitter: float = 1e-9, max_tries: int = 12):
    """
    Robust Cholesky factorization with diagonal jitter fallback (scale-aware).

    Returns a SciPy cho_factor tuple (c, lower=True) when SciPy is available,
    otherwise returns (L, True) as a numpy fallback.
    """
    A = _sym(A)
    n = A.shape[0]
    if n == 0:
        if _HAVE_SCIPY:
            return sla.cho_factor(A, lower=True, overwrite_a=False, check_finite=False)
        L = np.linalg.cholesky(A)
        return (L, True)

    diag = np.diag(A)
    scale = float(np.max(diag)) if diag.size else 1.0
    if (not np.isfinite(scale)) or scale <= 0.0:
        scale = 1.0

    if not _HAVE_SCIPY:
        L = _chol_factor_numpy(A, jitter=jitter, max_tries=max_tries)
        return (L, True)

    I = np.eye(n)
    j = 0.0
    for _ in range(max_tries):
        try:
            return sla.cho_factor(
                A + j * I,
                lower=True,
                overwrite_a=False,
                check_finite=False,
            )
        except Exception:
            j = (jitter * scale) if j == 0.0 else (10.0 * j)

    # last attempt: eigenvalue floor (scale-aware)
    w, V = np.linalg.eigh(A)
    w = np.maximum(w, jitter * scale)
    A_pd = _sym((V * w) @ V.T)
    return sla.cho_factor(
        A_pd,
        lower=True,
        overwrite_a=False,
        check_finite=False,
    )


def _cho_solve(fact, B: np.ndarray) -> np.ndarray:
    """Solve (LL')X=B using SciPy cho_solve when available."""
    if not _HAVE_SCIPY:
        L, _lower = fact
        y = np.linalg.solve(L, B)
        return np.linalg.solve(L.T, y)

    return sla.cho_solve(
        fact,
        B,
        overwrite_b=False,
        check_finite=False,
    )


def build_companion_transition(
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Build companion-form (T, Q) for a VAR(p) with innovation covariance Q_u."""
    r = Phi_list[0].shape[0]
    p = len(Phi_list)
    m = r * p

    T = np.zeros((m, m))
    T[:r, : r * p] = np.hstack(Phi_list)
    for j in range(1, p):
        T[j * r: (j + 1) * r, (j - 1) * r: j * r] = np.eye(r)

    Q = np.zeros((m, m))
    Q[:r, :r] = Q_u
    return T, Q


def build_dfm_state_space(
    Lambda: np.ndarray,
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
    R: np.ndarray,
    a0: Optional[np.ndarray] = None,
    P0: Optional[np.ndarray] = None,
    init_var_scale: float = 1e4,
) -> StateSpaceParams:
    """Dynamic factor model in state-space form."""
    n, r = Lambda.shape
    p = len(Phi_list)

    if p == 0:
        T = np.zeros((r, r))
        Q = Q_u.copy()
        C = Lambda.copy()
        m = r
    else:
        T, Q = build_companion_transition(Phi_list, Q_u)
        m = T.shape[0]
        C = np.zeros((n, m))
        C[:, :r] = Lambda

    if a0 is None:
        a0 = np.zeros(m)
    if P0 is None:
        P0 = np.eye(m) * init_var_scale

    return StateSpaceParams(T=T, Q=Q, C=C, R=R, a0=a0, P0=P0)


def _joseph_update(
    *,
    P_pr: np.ndarray,
    K: np.ndarray,
    C_t: np.ndarray,
    use_diag_R: bool,
    R_piece: np.ndarray,
    I_m: np.ndarray,
) -> np.ndarray:
    """Joseph form covariance update (numerically robust, slower)."""
    I_KC = I_m - K @ C_t
    if use_diag_R:
        r_obs = R_piece  # (k,)
        KR = K * r_obs[None, :]
        P_upd = I_KC @ P_pr @ I_KC.T + KR @ K.T
    else:
        P_upd = I_KC @ P_pr @ I_KC.T + K @ R_piece @ K.T
    return _sym(P_upd)


@profile
def kalman_filter_only(Y: np.ndarray, ss: StateSpaceParams):
    """
    Arithmetic-refactor version:
      - reuse B = C_t @ P_pr for both S and K
      - avoid np.ix_ in the R slicing path (dense-R uses 2D fancy indexing cache)
      - PRIORITY 1: replace Joseph update with standard update (fast) + safe fallback
    """
    Y = np.asarray(Y, float)
    Tn, _n = Y.shape

    T_mat, Q, C, R, a0, P0 = ss.T, ss.Q, ss.C, ss.R, ss.a0, ss.P0
    m = a0.shape[0]

    a_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    a_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))

    a_prev = a0.copy()
    P_prev = P0.copy()
    I_m = np.eye(m)
    loglik = 0.0

    # Detect diagonal-R fast path once
    use_diag_R = False
    R_diag = None

    if isinstance(R, np.ndarray) and R.ndim == 1:
        R_diag = np.asarray(R, dtype=float).copy()
        use_diag_R = True
        R_diag = np.maximum(R_diag, 1e-10)
    elif isinstance(R, np.ndarray) and R.ndim == 2 and R.shape[0] == R.shape[1]:
        R_diag_tmp = np.diag(R).astype(float).copy()
        off = R - np.diag(R_diag_tmp)
        if np.max(np.abs(off)) < 1e-12:
            use_diag_R = True
            R_diag = np.maximum(R_diag_tmp, 1e-10)

    # Cache observation patterns
    # value = (obs_idx, C_t, r_obs) if diagonal-R
    #      or (obs_idx, C_t, R_t)   if dense-R
    obs_cache = {}

    # tiny PSD floor for roundoff (applied only if needed)
    diag_floor = 1e-12

    for t in range(Tn):
        # prediction
        if t == 0:
            a_pr = a_prev
            P_pr = P_prev
        else:
            a_pr = T_mat @ a_prev
            TP = T_mat @ P_prev
            P_pr = TP @ T_mat.T + Q

        P_pr = _sym(P_pr)
        a_pred[t] = a_pr
        P_pred[t] = P_pr

        y_t = Y[t]
        obs_mask = ~np.isnan(y_t)
        if not np.any(obs_mask):
            a_filt[t] = a_pr
            P_filt[t] = P_pr
            a_prev, P_prev = a_pr, P_pr
            continue

        key = obs_mask.tobytes()
        cached = obs_cache.get(key)
        if cached is None:
            obs_idx = np.flatnonzero(obs_mask).astype(np.int64)
            C_t = C[obs_idx, :]

            if use_diag_R:
                if R_diag is None:
                    raise RuntimeError("Internal error: R_diag is None in diagonal-R path.")
                r_obs = R_diag[obs_idx]
                obs_cache[key] = (obs_idx, C_t, r_obs)
            else:
                obs_col = obs_idx[:, None]
                obs_row = obs_idx[None, :]
                R_t = R[obs_col, obs_row]
                obs_cache[key] = (obs_idx, C_t, R_t)

            cached = obs_cache[key]

        obs_idx, C_t, R_piece = cached
        y_obs = y_t[obs_idx]

        # innovation
        v = y_obs - C_t @ a_pr

        # reuse B = C_t @ P_pr for both S and K
        B = C_t @ P_pr            # (k, m)
        S = B @ C_t.T             # (k, k)

        if use_diag_R:
            r_obs = R_piece  # (k,)
            S.flat[:: S.shape[0] + 1] += r_obs
        else:
            S = S + R_piece

        S = _sym(S)

        factS = _cho_factor_pd(S, jitter=1e-9)
        S_inv_v = _cho_solve(factS, v)

        # loglik
        Ltri = factS[0]
        logdet_S = 2.0 * float(np.sum(np.log(np.diag(Ltri))))
        k = int(obs_idx.size)
        loglik += -0.5 * (logdet_S + float(v @ S_inv_v) + k * np.log(2.0 * np.pi))

        # K = (S^{-1} B)^T
        S_inv_B = _cho_solve(factS, B)  # (k, m)
        K = S_inv_B.T                   # (m, k)

        # state update
        a_upd = a_pr + K @ v

        # standard covariance update (fast), with fallback to Joseph if needed
        P_upd = P_pr - K @ B
        P_upd = _sym(P_upd)

        d = np.diag(P_upd)
        if (not np.all(np.isfinite(d))) or (float(np.min(d)) < -1e-8):
            P_upd = _joseph_update(P_pr=P_pr, K=K, C_t=C_t, use_diag_R=use_diag_R, R_piece=R_piece, I_m=I_m)
        else:
            if float(np.min(d)) < diag_floor:
                P_upd[np.diag_indices_from(P_upd)] = np.maximum(d, diag_floor)

        a_filt[t] = a_upd
        P_filt[t] = P_upd
        a_prev, P_prev = a_upd, P_upd

    return a_pred, P_pred, a_filt, P_filt, float(loglik)


def kalman_filter_smoother(Y: np.ndarray, ss: StateSpaceParams) -> KalmanSmootherResult:
    """
    Kalman filter + RTS smoother with missing data (NaNs).

    FIX B:
      - remove J allocation
      - compute P_lag_smooth[t] inside backward pass:
          P_lag_smooth[t+1] = P_smooth[t+1] @ J_t.T
      - remove the extra forward pass over t
    """
    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(Y, ss)

    T_mat = ss.T
    Tn, m = a_filt.shape

    a_smooth = np.zeros_like(a_filt)
    P_smooth = np.zeros_like(P_filt)
    P_lag_smooth = np.zeros_like(P_filt)

    a_smooth[-1] = a_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_pred_next = _sym(P_pred[t + 1])
        factP = _cho_factor_pd(P_pred_next, jitter=1e-9)

        # J_t = P_filt[t] T' P_pred[t+1]^{-1}
        M = P_filt[t] @ T_mat.T
        J_t = _cho_solve(factP, M.T).T

        # lag-one covariance for (t+1, t)
        P_lag_smooth[t + 1] = P_smooth[t + 1] @ J_t.T

        a_smooth[t] = a_filt[t] + J_t @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = _sym(P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T)

    return KalmanSmootherResult(
        a_pred=a_pred,
        P_pred=P_pred,
        a_filt=a_filt,
        P_filt=P_filt,
        a_smooth=a_smooth,
        P_smooth=P_smooth,
        P_lag_smooth=P_lag_smooth,
        loglik=loglik,
    )


# -----------------------------------------------------------------------------
# Compatibility wrappers
# -----------------------------------------------------------------------------

def kalman_filter(
    *,
    Y: np.ndarray,
    T: np.ndarray,
    Z: np.ndarray,
    R: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
):
    """Backward-compatible wrapper."""
    ss = StateSpaceParams(
        T=np.asarray(T),
        Q=np.asarray(Q),
        C=np.asarray(Z),
        R=np.asarray(R),
        a0=np.asarray(a0),
        P0=np.asarray(P0),
    )
    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(np.asarray(Y), ss)
    return {
        "Y": np.asarray(Y),
        "ss": ss,
        "a_pred": a_pred,
        "P_pred": P_pred,
        "a_filt": a_filt,
        "P_filt": P_filt,
        "loglik": float(loglik),
    }


def kalman_smoother(kf_res):
    """Backward-compatible smoother wrapper."""
    ss = kf_res["ss"]
    Y = kf_res["Y"]
    return kalman_filter_smoother(np.asarray(Y), ss)