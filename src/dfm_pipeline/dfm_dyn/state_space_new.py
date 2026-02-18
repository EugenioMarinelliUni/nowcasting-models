import numpy as np
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any

# SciPy Cholesky tools (faster than np.linalg.solve in this pattern)
try:
    import scipy.linalg as sla

    _HAVE_SCIPY = True
except Exception:
    sla = None
    _HAVE_SCIPY = False


@dataclass
class StateSpaceParams:
    """Linear Gaussian state-space model parameters."""
    T: np.ndarray  # (m, m)
    Q: np.ndarray  # (m, m)
    C: np.ndarray  # (n, m)
    R: np.ndarray  # (n, n) (or diagonal stored as full matrix)
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


def _chol_factor_numpy(A: np.ndarray, jitter: float = 1e-9, max_tries: int = 8) -> np.ndarray:
    """Cholesky factorization with diagonal jitter fallback."""
    A = _sym(A)
    j = 0.0
    for _ in range(max_tries):
        try:
            return np.linalg.cholesky(A + j * np.eye(A.shape[0]))
        except np.linalg.LinAlgError:
            j = jitter if j == 0.0 else (10.0 * j)

    # last attempt: eigenvalue floor
    w, V = np.linalg.eigh(A)
    w = np.maximum(w, jitter)
    A_pd = (V * w) @ V.T
    return np.linalg.cholesky(_sym(A_pd))


def _cho_factor_pd(A: np.ndarray, jitter: float = 1e-9, max_tries: int = 8):
    """
    Robust Cholesky factorization with diagonal jitter fallback.
    Returns a SciPy cho_factor tuple (c, lower=True) when SciPy is available,
    otherwise returns (L, True) as a numpy fallback.
    """
    A = _sym(A)

    if not _HAVE_SCIPY:
        L = _chol_factor_numpy(A, jitter=jitter, max_tries=max_tries)
        return (L, True)

    j = 0.0
    for _ in range(max_tries):
        try:
            return sla.cho_factor(
                A + j * np.eye(A.shape[0]),
                lower=True,
                overwrite_a=False,
                check_finite=False,
            )
        except Exception:
            j = jitter if j == 0.0 else (10.0 * j)

    # last attempt: eigenvalue floor
    w, V = np.linalg.eigh(A)
    w = np.maximum(w, jitter)
    A_pd = (V * w) @ V.T
    A_pd = _sym(A_pd)

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
    # Transition
    T, Q = build_companion_transition(Phi_list, Q_u)

    n = Lambda.shape[0]
    r = Lambda.shape[1]
    p = len(Phi_list)
    m = r * p

    # Observation matrix in companion state: maps to first r states
    C = np.zeros((n, m))
    C[:, :r] = Lambda

    if a0 is None:
        a0 = np.zeros(m)
    if P0 is None:
        P0 = init_var_scale * np.eye(m)

    return StateSpaceParams(T=T, Q=Q, C=C, R=R, a0=a0, P0=P0)


def kalman_filter_only(
    Y: np.ndarray,
    ss: StateSpaceParams,
    *,
    jitter: float = 1e-9,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
    """
    Kalman filter with missing data (NaNs) only.
    Returns (a_pred, P_pred, a_filt, P_filt, loglik).
    """
    Y = np.asarray(Y, dtype=float)
    T_mat = np.asarray(ss.T, dtype=float)
    Q = np.asarray(ss.Q, dtype=float)
    C = np.asarray(ss.C, dtype=float)
    R = np.asarray(ss.R, dtype=float)
    a_prev = np.asarray(ss.a0, dtype=float).copy()
    P_prev = np.asarray(ss.P0, dtype=float).copy()

    Tn, n_obs = Y.shape
    m = a_prev.shape[0]

    a_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    a_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))

    loglik = 0.0

    # Detect diagonal R
    use_diag_R = (R.ndim == 1) or (R.ndim == 2 and np.allclose(R, np.diag(np.diag(R))))
    if R.ndim == 2 and use_diag_R:
        R_diag = np.diag(R).copy()
    elif R.ndim == 1:
        R_diag = R.copy()
    else:
        R_diag = None

    I = np.eye(m)

    for t in range(Tn):
        # Predict
        a_pr = T_mat @ a_prev
        P_pr = _sym(T_mat @ P_prev @ T_mat.T + Q)

        a_pred[t] = a_pr
        P_pred[t] = P_pr

        y_t = Y[t]
        obs_mask = np.isfinite(y_t)
        if not np.any(obs_mask):
            a_filt[t] = a_pr
            P_filt[t] = P_pr
            a_prev, P_prev = a_pr, P_pr
            continue

        y_obs = y_t[obs_mask]
        C_obs = C[obs_mask, :]

        r_obs: Optional[np.ndarray] = None  # for static analyzers

        if use_diag_R:
            assert R_diag is not None
            r_obs = R_diag[obs_mask]
            S = _sym(C_obs @ P_pr @ C_obs.T + np.diag(r_obs))
        else:
            R_t = R[np.ix_(obs_mask, obs_mask)]
            S = _sym(C_obs @ P_pr @ C_obs.T + R_t)

        factS = _cho_factor_pd(S, jitter=jitter)
        v = y_obs - (C_obs @ a_pr)

        # loglik contribution
        if _HAVE_SCIPY:
            sign, logdet = np.linalg.slogdet(S)
            if sign <= 0:
                # fallback: use cholesky diag
                L, _ = factS
                logdet = 2.0 * np.sum(np.log(np.diag(L)))
        else:
            L, _ = factS
            logdet = 2.0 * np.sum(np.log(np.diag(L)))

        Sv = _cho_solve(factS, v)
        loglik += -0.5 * (v.T @ Sv + logdet + y_obs.size * np.log(2.0 * np.pi))

        # Gain
        PCt = P_pr @ C_obs.T
        K = _cho_solve(factS, PCt.T).T  # (m,k)

        # Joseph covariance update
        I_KC = I - K @ C_obs
        if use_diag_R:
            assert r_obs is not None
            KR = K * r_obs[None, :]  # (m,k)
            P_upd = I_KC @ P_pr @ I_KC.T + KR @ K.T
        else:
            R_piece = R[np.ix_(obs_mask, obs_mask)]
            P_upd = I_KC @ P_pr @ I_KC.T + K @ R_piece @ K.T

        P_upd = _sym(P_upd)
        a_upd = a_pr + K @ v

        a_filt[t] = a_upd
        P_filt[t] = P_upd
        a_prev, P_prev = a_upd, P_upd

    return a_pred, P_pred, a_filt, P_filt, float(loglik)


def kalman_filter_smoother(Y: np.ndarray, ss: StateSpaceParams) -> KalmanSmootherResult:
    """Kalman filter + RTS smoother with missing data (NaNs)."""
    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(Y, ss)

    T_mat = ss.T
    Tn, m = a_filt.shape

    a_smooth = np.zeros_like(a_filt)
    P_smooth = np.zeros_like(P_filt)
    J = np.zeros((max(Tn - 1, 0), m, m))

    a_smooth[-1] = a_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_pred_next = _sym(P_pred[t + 1])
        factP = _cho_factor_pd(P_pred_next, jitter=1e-9)

        # J_t = P_filt[t] T' P_pred[t+1]^{-1}
        M = P_filt[t] @ T_mat.T
        J_t = _cho_solve(factP, M.T).T
        J[t] = J_t

        a_smooth[t] = a_filt[t] + J_t @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = _sym(P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T)

    P_lag_smooth = np.zeros_like(P_smooth)
    for t in range(1, Tn):
        P_lag_smooth[t] = P_smooth[t] @ J[t - 1].T

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


# ---------------------------------------------------------------------
# Backward-compatible wrappers expected by eval_pseudort/bm_pseudort.py
# ---------------------------------------------------------------------

def kalman_filter(
    *,
    Y: np.ndarray,
    T: np.ndarray,
    Z: np.ndarray,
    R: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Dict[str, Any]:
    """
    Backward-compatible Kalman filter API.

    Returns a dict 'kf' that can be passed to kalman_smoother(kf).
    """
    ss = StateSpaceParams(
        T=np.asarray(T, float),
        Q=np.asarray(Q, float),
        C=np.asarray(Z, float),
        R=np.asarray(R, float),
        a0=np.asarray(a0, float),
        P0=np.asarray(P0, float),
    )
    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(np.asarray(Y, float), ss)
    return {
        "Y": np.asarray(Y, float),
        "ss": ss,
        "a_pred": a_pred,
        "P_pred": P_pred,
        "a_filt": a_filt,
        "P_filt": P_filt,
        "loglik": float(loglik),
    }


def kalman_smoother(kf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Backward-compatible RTS smoother API.

    Accepts the dict returned by kalman_filter and returns a dict with keys:
      a_smooth, P_smooth, P_lag_smooth, plus filter arrays and loglik.
    """
    ss: StateSpaceParams = kf["ss"]
    a_pred = kf["a_pred"]
    P_pred = kf["P_pred"]
    a_filt = kf["a_filt"]
    P_filt = kf["P_filt"]
    loglik = float(kf.get("loglik", 0.0))

    T_mat = ss.T
    Tn, m = a_filt.shape

    a_smooth = np.zeros_like(a_filt)
    P_smooth = np.zeros_like(P_filt)
    J = np.zeros((max(Tn - 1, 0), m, m))

    a_smooth[-1] = a_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_pred_next = _sym(P_pred[t + 1])
        factP = _cho_factor_pd(P_pred_next, jitter=1e-9)

        M = P_filt[t] @ T_mat.T
        J_t = _cho_solve(factP, M.T).T
        J[t] = J_t

        a_smooth[t] = a_filt[t] + J_t @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = _sym(P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T)

    P_lag_smooth = np.zeros_like(P_smooth)
    for t in range(1, Tn):
        P_lag_smooth[t] = P_smooth[t] @ J[t - 1].T

    return {
        "a_pred": a_pred,
        "P_pred": P_pred,
        "a_filt": a_filt,
        "P_filt": P_filt,
        "a_smooth": a_smooth,
        "P_smooth": P_smooth,
        "P_lag_smooth": P_lag_smooth,
        "loglik": loglik,
    }
