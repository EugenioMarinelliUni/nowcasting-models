import numpy as np
from dataclasses import dataclass
from typing import Optional, List, Tuple


@dataclass
class StateSpaceParams:
    """Linear Gaussian state-space model parameters."""

    T: np.ndarray  # (m, m)
    Q: np.ndarray  # (m, m)
    C: np.ndarray  # (n, m)
    R: np.ndarray  # (n, n)
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


def _chol_factor(A: np.ndarray, jitter: float = 1e-9, max_tries: int = 8) -> np.ndarray:
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


def _chol_solve(L: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Solve (L L^T) X = B for X, with L lower-triangular."""
    y = np.linalg.solve(L, B)
    return np.linalg.solve(L.T, y)


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
        T[j * r : (j + 1) * r, (j - 1) * r : j * r] = np.eye(r)

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
    """Dynamic factor model in state-space form.

    Factor dynamics:
      f_t = sum_{j=1}^p Phi_j f_{t-j} + u_t

    Measurement:
      x_t = Lambda f_t + e_t

    State:
      alpha_t = [f_t', f_{t-1}', ..., f_{t-p+1}']'  (for p>=1)

    Special case p=0:
      alpha_t = f_t, with alpha_t = 0 * alpha_{t-1} + u_t.
    """
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


def kalman_filter_only(Y: np.ndarray, ss: StateSpaceParams) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
    """One-sided Kalman filter with missing data (NaNs). Standard initialization.

    At t=0, uses (a0, P0) as prior for alpha_0, then updates with y_0.
    For t>0, does prediction alpha_t|t-1 = T alpha_{t-1|t-1}.

    Returns (a_pred, P_pred, a_filt, P_filt, loglik).
    """
    Y = np.asarray(Y, float)
    Tn, n = Y.shape

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

    for t in range(Tn):
        if t == 0:
            a_pr = a_prev
            P_pr = P_prev
        else:
            a_pr = T_mat @ a_prev
            P_pr = T_mat @ P_prev @ T_mat.T + Q

        P_pr = _sym(P_pr)
        a_pred[t] = a_pr
        P_pred[t] = P_pr

        y_t = Y[t]
        obs_idx = np.where(~np.isnan(y_t))[0]
        if obs_idx.size == 0:
            a_filt[t] = a_pr
            P_filt[t] = P_pr
            a_prev, P_prev = a_pr, P_pr
            continue

        C_t = C[obs_idx, :]
        R_t = R[np.ix_(obs_idx, obs_idx)]
        y_obs = y_t[obs_idx]

        v = y_obs - C_t @ a_pr
        S = C_t @ P_pr @ C_t.T + R_t
        S = _sym(S)

        L = _chol_factor(S, jitter=1e-9)
        S_inv_v = _chol_solve(L, v)
        logdet_S = 2.0 * float(np.sum(np.log(np.diag(L))))

        k = int(obs_idx.size)
        loglik += -0.5 * (logdet_S + float(v @ S_inv_v) + k * np.log(2.0 * np.pi))

        # K = P_pr C_t' S^{-1} = (S^{-1} C_t P_pr)'
        B = C_t @ P_pr  # (k, m)
        S_inv_B = _chol_solve(L, B)
        K = S_inv_B.T

        I_KC = I_m - K @ C_t
        # Joseph-form covariance update for symmetry/PD
        P_upd = I_KC @ P_pr @ I_KC.T + K @ R_t @ K.T
        P_upd = _sym(P_upd)
        a_upd = a_pr + K @ v

        a_filt[t] = a_upd
        P_filt[t] = P_upd

        a_prev, P_prev = a_upd, P_upd

    return a_pred, P_pred, a_filt, P_filt, float(loglik)


def kalman_filter_smoother(Y: np.ndarray, ss: StateSpaceParams) -> KalmanSmootherResult:
    """Kalman filter + RTS smoother with missing data (NaNs).

    P_lag_smooth[t] is Cov(alpha_t, alpha_{t-1} | Y_{0:T-1}) for t>=1.
    """
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
        Lp = _chol_factor(P_pred_next, jitter=1e-9)

        # J_t = P_filt[t] T' P_pred[t+1]^{-1}
        M = P_filt[t] @ T_mat.T
        J_t = np.linalg.solve(Lp.T, np.linalg.solve(Lp, M.T)).T
        J[t] = J_t

        a_smooth[t] = a_filt[t] + J_t @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = _sym(P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T)

    P_lag_smooth = np.zeros_like(P_smooth)
    for t in range(1, Tn):
        # Cov(alpha_t, alpha_{t-1} | Y) = P_t|T * J_{t-1}'
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
