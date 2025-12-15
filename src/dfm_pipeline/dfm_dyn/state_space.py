import numpy as np
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class StateSpaceParams:
    """
    Linear Gaussian state-space model:

        alpha_t = T @ alpha_{t-1} + eta_t      ,  eta_t ~ N(0, Q)
        y_t     = C @ alpha_t     + epsilon_t  ,  epsilon_t ~ N(0, R)

    Dimensions:
        alpha_t : (m,)
        y_t     : (n,)
        T, Q    : (m, m)
        C, R    : (n, m), (n, n)
    """
    T: np.ndarray
    Q: np.ndarray
    C: np.ndarray
    R: np.ndarray
    a0: np.ndarray
    P0: np.ndarray


@dataclass
class KalmanSmootherResult:
    a_pred: np.ndarray      # (T, m)
    P_pred: np.ndarray      # (T, m, m)
    a_filt: np.ndarray      # (T, m)
    P_filt: np.ndarray      # (T, m, m)
    a_smooth: np.ndarray    # (T, m)
    P_smooth: np.ndarray    # (T, m, m)
    P_lag_smooth: np.ndarray  # (T, m, m), Cov(alpha_t, alpha_{t-1} | Y), t>=1
    loglik: float


def build_companion_transition(
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build companion-form transition matrix T and state covariance Q
    for factor VAR(p):

        f_t = sum_{j=1}^p Phi_j f_{t-j} + u_t,   u_t ~ N(0, Q_u)

    State vector:
        alpha_t = [f_t', f_{t-1}', ..., f_{t-p+1}']'

    Phi_list:
        list of p matrices Phi_j, each (r, r)
    Q_u:
        (r, r) innovation covariance
    """
    r = Phi_list[0].shape[0]
    p = len(Phi_list)
    m = r * p

    T = np.zeros((m, m))
    # top block row: [Phi_1, ..., Phi_p]
    T[:r, :r * p] = np.hstack(Phi_list)
    # subdiagonal identity blocks
    for j in range(1, p):
        T[j * r:(j + 1) * r, (j - 1) * r:j * r] = np.eye(r)

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
    """
    Dynamic factor model:

        f_t = sum Phi_j f_{t-j} + u_t
        x_t = Lambda f_t + e_t

    State vector alpha_t = [f_t', f_{t-1}', ..., f_{t-p+1}']'.

    Lambda:
        (n, r)
    Phi_list:
        list of p (r, r) matrices
    Q_u:
        (r, r)
    R:
        (n, n), usually diagonal

    Returns StateSpaceParams with
        T, Q, C, R, a0, P0
    """
    n, r = Lambda.shape
    p = len(Phi_list)
    if p < 1:
        raise ValueError("p must be >= 1 for companion-form DFM.")

    T, Q = build_companion_transition(Phi_list, Q_u)
    m = T.shape[0]

    C = np.zeros((n, m))
    C[:, :r] = Lambda

    if a0 is None:
        a0 = np.zeros(m)
    if P0 is None:
        P0 = np.eye(m) * init_var_scale

    return StateSpaceParams(T=T, Q=Q, C=C, R=R, a0=a0, P0=P0)


def kalman_filter_smoother(
    Y: np.ndarray,
    ss: StateSpaceParams,
) -> KalmanSmootherResult:
    """
    Kalman filter + Rauch–Tung–Striebel smoother with missing data.

    Y:
        (T, n) array, np.nan for missing entries.
    ss:
        StateSpaceParams(T, Q, C, R, a0, P0)

    Handles arbitrary missingness by selecting the observed
    subvector at each t.
    """
    Y = np.asarray(Y, float)
    Tn, n = Y.shape
    T_mat, Q, C, R, a0, P0 = ss.T, ss.Q, ss.C, ss.R, ss.a0, ss.P0
    m = a0.shape[0]

    a_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    a_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))
    loglik = 0.0

    a_prev = a0.copy()
    P_prev = P0.copy()
    eps = 1e-7

    # Filtering
    for t in range(Tn):
        # prediction
        a_pr = T_mat @ a_prev
        P_pr = T_mat @ P_prev @ T_mat.T + Q

        a_pred[t] = a_pr
        P_pred[t] = P_pr

        y_t = Y[t]
        obs_idx = np.where(~np.isnan(y_t))[0]

        if obs_idx.size == 0:
            # no observation update
            a_filt[t] = a_pr
            P_filt[t] = P_pr
            a_prev, P_prev = a_pr, P_pr
            continue

        C_t = C[obs_idx, :]                      # (k, m)
        R_t = R[np.ix_(obs_idx, obs_idx)]        # (k, k)
        y_obs = y_t[obs_idx]                     # (k,)

        v = y_obs - C_t @ a_pr                   # (k,)
        F_t = C_t @ P_pr @ C_t.T + R_t           # (k, k)
        F_t = (F_t + F_t.T) / 2.0

        try:
            F_inv = np.linalg.inv(F_t)
            sign, logdet = np.linalg.slogdet(F_t)
            if sign <= 0:
                raise np.linalg.LinAlgError
        except np.linalg.LinAlgError:
            F_t = F_t + eps * np.eye(F_t.shape[0])
            F_inv = np.linalg.inv(F_t)
            sign, logdet = np.linalg.slogdet(F_t)

        K = P_pr @ C_t.T @ F_inv                 # (m, k)
        a_upd = a_pr + K @ v                     # (m,)
        P_upd = P_pr - K @ C_t @ P_pr
        P_upd = (P_upd + P_upd.T) / 2.0

        a_filt[t] = a_upd
        P_filt[t] = P_upd

        ll_t = -0.5 * (logdet + v @ F_inv @ v + len(v) * np.log(2.0 * np.pi))
        loglik += ll_t

        a_prev, P_prev = a_upd, P_upd

    # Smoothing
    a_smooth = np.zeros((Tn, m))
    P_smooth = np.zeros((Tn, m, m))
    P_lag_smooth = np.zeros((Tn, m, m))

    a_smooth[-1] = a_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_pr_next = P_pred[t + 1]
        P_pr_next = (P_pr_next + P_pr_next.T) / 2.0

        try:
            P_pr_inv = np.linalg.inv(P_pr_next)
        except np.linalg.LinAlgError:
            P_pr_next = P_pr_next + eps * np.eye(m)
            P_pr_inv = np.linalg.inv(P_pr_next)

        J = P_filt[t] @ T_mat.T @ P_pr_inv       # (m, m)

        a_smooth[t] = a_filt[t] + J @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = P_filt[t] + J @ (P_smooth[t + 1] - P_pr_next) @ J.T
        P_smooth[t] = (P_smooth[t] + P_smooth[t].T) / 2.0

        # Cov(alpha_t, alpha_{t-1} | Y) for t>=1
        P_lag_smooth[t] = J @ P_smooth[t + 1]

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
