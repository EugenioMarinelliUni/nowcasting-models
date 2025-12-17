import numpy as np
from dataclasses import dataclass
from typing import List, Optional

from .state_space import (
    StateSpaceParams,
    build_companion_transition,
    build_dfm_state_space,
    kalman_filter_smoother,
)

# Optional tqdm progress bar
try:
    from tqdm import trange
except ImportError:  # fallback if tqdm is not installed
    trange = range


@dataclass
class DynDFMParams:
    r: int
    p: int
    Phi_list: List[np.ndarray]   # VAR(p) factor dynamics, each (r, r)
    Q_u: np.ndarray              # (r, r)
    Lambda: np.ndarray           # (n, r)
    R: np.ndarray                # (n, n)
    ss: StateSpaceParams
    loglik_history: np.ndarray   # (n_iter,)
    factors_smooth: np.ndarray   # (T, r)
    states_smooth: np.ndarray    # (T, r * p)


def _init_from_pca(
    X: np.ndarray,
    r: int,
    p: int,
    diag_R: bool = True,
) -> tuple[List[np.ndarray], np.ndarray, np.ndarray, np.ndarray]:
    """
    PCA-based initialization (Bańbura–Modugno style):

        1. Fill missing with series means.
        2. Static PCA -> initial F (T, r) and Lambda (n, r).
        3. Fit VAR(p) to F by OLS -> Phi_list, Q_u.
        4. Idiosyncratic R from PCA residuals.
    """
    Tn, n = X.shape

    # crude fill
    X_filled = X.copy()
    col_means = np.nanmean(X_filled, axis=0)
    inds = np.where(np.isnan(X_filled))
    X_filled[inds] = np.take(col_means, inds[1])

    # static PCA (center after mean-imputation)
    Xc = X_filled - X_filled.mean(axis=0, keepdims=True)
    U, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    F_init = U[:, :r] * s[:r]           # (T, r)
    Lambda_init = Vt[:r, :].T           # (n, r)

    # VAR(p) on F_init
    if p < 1:
        Phi_list: List[np.ndarray] = []
        Q_u = np.eye(r) * 0.1
    else:
        Z_rows = Tn - p
        Z = np.zeros((Z_rows, r * p))
        F_target = F_init[p:, :]        # (T-p, r)

        for t in range(p, Tn):
            row_lags = []
            for lag in range(1, p + 1):
                row_lags.append(F_init[t - lag, :])
            Z[t - p, :] = np.concatenate(row_lags)

        # OLS for all r factors at once
        B, _, _, _ = np.linalg.lstsq(Z, F_target, rcond=None)  # (r*p, r)

        Phi_list = []
        for j in range(p):
            # rows j*r:(j+1)*r are Phi_{j+1}^T
            Phi_j = B[j * r:(j + 1) * r, :].T                 # (r, r)
            Phi_list.append(Phi_j)

        # innovations u_t and Q_u
        U_eps = np.zeros_like(F_target)
        for t in range(p, Tn):
            pred = np.zeros(r)
            for j in range(p):
                pred += Phi_list[j] @ F_init[t - j - 1, :]
            U_eps[t - p, :] = F_init[t, :] - pred

        if U_eps.shape[0] > 1:
            Q_u = np.cov(U_eps.T)
        else:
            Q_u = np.eye(r) * 0.1

    # idiosyncratic R from PCA residuals
    X_hat = F_init @ Lambda_init.T
    E = Xc - X_hat

    if diag_R:
        R_diag = np.var(E, axis=0, ddof=1)
        R = np.diag(np.maximum(R_diag, 1e-6))
    else:
        R = np.cov(E.T)
        R = (R + R.T) / 2.0
        eigvals = np.linalg.eigvalsh(R)
        if eigvals.min() <= 1e-8:
            R += (1e-6 - eigvals.min()) * np.eye(n)

    return Phi_list, Q_u, Lambda_init, R


def _spectral_radius(A: np.ndarray) -> float:
    """Spectral radius (max |eigenvalue|) for a square matrix."""
    if A.size == 0:
        return 0.0
    eigvals = np.linalg.eigvals(A)
    return float(np.max(np.abs(eigvals)))


def _stabilize_var_companion(
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
    max_radius: float = 0.995,
    max_tries: int = 20,
) -> tuple[List[np.ndarray], np.ndarray]:
    """Shrink factor VAR coefficients if the companion matrix is unstable.

    EM M-steps can produce an unstable VAR(p). For forecasting, this is usually
    undesirable (exploding factor dynamics). A lightweight stabilization is:
        - build companion matrix
        - if spectral radius > max_radius, scale all Phi_j by a constant factor
          until stability.

    This is a pragmatic guardrail (not a constrained M-step).
    """
    if not Phi_list:
        return Phi_list, Q_u

    # Symmetrize / floor Q_u to be PSD-ish
    Q_u = 0.5 * (Q_u + Q_u.T)
    w = np.linalg.eigvalsh(Q_u)
    if float(np.min(w)) < 1e-10:
        Q_u = Q_u + (1e-10 - float(np.min(w))) * np.eye(Q_u.shape[0])

    T_comp, _ = build_companion_transition(Phi_list, Q_u)
    rad = _spectral_radius(T_comp)
    if not np.isfinite(rad) or rad <= max_radius:
        return Phi_list, Q_u

    # Shrink Phi blocks until stable.
    scale = max_radius / (rad + 1e-12)
    scale = float(np.clip(scale, 0.0, 0.999))
    Phi_new = [scale * Phi for Phi in Phi_list]

    for _ in range(max_tries):
        T_comp, _ = build_companion_transition(Phi_new, Q_u)
        rad = _spectral_radius(T_comp)
        if np.isfinite(rad) and rad <= max_radius:
            return Phi_new, Q_u
        Phi_new = [0.98 * Phi for Phi in Phi_new]

    return Phi_new, Q_u
def em_dfm_full(
    X: np.ndarray,
    r: int,
    p: int,
    max_iter: int = 50,
    tol: float = 1e-4,
    verbose: bool = False,
    diag_R: bool = True,
    a0: Optional[np.ndarray] = None,
    P0: Optional[np.ndarray] = None,
    use_tqdm: bool = False,
) -> DynDFMParams:
    """
    EM estimation of a Bańbura–Modugno-style dynamic factor model.

    Model:
        f_t   = sum_{j=1}^p Phi_j f_{t-j} + u_t,   u_t ~ N(0, Q_u)
        x_t   = Lambda f_t + e_t,                  e_t ~ N(0, R)
        alpha_t = [f_t', f_{t-1}', ..., f_{t-p+1}']'

    X:
        (T, n) panel, np.nan for missing entries.
        Assumed to be already transformed / standardized.
    r:
        number of dynamic factors
    p:
        VAR order for factors
    max_iter:
        EM iterations (must be >= 1)
    tol:
        stopping rule on |ll_{k} - ll_{k-1}|
    diag_R:
        if True, keep R diagonal.
    use_tqdm:
        if True, show a tqdm progress bar over EM iterations (if tqdm is installed).
    """
    if max_iter < 1:
        raise ValueError("max_iter must be >= 1")

    X = np.asarray(X, float)
    Tn, n = X.shape
    mask = ~np.isnan(X)

    # initialization
    Phi_list, Q_u, Lambda, R = _init_from_pca(X, r=r, p=p, diag_R=diag_R)

    ss = build_dfm_state_space(
        Lambda=Lambda,
        Phi_list=Phi_list,
        Q_u=Q_u,
        R=R,
        a0=a0,
        P0=P0,
    )

    loglik_history: list[float] = []

    # explicitly track smoothed states, so we don't need `ks` outside the loop
    factors_smooth: Optional[np.ndarray] = None
    states_smooth: Optional[np.ndarray] = None

    iterator = trange(max_iter) if use_tqdm else range(max_iter)

    for it in iterator:
        # E-step: Kalman filter + smoother
        ks = kalman_filter_smoother(X, ss)

        a_smooth = ks.a_smooth
        P_smooth = ks.P_smooth
        P_lag = ks.P_lag_smooth
        ll = ks.loglik
        loglik_history.append(ll)

        # keep last smoothed states
        factors_smooth = a_smooth[:, :r]
        states_smooth = a_smooth

        # M-step (1): factor VAR(p) dynamics via sufficient statistics
        if p > 0:
            m = ss.T.shape[0]

            S00 = np.zeros((m, m))
            S11 = np.zeros((m, m))
            S10 = np.zeros((m, m))

            for t in range(1, Tn):
                alpha_t = a_smooth[t]
                alpha_tm1 = a_smooth[t - 1]
                P_t = P_smooth[t]
                P_tm1 = P_smooth[t - 1]
                P_t_tm1 = P_lag[t]   # Cov(alpha_t, alpha_{t-1})

                S11 += P_t + np.outer(alpha_t, alpha_t)
                S00 += P_tm1 + np.outer(alpha_tm1, alpha_tm1)
                S10 += P_t_tm1 + np.outer(alpha_t, alpha_tm1)

            S00 = (S00 + S00.T) / 2.0
            S11 = (S11 + S11.T) / 2.0
            S10 = (S10 + S10.T) / 2.0

            S00_inv = np.linalg.pinv(S00)
            T_un = S10 @ S00_inv
            Q_un = (S11 - T_un @ S10.T) / (Tn - 1)
            Q_un = (Q_un + Q_un.T) / 2.0

            # Extract VAR(p) structure from companion-form T
            top = T_un[:r, :r * p]
            new_Phi_list: List[np.ndarray] = []
            for j in range(p):
                Phi_j = top[:, j * r:(j + 1) * r]
                new_Phi_list.append(Phi_j)

            Phi_list = new_Phi_list
            Q_u = Q_un[:r, :r]
            Q_u = (Q_u + Q_u.T) / 2.0

            # Stability guard: shrink VAR coefficients if companion dynamics are unstable.
            Phi_list, Q_u = _stabilize_var_companion(Phi_list, Q_u, max_radius=0.995)

            # Rebuild canonical state-space
            T_mat, Q = build_companion_transition(Phi_list, Q_u)
            ss.T = T_mat
            ss.Q = Q

        # M-step (2): loadings Lambda and idiosyncratic R from smoothed factors
        F_smooth = a_smooth[:, :r]          # (T, r)
        Lambda_new = np.zeros((n, r))

        # Pre-allocate both, to avoid "might be uninitialized" warnings
        R_diag = np.zeros(n)
        R_new = np.zeros((n, n))

        for i in range(n):
            mask_i = mask[:, i]
            Ti = int(mask_i.sum())

            if Ti < r:
                # not enough data for a stable regression; keep old loading
                Lambda_new[i] = Lambda[i]
                if diag_R:
                    R_diag[i] = max(R[i, i], 1.0)
                else:
                    R_new[i, i] = max(R[i, i], 1.0)
                continue

            Fi = F_smooth[mask_i, :]       # (Ti, r)
            yi = X[mask_i, i]              # (Ti,)

            beta, _, _, _ = np.linalg.lstsq(Fi, yi, rcond=None)
            Lambda_new[i] = beta

            resid = yi - Fi @ beta
            if resid.size <= 1:
                var_i = max(R[i, i], 1e-6)
            else:
                var_i = float(np.var(resid, ddof=1))
                if not np.isfinite(var_i) or var_i < 1e-8:
                    var_i = 1e-6

            if diag_R:
                R_diag[i] = var_i
            else:
                R_new[i, i] = var_i

        Lambda = Lambda_new
        if diag_R:
            R = np.diag(R_diag)
        else:
            R = (R_new + R_new.T) / 2.0
            # ensure positive diagonal
            R += np.diag(np.maximum(1e-6 - np.diag(R), 0.0))

        ss.C[:, :r] = Lambda
        ss.R = R

        if verbose:
            print(f"EM iter {it + 1}: loglik = {ll:.3f}")

        if use_tqdm and hasattr(iterator, "set_postfix"):
            try:
                iterator.set_postfix(ll=f"{ll:.3f}")
            except Exception:
                pass

        if it > 0 and abs(loglik_history[-1] - loglik_history[-2]) < tol:
            break

    if factors_smooth is None or states_smooth is None:
        # Should not happen because max_iter >= 1, but keeps linters happy
        raise RuntimeError("EM loop did not run; check max_iter.")

    loglik_arr = np.array(loglik_history, float)
    return DynDFMParams(
        r=r,
        p=p,
        Phi_list=Phi_list,
        Q_u=Q_u,
        Lambda=Lambda,
        R=R,
        ss=ss,
        loglik_history=loglik_arr,
        factors_smooth=factors_smooth,
        states_smooth=states_smooth,
    )
