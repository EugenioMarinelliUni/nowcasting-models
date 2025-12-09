# src/dfm_pipeline/dfm_dyn/state_space.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np


@dataclass
class StateSpaceParams:
    """
    State-space representation of the dynamic factor model:

        alpha_t = T @ alpha_{t-1} + eta_t,    eta_t ~ N(0, Q)
        Y_t     = C @ alpha_t     + eps_t,    eps_t ~ N(0, R)

    where:
      - alpha_t: r*p-dimensional state (companion form)
      - Y_t:     n-dimensional observation (X_t, optionally y_t)
    """

    T: np.ndarray      # (rp, rp) transition
    Q: np.ndarray      # (rp, rp) state noise covariance
    C: np.ndarray      # (n, rp) measurement matrix
    R: np.ndarray      # (n, n)  measurement noise covariance
    a0: np.ndarray     # (rp,)    initial state mean
    P0: np.ndarray     # (rp, rp) initial state covariance


def build_companion_transition(Phi_list: List[np.ndarray], Q_u: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Build companion-form transition (T, Q) for VAR(p) on r factors.

    Phi_list: list of length p, each (r, r) matrix.
    Q_u:      (r, r) covariance of VAR innovation u_t.
    """
    p = len(Phi_list)
    if p < 1:
        raise ValueError("Phi_list must have length >= 1.")
    r = Phi_list[0].shape[0]
    rp = r * p

    # Transition T (rp x rp)
    T = np.zeros((rp, rp), dtype=float)

    # Top block row: [Phi1, Phi2, ..., Phip]
    T[0:r, 0:(r * p)] = np.hstack(Phi_list)

    # Subdiagonal identity blocks: shift f_{t-1},...,f_{t-p+1} down
    if p > 1:
        for i in range(1, p):
            # row block i (i-th lag), col block i-1
            T[i * r:(i + 1) * r, (i - 1) * r:i * r] = np.eye(r)

    # Q: only top-left r x r block is Q_u, rest zeros
    Q = np.zeros((rp, rp), dtype=float)
    Q[0:r, 0:r] = Q_u

    return T, Q


def build_dfm_state_space(
    Lambda: np.ndarray,
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
    R: np.ndarray,
    a0: Optional[np.ndarray] = None,
    P0: Optional[np.ndarray] = None,
) -> StateSpaceParams:
    """
    Build state-space parameters for a VAR(p) DFM on X-only.

    Inputs
    ------
    Lambda : (n, r)
        Factor loadings for X_t.
    Phi_list : list of p (r, r) matrices
        VAR(p) autoregressive coefficient matrices.
    Q_u : (r, r)
        Innovation covariance of VAR on factors.
    R : (n, n)
        Idiosyncratic covariance of X_t (often diagonal).
    a0, P0 : optional
        Initial state mean and covariance. If None, they are set to 0, I.
    """
    n, r = Lambda.shape
    p = len(Phi_list)
    rp = r * p

    T, Q = build_companion_transition(Phi_list, Q_u)

    # Measurement matrix C: [Lambda, 0, ..., 0]
    C = np.zeros((n, rp), dtype=float)
    C[:, 0:r] = Lambda

    if a0 is None:
        a0 = np.zeros(rp, dtype=float)
    if P0 is None:
        P0 = np.eye(rp, dtype=float)

    return StateSpaceParams(
        T=T,
        Q=Q,
        C=C,
        R=R,
        a0=a0,
        P0=P0,
    )
