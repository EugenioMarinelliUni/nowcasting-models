from __future__ import annotations

from typing import Sequence, Optional

import numpy as np


def _companion_from_var(Phi_lags: Sequence[np.ndarray], *, ppC: int) -> np.ndarray:
    """
    Build ppC-lag companion matrix for VAR(p) with p=len(Phi_lags).

    State ordering within a block:
        [f_t, f_{t-1}, ..., f_{t-ppC+1}]

    Only the first p lag blocks are populated.
    """
    p = len(Phi_lags)
    if p <= 0:
        raise ValueError("Phi_lags must be non-empty")

    r = int(Phi_lags[0].shape[0])
    for A in Phi_lags:
        if A.shape != (r, r):
            raise ValueError("All Phi_lags must have shape (r,r) and share the same r")

    m = r * int(ppC)
    Acomp = np.zeros((m, m), dtype=float)

    # Top row: [Phi1 Phi2 ... Phip 0 ... 0]
    for lag, Phi in enumerate(Phi_lags, start=1):
        Acomp[0:r, (lag - 1) * r : lag * r] = Phi

    # Shift identity for the lag stack
    for k in range(1, int(ppC)):
        Acomp[k * r : (k + 1) * r, (k - 1) * r : k * r] = np.eye(r, dtype=float)

    return Acomp


def spectral_radius(A: np.ndarray) -> float:
    """Return max |eigenvalue|."""
    ev = np.linalg.eigvals(A)
    return float(np.max(np.abs(ev)))


def enforce_var_stability(
    Phi_lags: Sequence[np.ndarray],
    *,
    r: Optional[int] = None,
    p: Optional[int] = None,
    ppC: int,
    shrink: float = 0.98,
    radius_target: float = 0.999,
    max_iter: int = 200,
) -> list[np.ndarray]:
    """
    Shrink VAR lag matrices until the ppC-companion matrix is stable.

    Strategy: multiply all lag matrices by `shrink` repeatedly until
    spectral_radius(companion) <= radius_target.

    Parameters r,p are optional and only used for sanity checks when provided.
    """
    Phi = [np.asarray(A, dtype=float).copy() for A in Phi_lags]
    if len(Phi) == 0:
        return Phi

    if r is not None:
        rr = int(r)
        for A in Phi:
            if A.shape != (rr, rr):
                raise ValueError("Provided r is inconsistent with Phi_lags shapes")

    if p is not None and int(p) != len(Phi):
        raise ValueError("Provided p is inconsistent with len(Phi_lags)")

    shrink = float(shrink)
    if not (0.0 < shrink < 1.0):
        raise ValueError("shrink must be in (0,1)")

    for _ in range(int(max_iter)):
        Acomp = _companion_from_var(Phi, ppC=int(ppC))
        rad = spectral_radius(Acomp)
        if np.isfinite(rad) and rad <= float(radius_target):
            return Phi
        for i in range(len(Phi)):
            Phi[i] *= shrink

    return Phi
