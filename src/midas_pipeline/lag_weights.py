from __future__ import annotations

import numpy as np


VALID_WEIGHT_SCHEMES = {"beta", "exp_almon", "equal", "unrestricted"}


def _normalize_weights(w: np.ndarray) -> np.ndarray:
    w = np.asarray(w, dtype=float)

    if w.ndim != 1:
        raise ValueError("weights must be one-dimensional")

    if len(w) == 0:
        raise ValueError("weights cannot be empty")

    if not np.isfinite(w).all():
        return np.full(len(w), 1.0 / len(w))

    s = float(w.sum())
    if not np.isfinite(s) or abs(s) <= 1e-15:
        return np.full(len(w), 1.0 / len(w))

    return w / s


def equal_weights(K: int) -> np.ndarray:
    if K <= 0:
        raise ValueError("K must be positive")
    return np.full(K, 1.0 / K)


def beta_weights(K: int, a: float, b: float) -> np.ndarray:
    """
    Numerically stable Beta lag weights.

    Uses interior grid points j/(K+1) to avoid singularities at 0 and 1.
    """
    if K <= 0:
        raise ValueError("K must be positive")

    if a <= 0 or b <= 0 or not np.isfinite(a) or not np.isfinite(b):
        return equal_weights(K)

    j = np.arange(1, K + 1, dtype=float)
    x = j / (K + 1.0)

    eps = 1e-10
    x = np.clip(x, eps, 1.0 - eps)

    logw = (a - 1.0) * np.log(x) + (b - 1.0) * np.log1p(-x)

    if not np.isfinite(logw).all():
        return equal_weights(K)

    logw = logw - np.max(logw)
    w = np.exp(logw)

    return _normalize_weights(w)


def exp_almon_weights(K: int, theta1: float, theta2: float) -> np.ndarray:
    """
    Exponential Almon MIDAS weights.

    w_j proportional to exp(theta1 * j + theta2 * j^2),
    normalized to sum to one.
    """
    if K <= 0:
        raise ValueError("K must be positive")

    j = np.arange(1, K + 1, dtype=float)
    z = theta1 * j + theta2 * (j**2)

    if not np.isfinite(z).all():
        return equal_weights(K)

    z = z - np.max(z)
    w = np.exp(z)

    return _normalize_weights(w)


def lag_weights(
    K: int,
    scheme: str,
    params: np.ndarray | list[float] | tuple[float, ...] | None = None,
) -> np.ndarray:
    scheme = scheme.lower()

    if scheme == "equal":
        return equal_weights(K)

    if scheme == "beta":
        if params is None or len(params) != 2:
            return equal_weights(K)
        a, b = float(params[0]), float(params[1])
        return beta_weights(K, a=a, b=b)

    if scheme == "exp_almon":
        if params is None or len(params) != 2:
            return equal_weights(K)
        theta1, theta2 = float(params[0]), float(params[1])
        return exp_almon_weights(K, theta1=theta1, theta2=theta2)

    if scheme == "unrestricted":
        raise ValueError("unrestricted MIDAS does not use parametric lag weights")

    raise ValueError(f"Unknown MIDAS weight scheme: {scheme}")