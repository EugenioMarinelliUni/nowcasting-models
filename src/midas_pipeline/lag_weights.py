from __future__ import annotations

import numpy as np


def beta_weights(K: int, a: float, b: float) -> np.ndarray:
    if K <= 0:
        raise ValueError("K must be positive")

    # Exclude endpoints 0 and 1 to avoid singularities in Beta-style weights
    j = np.arange(1, K + 1, dtype=float)
    x = j / (K + 1.0)

    # Extra protection against numerical edge cases
    eps = 1e-10
    x = np.clip(x, eps, 1.0 - eps)

    logw = (a - 1.0) * np.log(x) + (b - 1.0) * np.log1p(-x)

    if not np.isfinite(logw).all():
        return np.full(K, 1.0 / K)

    logw = logw - np.max(logw)
    w = np.exp(logw)

    s = w.sum()
    if not np.isfinite(s) or s <= 0.0:
        return np.full(K, 1.0 / K)

    return w / s