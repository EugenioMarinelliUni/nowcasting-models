from __future__ import annotations

import numpy as np


def beta_weights(K: int, a: float, b: float) -> np.ndarray:
    j = np.arange(1, K + 1, dtype=float)
    x = j / K
    w = (x ** (a - 1.0)) * ((1.0 - x) ** (b - 1.0))
    w = np.where(np.isfinite(w), w, 0.0)
    s = w.sum()
    if s <= 0:
        return np.full(K, 1.0 / K)
    return w / s