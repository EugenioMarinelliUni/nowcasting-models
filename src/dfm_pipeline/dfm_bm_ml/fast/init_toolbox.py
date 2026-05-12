from __future__ import annotations

from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd

from .init import init_params_pca as _legacy_init_params_pca


def _fill_series(values: np.ndarray, method: str) -> np.ndarray:
    s = pd.Series(np.asarray(values, dtype=float))

    if s.notna().sum() == 0:
        return np.zeros(len(s), dtype=float)

    if method == "toolbox_spline":
        try:
            s = s.interpolate(method="spline", order=3, limit_direction="both")
        except Exception:
            s = s.interpolate(method="linear", limit_direction="both")
    elif method == "linear_interp":
        s = s.interpolate(method="linear", limit_direction="both")
    else:
        raise ValueError(f"Unsupported fill method: {method!r}")

    s = s.ffill().bfill()
    if s.isna().any():
        s = s.fillna(float(s.mean()))
    return s.to_numpy(dtype=float)


def prepare_init_panel(Y: np.ndarray, nM: int, method: str) -> np.ndarray:
    """
    Prepare a dense panel for PCA-style initialization while leaving the main EM/Kalman
    pipeline unchanged. This is intentionally conservative: we only change the data used
    to initialize the factors/parameters.
    """
    Y = np.asarray(Y, dtype=float)
    out = np.array(Y, copy=True)

    for j in range(out.shape[1]):
        out[:, j] = _fill_series(out[:, j], method=method)

    # Recentre any perfectly constant columns after fill to avoid singular PCA artifacts.
    col_std = np.nanstd(out, axis=0)
    constant = col_std <= 1e-12
    if np.any(constant):
        out[:, constant] = 0.0
    return out


def init_params_pca_toolbox(Y: np.ndarray, nM: int, config: Any):
    method = str(getattr(config, "init_missing_method", "toolbox_spline"))

    if method == "legacy_mean":
        cfg = replace(config, pca_fill="mean") if hasattr(config, "__dataclass_fields__") else config
        return _legacy_init_params_pca(Y, nM=nM, config=cfg)
    if method == "legacy_ffill":
        cfg = replace(config, pca_fill="ffill") if hasattr(config, "__dataclass_fields__") else config
        return _legacy_init_params_pca(Y, nM=nM, config=cfg)

    Y_init = prepare_init_panel(Y, nM=nM, method=method)
    cfg = replace(config, pca_fill="mean") if hasattr(config, "__dataclass_fields__") else config
    return _legacy_init_params_pca(Y_init, nM=nM, config=cfg)


__all__ = ["prepare_init_panel", "init_params_pca_toolbox"]
