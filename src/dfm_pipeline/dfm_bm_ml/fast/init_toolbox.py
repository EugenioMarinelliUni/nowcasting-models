from __future__ import annotations

from dataclasses import replace
from typing import Any, Optional

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
    """Prepare monthly predictors for PCA initialization without densifying GDP.

    Only the first ``nM`` columns are monthly indicators and may be interpolated for
    factor extraction. Quarterly target columns remain exactly as observed (including
    their structural non-quarter-end NaNs), so the initial quarterly regression uses
    genuine quarterly observations only. The EM/Kalman likelihood always receives the
    original sparse panel.
    """
    Y = np.asarray(Y, dtype=float)
    if Y.ndim != 2:
        raise ValueError("Y must be a 2D panel.")
    nM = int(nM)
    if nM < 0 or nM > Y.shape[1]:
        raise ValueError(f"nM must lie in [0, {Y.shape[1]}], got {nM}.")

    out = np.array(Y, copy=True)
    for j in range(nM):
        out[:, j] = _fill_series(out[:, j], method=method)

    # Constant monthly predictors cannot contribute to PCA. Never alter the sparse
    # quarterly target columns here.
    if nM:
        col_std = np.nanstd(out[:, :nM], axis=0)
        constant = col_std <= 1e-12
        if np.any(constant):
            out[:, np.where(constant)[0]] = 0.0
    return out


def init_params_pca_toolbox(
    Y: np.ndarray,
    nM: int,
    config: Any,
    *,
    blocks: Optional[np.ndarray] = None,
):
    method = str(getattr(config, "init_missing_method", "toolbox_spline"))

    if method == "legacy_mean":
        cfg = replace(config, pca_fill="mean") if hasattr(config, "__dataclass_fields__") else config
        return _legacy_init_params_pca(Y, nM=nM, config=cfg, blocks=blocks)
    if method == "legacy_ffill":
        cfg = replace(config, pca_fill="ffill") if hasattr(config, "__dataclass_fields__") else config
        return _legacy_init_params_pca(Y, nM=nM, config=cfg, blocks=blocks)

    Y_init = prepare_init_panel(Y, nM=nM, method=method)
    cfg = replace(config, pca_fill="mean") if hasattr(config, "__dataclass_fields__") else config
    return _legacy_init_params_pca(Y_init, nM=nM, config=cfg, blocks=blocks)


__all__ = ["prepare_init_panel", "init_params_pca_toolbox"]
