from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal

import numpy as np


ScalingMode = Literal["external_frozen", "internal_per_run"]


@dataclass(frozen=True)
class PanelScaler:
    """Column-wise scaler for a panel.

    Convention:
    - internal_per_run: Ys = (Y - mu) / sd
    - external_frozen: Ys = Y, mu=0, sd=1 (or user-provided values elsewhere)

    Notes
    -----
    - NaNs are ignored when estimating mu/sd.
    - sd is floored at 1.0 for zero-variance columns to avoid division by zero.
    """
    mode: ScalingMode
    mu: np.ndarray  # (n,)
    sd: np.ndarray  # (n,)


def scale_panel(
    Y: np.ndarray,
    mode: ScalingMode,
) -> tuple[np.ndarray, PanelScaler]:
    """Scale a panel according to `mode` and return (Ys, scaler)."""
    if mode == "external_frozen":
        mu = np.zeros((Y.shape[1],), dtype=float)
        sd = np.ones((Y.shape[1],), dtype=float)
        return Y.astype(float, copy=False), PanelScaler(mode=mode, mu=mu, sd=sd)

    if mode == "internal_per_run":
        mu = np.nanmean(Y, axis=0)
        sd = np.nanstd(Y, axis=0, ddof=0)
        sd = np.where(sd == 0.0, 1.0, sd)
        Ys = (Y - mu) / sd
        return Ys, PanelScaler(mode=mode, mu=mu.astype(float), sd=sd.astype(float))

    raise ValueError(f"Unknown scaling mode: {mode!r}")
