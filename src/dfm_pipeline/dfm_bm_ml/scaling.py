from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np

ScalingMode = Literal["external_frozen", "internal_per_run", "toolbox_vintage"]


@dataclass
class PanelScaler:
    mode: ScalingMode
    mu: np.ndarray
    sd: np.ndarray

    def transform(self, Y: np.ndarray) -> np.ndarray:
        if self.mode == "external_frozen":
            return Y
        return (Y - self.mu) / self.sd

    def inverse_transform(self, Z: np.ndarray) -> np.ndarray:
        if self.mode == "external_frozen":
            return Z
        return Z * self.sd + self.mu


def _nanmean_std(Y: np.ndarray, min_std: float = 1e-12) -> tuple[np.ndarray, np.ndarray]:
    mu = np.nanmean(Y, axis=0)
    sd = np.nanstd(Y, axis=0, ddof=0)
    sd = np.maximum(sd, min_std)
    return mu, sd


def scale_panel(
    Y: np.ndarray,
    mode: ScalingMode,
    min_std: float = 1e-12,
) -> tuple[np.ndarray, PanelScaler]:
    """
    Standardize a 2D panel Y (T x N) under either:
      - external_frozen: no-op (assume already standardized)
      - internal_per_run/toolbox_vintage: compute nanmean/nanstd and standardize
    """
    if mode == "external_frozen":
        mu = np.zeros(Y.shape[1], dtype=float)
        sd = np.ones(Y.shape[1], dtype=float)
        return Y, PanelScaler(mode=mode, mu=mu, sd=sd)

    # toolbox_vintage is an alias of internal_per_run; keep mode label for bookkeeping.
    mu, sd = _nanmean_std(Y, min_std=min_std)
    Z = (Y - mu) / sd
    return Z, PanelScaler(mode=mode, mu=mu, sd=sd)