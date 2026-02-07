from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np


ScalingMode = Literal["external_frozen", "internal_per_run"]


@dataclass(frozen=True)
class PanelScaler:
    """
    Column-wise scaler for a panel.

    Conventions:
    - internal_per_run: Ys = (Y - mu) / sd, with mu/sd estimated on current Y (ignoring NaNs).
    - external_frozen: Ys = Y, mu=0, sd=1 (identity; caller guarantees Y already scaled).
    """
    mode: ScalingMode
    mu: np.ndarray  # (n,)
    sd: np.ndarray  # (n,)

    def transform(self, Y: np.ndarray) -> np.ndarray:
        Y = np.asarray(Y, dtype=float)
        if self.mode == "external_frozen":
            return Y
        return (Y - self.mu) / self.sd

    def inverse_transform(self, Ys: np.ndarray) -> np.ndarray:
        Ys = np.asarray(Ys, dtype=float)
        if self.mode == "external_frozen":
            return Ys
        return Ys * self.sd + self.mu


def scale_panel(Y: np.ndarray, mode: ScalingMode) -> tuple[np.ndarray, PanelScaler]:
    """
    Scale a panel according to `mode` and return (Ys, scaler).

    Notes:
    - NaNs are ignored when estimating mu/sd.
    - For columns with all-NaN or zero variance, uses mu=0 and sd=1.
    """
    Y = np.asarray(Y, dtype=float)

    n = int(Y.shape[1])

    if mode == "external_frozen":
        mu = np.zeros((n,), dtype=float)
        sd = np.ones((n,), dtype=float)
        return Y, PanelScaler(mode=mode, mu=mu, sd=sd)

    if mode == "internal_per_run":
        mu = np.nanmean(Y, axis=0)
        sd = np.nanstd(Y, axis=0, ddof=0)

        bad_mu = ~np.isfinite(mu)
        bad_sd = (~np.isfinite(sd)) | (sd == 0.0)

        if np.any(bad_mu):
            mu = mu.copy()
            mu[bad_mu] = 0.0
        if np.any(bad_sd):
            sd = sd.copy()
            sd[bad_sd] = 1.0

        Ys = (Y - mu) / sd
        return Ys, PanelScaler(mode=mode, mu=mu.astype(float), sd=sd.astype(float))

    raise ValueError(f"Unknown scaling mode: {mode!r}")
