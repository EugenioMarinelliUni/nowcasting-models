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

    def __post_init__(self) -> None:
        self.mu = np.asarray(self.mu, dtype=float).reshape(-1)
        self.sd = np.asarray(self.sd, dtype=float).reshape(-1)
        if self.mu.shape != self.sd.shape:
            raise ValueError("PanelScaler.mu and PanelScaler.sd must have the same shape.")
        if np.any(~np.isfinite(self.mu)) or np.any(~np.isfinite(self.sd)):
            raise ValueError("PanelScaler parameters must be finite.")
        if np.any(self.sd <= 0.0):
            raise ValueError("PanelScaler standard deviations must be strictly positive.")

    def _validate_panel(self, Y: np.ndarray) -> np.ndarray:
        Y = np.asarray(Y, dtype=float)
        if Y.ndim == 1:
            if Y.shape[0] != self.mu.shape[0]:
                raise ValueError(
                    f"Scaler expects {self.mu.shape[0]} variables, got vector shape {Y.shape}."
                )
        elif Y.ndim == 2:
            if Y.shape[1] != self.mu.shape[0]:
                raise ValueError(
                    f"Scaler expects {self.mu.shape[0]} variables, got panel shape {Y.shape}."
                )
        else:
            raise ValueError("PanelScaler accepts only 1D vectors or 2D panels.")
        return Y

    def transform(self, Y: np.ndarray) -> np.ndarray:
        Y = self._validate_panel(Y)
        if self.mode == "external_frozen":
            return Y.copy()
        return (Y - self.mu) / self.sd

    def inverse_transform(self, Z: np.ndarray) -> np.ndarray:
        Z = self._validate_panel(Z)
        if self.mode == "external_frozen":
            return Z.copy()
        return Z * self.sd + self.mu

    def column_location_scale(self, column: int) -> tuple[float, float]:
        """Return the location and scale used for one observed variable."""
        j = int(column)
        if j < 0:
            j += self.mu.shape[0]
        if j < 0 or j >= self.mu.shape[0]:
            raise IndexError(f"Scaler column {column} is out of bounds for {self.mu.shape[0]} variables.")
        return float(self.mu[j]), float(self.sd[j])


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

@dataclass(frozen=True)
class TargetOutputScaler:
    """Optional mapping from the target file's units to reported economic units.

    This is primarily useful with ``scaling_mode='external_frozen'`` where the
    input target is already a z-score.  The state-space ``PanelScaler`` then is
    intentionally the identity, while this object stores the frozen training
    mean and standard deviation needed to report GDP growth in original units.
    """

    mean: float = 0.0
    std: float = 1.0
    label: str = "input_units"

    def __post_init__(self) -> None:
        if not np.isfinite(float(self.mean)):
            raise ValueError("TargetOutputScaler.mean must be finite.")
        if not np.isfinite(float(self.std)) or float(self.std) <= 0.0:
            raise ValueError("TargetOutputScaler.std must be finite and strictly positive.")

    def to_output(self, value):
        arr = np.asarray(value, dtype=float)
        out = arr * float(self.std) + float(self.mean)
        return float(out) if out.ndim == 0 else out

    def from_output(self, value):
        arr = np.asarray(value, dtype=float)
        out = (arr - float(self.mean)) / float(self.std)
        return float(out) if out.ndim == 0 else out

    def scale_sd(self, value):
        arr = np.asarray(value, dtype=float) * float(self.std)
        return float(arr) if arr.ndim == 0 else arr

    @classmethod
    def from_json(cls, path: str) -> "TargetOutputScaler":
        import json
        from pathlib import Path

        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        mean = payload.get("mean", payload.get("mu", payload.get("target_mean")))
        std = payload.get("std", payload.get("sd", payload.get("target_std")))
        if mean is None or std is None:
            raise ValueError("Target scale JSON must contain mean/mu and std/sd.")
        return cls(mean=float(mean), std=float(std), label=str(payload.get("label", "output_units")))
