from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Scaler:
    """Column-wise frozen scaler."""
    mu: pd.Series
    sigma: pd.Series

    @staticmethod
    def from_training_panel(train_raw: pd.DataFrame, eps: float = 1e-12) -> "Scaler":
        """
        Compute μ, σ using ONLY the training window.
        NaNs are ignored (np.nanmean / nanstd with ddof=0).
        σ==0 is set to 1 to avoid division by zero.
        """
        mu = pd.Series(np.nanmean(train_raw.values, axis=0), index=train_raw.columns)
        sd = pd.Series(np.nanstd(train_raw.values, axis=0, ddof=0), index=train_raw.columns)
        sd = sd.where(sd > eps, 1.0)
        return Scaler(mu=mu, sigma=sd)

    def apply(self, X: pd.DataFrame, align: bool = True) -> pd.DataFrame:
        """
        Apply z = (X - μ) / σ. If align=True, reindex columns to the scaler's index
        (order and subset controlled by scaler).
        """
        if align:
            X = X.reindex(columns=self.mu.index)

        # broadcast-safe transform
        Z = (X.values.astype(float) - self.mu.values[np.newaxis, :]) / self.sigma.values[np.newaxis, :]
        return pd.DataFrame(Z, index=X.index, columns=X.columns)

    def save_csv(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"mu": self.mu, "sigma": self.sigma})
        df.to_csv(path)

    @staticmethod
    def load_csv(path: Path) -> "Scaler":
        df = pd.read_csv(path, index_col=0)
        return Scaler(mu=df["mu"], sigma=df["sigma"])


def load_panel_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Panel not found: {path}")
    return pd.read_csv(path, index_col=0)


def compute_and_optionally_save_scaler(
    train_raw: Optional[pd.DataFrame],
    scaler_path: Optional[Path],
    eps: float = 1e-12,
) -> Optional[Scaler]:
    """
    If train_raw is provided, compute a scaler (and save if scaler_path is given).
    If train_raw is None, return None (caller may load an existing scaler).
    """
    if train_raw is None:
        return None
    sc = Scaler.from_training_panel(train_raw, eps=eps)
    if scaler_path is not None:
        sc.save_csv(scaler_path)
    return sc
