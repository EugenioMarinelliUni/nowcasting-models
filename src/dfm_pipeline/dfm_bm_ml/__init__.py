"""Bańbura–Modugno (2014) mixed-frequency DFM (ML-EM)."""

from .spec import BMDfmConfig, BMDfmResult

# Stable public entrypoint (wrapper over the maintained fast implementation)
from .fit import fit_bm_dfm

# Explicit access to the fast implementations
from .fast.fit_fast import fit_bm_dfm_fast
from .fast.fit_fast_numba import fit_bm_dfm_fast_numba

__all__ = [
    "BMDfmConfig",
    "BMDfmResult",
    "fit_bm_dfm",
    "fit_bm_dfm_fast",
    "fit_bm_dfm_fast_numba",
]
