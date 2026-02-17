from __future__ import annotations

from .spec import BMDfmConfig

try:
    # Newer layout: result/typing artifacts live in types.py
    from .types import BMDfmResult  # type: ignore
except Exception:  # pragma: no cover
    # Backward compatible: BMDfmResult lives in spec.py
    from .spec import BMDfmResult  # type: ignore

from .fit import fit_bm_dfm
from .fast.fit_fast import fit_bm_dfm_fast
from .fast.fit_fast_numba import fit_bm_dfm_fast_numba

__all__ = [
    "BMDfmConfig",
    "BMDfmResult",
    "fit_bm_dfm",
    "fit_bm_dfm_fast",
    "fit_bm_dfm_fast_numba",
]
