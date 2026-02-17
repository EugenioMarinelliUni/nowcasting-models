from __future__ import annotations

"""Deprecated module.

This repository's BM-DFM implementation performs the M-step inside the EM iteration
functions in em_fast_numba.py.

The previous standalone m_step_numba modules were part of an older API and are kept only
to avoid import-time crashes in downstream code. They are not used by the current
fitters.
"""


def __getattr__(name: str):
    raise AttributeError(
        f"dfm_bm_ml.fast.m_step_numba does not expose {name!r}. " 
        "Use dfm_bm_ml.fast.em_fast.em_step_numba_ml_fast (or the fit_bm_dfm_fast wrapper)."
    )
