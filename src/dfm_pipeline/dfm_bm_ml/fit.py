from __future__ import annotations

from typing import Any, Optional
import inspect

import numpy as np

from dfm_pipeline.dfm_bm_ml.types import BMDfmConfig, BMParams, BMDfmResult


def _call_supported(fn, **kwargs):
    """
    Call fn with only the kwargs it supports (by signature).
    Prevents 'unexpected keyword argument' errors across versions.
    """
    sig = inspect.signature(fn)
    filt = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return fn(**filt)


def fit_bm_dfm(
    X_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    config: BMDfmConfig,
    *,
    init_params: Optional[BMParams] = None,
    em_cache: Any = None,
    verbose: bool = False,
) -> BMDfmResult:
    """
    Public entrypoint (backward compatible).
    Routes to the fast implementation.
    """
    return fit_bm_dfm_fast(
        X_monthly=X_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
        verbose=verbose,
    )


def fit_bm_dfm_fast(
    Y_monthly: Optional[np.ndarray] = None,
    y_quarterly: Optional[np.ndarray] = None,
    config: Optional[BMDfmConfig] = None,
    *,
    X_monthly: Optional[np.ndarray] = None,          # alias
    init_params: Optional[BMParams] = None,
    params_init: Optional[BMParams] = None,          # alias
    em_cache: Any = None,
    verbose: bool = False,
) -> BMDfmResult:
    """
    Fast BM-DFM fitter wrapper.

    Accepts:
      - Y_monthly or X_monthly (alias)
      - init_params or params_init (alias)
    """
    if Y_monthly is None:
        Y_monthly = X_monthly
    if Y_monthly is None:
        raise ValueError("Must provide Y_monthly (or X_monthly alias).")
    if y_quarterly is None:
        raise ValueError("Must provide y_quarterly.")
    if config is None:
        raise ValueError("Must provide config.")

    if init_params is None and params_init is not None:
        init_params = params_init

    from dfm_pipeline.dfm_bm_ml.fast.fit_fast import fit_bm_dfm_fast as _fit

    # Pass both Y_monthly and X_monthly; callee will take what it supports.
    return _call_supported(
        _fit,
        Y_monthly=Y_monthly,
        X_monthly=Y_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
        verbose=verbose,
    )


def fit_bm_dfm_fast_numba(
    Y_monthly: Optional[np.ndarray] = None,
    y_quarterly: Optional[np.ndarray] = None,
    config: Optional[BMDfmConfig] = None,
    *,
    X_monthly: Optional[np.ndarray] = None,          # alias
    init_params: Optional[BMParams] = None,
    params_init: Optional[BMParams] = None,          # alias
    em_cache: Any = None,
    verbose: bool = False,
) -> BMDfmResult:
    """
    Numba BM-DFM fitter wrapper.

    Accepts:
      - Y_monthly or X_monthly (alias)
      - init_params or params_init (alias)
    """
    if Y_monthly is None:
        Y_monthly = X_monthly
    if Y_monthly is None:
        raise ValueError("Must provide Y_monthly (or X_monthly alias).")
    if y_quarterly is None:
        raise ValueError("Must provide y_quarterly.")
    if config is None:
        raise ValueError("Must provide config.")

    if init_params is None and params_init is not None:
        init_params = params_init

    from dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba import fit_bm_dfm_fast_numba as _fit

    return _call_supported(
        _fit,
        Y_monthly=Y_monthly,
        X_monthly=Y_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
        verbose=verbose,
    )
