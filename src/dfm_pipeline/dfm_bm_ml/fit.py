from __future__ import annotations

from typing import Any, Optional

import numpy as np

from dfm_pipeline.dfm_bm_ml.types import BMDfmConfig, BMParams, BMDfmResult


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
    Routes to fast implementation.
    """
    return fit_bm_dfm_fast(
        Y_monthly=X_monthly,
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
    # alias for backward compatibility
    X_monthly: Optional[np.ndarray] = None,
    # warm start (support both names)
    init_params: Optional[BMParams] = None,
    params_init: Optional[BMParams] = None,
    em_cache: Any = None,
    verbose: bool = False,
) -> BMDfmResult:
    """
    Fast BM-DFM fitter wrapper.
    Accepts Y_monthly or X_monthly (alias). Warm-start via init_params/params_init.
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

    return _fit(
        Y_monthly=Y_monthly,
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
    # alias for backward compatibility
    X_monthly: Optional[np.ndarray] = None,
    # warm start (support both names)
    init_params: Optional[BMParams] = None,
    params_init: Optional[BMParams] = None,
    em_cache: Any = None,
    verbose: bool = False,
) -> BMDfmResult:
    """
    Numba BM-DFM fitter wrapper.
    Accepts Y_monthly or X_monthly (alias). Warm-start via init_params/params_init.
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

    return _fit(
        Y_monthly=Y_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
        verbose=verbose,
    )
