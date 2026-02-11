from __future__ import annotations

from typing import Any

import numpy as np

from .spec import BMDfmConfig, BMDfmResult
from .state_builder import BMParams


try:
    # Optional type (used only for warm-start caches in the fast path)
    from .fast.em_fast import EMStepCache
except Exception:  # pragma: no cover
    EMStepCache = Any  # type: ignore


def fit_bm_dfm(
    Y_monthly: np.ndarray,  # (T, nM) with NaNs
    y_quarterly: np.ndarray,  # (T,) with NaNs except quarter-end months
    config: BMDfmConfig,
    *,
    init_params: BMParams | None = None,
    em_cache: EMStepCache | None = None,
) -> BMDfmResult:
    """Public BM-DFM entrypoint.

    The original implementation in this repository diverged from the maintained
    fast implementation and became API-incompatible with the current EM/state
    builder code.

    This wrapper makes the stable fast implementation the single source of
    truth while preserving the historical `fit_bm_dfm(...)` call signature.
    """

    from .fast.fit_fast import fit_bm_dfm_fast

    return fit_bm_dfm_fast(
        Y_monthly=Y_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
    )
