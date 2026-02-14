from __future__ import annotations

from dfm_pipeline.dfm_bm_ml.fast.fit_fast import fit_bm_dfm_fast
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.types import BMDfmResult, BMParams, EMStepCache


def fit_bm_dfm(
    X_monthly,
    y_quarterly,
    config: BMDfmConfig,
    init_params: BMParams | None = None,
    em_cache: EMStepCache | None = None,
) -> BMDfmResult:
    """
    Default BM-DFM fitter entrypoint.

    Currently routes to the fast implementation.
    """
    return fit_bm_dfm_fast(
        X_monthly=X_monthly,
        y_quarterly=y_quarterly,
        config=config,
        init_params=init_params,
        em_cache=em_cache,
    )