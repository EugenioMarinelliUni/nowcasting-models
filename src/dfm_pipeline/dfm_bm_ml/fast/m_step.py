"""Fast EM M-step wrapper.

The fast ML-EM implementation keeps the full EM update logic in
`em_fast.em_step_ml_fast`. Older versions of the code used a separate
`m_step` module; the fitter still imports it.

This thin wrapper keeps the public API stable and prevents import errors.
"""

from __future__ import annotations

from dfm_pipeline.dfm_bm_ml.fast.em_fast import EMStepCache, em_step_ml_fast
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig


def m_step(
    *,
    config: BMDfmConfig,
    Y: "object",
    params: BMParams,
    em_cache: EMStepCache | None = None,
    ss: "object" = None,
) -> tuple[BMParams, float, EMStepCache]:
    loglik, new_params, _a_s, _P_s, _P_lag_s, cache = em_step_ml_fast(
        Y=Y,
        params=params,
        nM=int(config.n_monthly),
        nQ=int(config.n_quarterly),
        r_by_block=tuple(config.r_by_block),
        p=int(config.p),
        ppC=int(config.ppC),
        mm_style=str(config.mm_style),
        min_var=float(config.min_var),
        jitter=float(config.jitter),
        monthly_meas_var_floor=float(config.monthly_meas_var_floor),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        fix_quarterly_R=bool(config.fix_quarterly_R),
        force_var_stability=bool(config.force_var_stability),
        var_stability_shrink=float(config.var_stability_shrink),
        P0_mode=str(config.P0_mode),
        cache=em_cache,
    )
    return new_params, float(loglik), cache