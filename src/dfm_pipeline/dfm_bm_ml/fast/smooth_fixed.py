from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother
from dfm_pipeline.dfm_bm_ml.scaling import PanelScaler, scale_panel
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams, build_state_space


@dataclass(frozen=True)
class FixedSmoothResult:
    loglik: float
    a_smooth: np.ndarray
    P_smooth: np.ndarray
    P_lag_smooth: np.ndarray
    A: np.ndarray  # measurement (named A in your BM result objects)
    C: np.ndarray  # transition (named C in your BM result objects)


def smooth_bm_dfm_fixed_params(
    *,
    Y_monthly: np.ndarray,
    y_quarterly: np.ndarray,
    params: BMParams,
    config: BMDfmConfig,
    scaler: Optional[PanelScaler] = None,
) -> Tuple[FixedSmoothResult, PanelScaler]:
    """
    Run Kalman filter/smoother with fixed BMParams (no EM / no parameter updates).
    Returns smoothed states + (transition, measurement) matrices in the same naming
    convention used by your BM fit results.
    """
    config.validate()

    Y_monthly = np.asarray(Y_monthly, float)
    y_quarterly = np.asarray(y_quarterly, float).reshape(-1)
    if Y_monthly.shape[0] != y_quarterly.shape[0]:
        raise ValueError("Y_monthly and y_quarterly must have same T.")

    Tn, nM = Y_monthly.shape
    nQ = int(config.n_quarterly)
    if nQ != 1:
        raise ValueError("Expected a single quarterly target (n_quarterly=1).")

    Y_raw = np.concatenate([Y_monthly, y_quarterly[:, None]], axis=1)

    scale_mode = str(config.scaling_mode)
    if scale_mode == "toolbox_vintage":
        scale_mode = "internal_per_run"

    if scaler is None:
        Y, scaler = scale_panel(Y_raw, mode=scale_mode)
    else:
        # external_frozen => transform is no-op
        Y = scaler.transform(Y_raw)

    P0_mode = str(getattr(config, "P0_mode", "diffuse"))

    Tm, Qm, C_meas, R_meas, a0, P0, _idx = build_state_space(
        params=params,
        nM=int(nM),
        nQ=int(nQ),
        r_by_block=tuple(int(x) for x in config.r_by_block),
        p=int(config.p),
        ppC=5,
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
        P0_mode=P0_mode,
        a0_override=None,
        P0_override=None,
    )

    ss = StateSpaceParams(T=Tm, Q=Qm, C=C_meas, R=R_meas, a0=a0, P0=P0)
    kres = kalman_filter_smoother(Y, ss)

    # Match your BM “result naming” convention:
    # res.C is transition, res.A is measurement (this is how your pipeline behaves today)
    out = FixedSmoothResult(
        loglik=float(kres.loglik),
        a_smooth=kres.a_smooth,
        P_smooth=kres.P_smooth,
        P_lag_smooth=kres.P_lag_smooth,
        A=C_meas,   # measurement
        C=Tm,       # transition
    )
    return out, scaler