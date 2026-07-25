from __future__ import annotations

from typing import Optional

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

from .em_fast import EMStepCache, build_em_cache, em_step_ml_fast
from .init import init_params_pca as init_params_pca_legacy
from .init_toolbox import init_params_pca_toolbox
from ..fit_core import run_em_loop
from ..identification import identify_signs
from ..scaling import scale_panel
from ..spec import BMDfmConfig
from ..state_builder import BMParams, build_state_space
from ..types import BMDfmResult


def _as_blocks_array(blocks: Optional[list[int]], nM: int) -> Optional[np.ndarray]:
    if blocks is None:
        return None
    arr = np.asarray(blocks, dtype=int)
    if arr.ndim != 1 or arr.shape[0] != nM:
        raise ValueError("config.blocks must be a 1D list/array of length n_monthly.")
    return arr


def _initialize_params(Y: np.ndarray, nM: int, config: BMDfmConfig) -> BMParams:
    method = str(getattr(config, "init_missing_method", "legacy_mean"))
    if method in {"toolbox_spline", "linear_interp", "legacy_mean", "legacy_ffill"}:
        return init_params_pca_toolbox(Y, nM=nM, config=config)
    return init_params_pca_legacy(Y, nM=nM, config=config)


def _kalman_R(R: np.ndarray) -> np.ndarray:
    R_arr = np.asarray(R, dtype=float)
    if R_arr.ndim == 1:
        return R_arr.copy()
    if R_arr.ndim != 2 or R_arr.shape[0] != R_arr.shape[1]:
        raise ValueError(f"R must be a vector or square matrix, got {R_arr.shape}.")
    diag = np.diag(R_arr).copy()
    return diag if np.allclose(R_arr, np.diag(diag)) else R_arr.copy()


def _smooth_with_final_parameters(
    Y: np.ndarray,
    *,
    A: np.ndarray,
    Q: np.ndarray,
    C: np.ndarray,
    R: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
):
    ss = StateSpaceParams(
        T=np.asarray(A, dtype=float),
        Q=np.asarray(Q, dtype=float),
        C=np.asarray(C, dtype=float),
        R=_kalman_R(R),
        a0=np.asarray(a0, dtype=float),
        P0=np.asarray(P0, dtype=float),
    )
    return kalman_filter_smoother(np.asarray(Y, dtype=float), ss)


def fit_bm_dfm_fast(
    Y_monthly: Optional[np.ndarray] = None,
    y_quarterly: Optional[np.ndarray] = None,
    config: Optional[BMDfmConfig] = None,
    *,
    X_monthly: Optional[np.ndarray] = None,
    init_params: Optional[BMParams] = None,
    em_cache: Optional[EMStepCache] = None,
    verbose: bool = False,
) -> BMDfmResult:
    if config is None:
        raise ValueError("config is required.")
    config.validate()

    if Y_monthly is None:
        Y_monthly = X_monthly
    if Y_monthly is None:
        raise ValueError("Y_monthly (or X_monthly) is required.")
    if y_quarterly is None:
        raise ValueError("y_quarterly is required.")

    Y_monthly = np.asarray(Y_monthly, dtype=float)
    y_quarterly = np.asarray(y_quarterly, dtype=float).reshape(-1)
    if Y_monthly.ndim != 2:
        raise ValueError("Y_monthly must be 2D (T, nM).")
    if y_quarterly.ndim != 1:
        raise ValueError("y_quarterly must be 1D (T,).")
    if Y_monthly.shape[0] != y_quarterly.shape[0]:
        raise ValueError("Y_monthly and y_quarterly must share the same T index.")

    _Tn, nM = Y_monthly.shape
    nQ = int(config.n_quarterly)
    Y_raw = np.concatenate([Y_monthly, y_quarterly[:, None]], axis=1)
    scale_mode = "internal_per_run" if config.scaling_mode == "toolbox_vintage" else config.scaling_mode
    Y, scaler = scale_panel(Y_raw, mode=scale_mode)

    blocks_arr = _as_blocks_array(config.blocks, nM)
    if em_cache is None:
        em_cache = build_em_cache(
            nM=nM,
            nQ=nQ,
            r_by_block=tuple(int(x) for x in config.r_by_block),
            blocks=blocks_arr,
            enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        )

    params0 = init_params if init_params is not None else _initialize_params(Y, nM=nM, config=config)

    em_kwargs = dict(
        nM=nM,
        nQ=nQ,
        r_by_block=tuple(int(x) for x in config.r_by_block),
        p=int(config.p),
        ppC=int(config.ppC),
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        monthly_meas_var_floor=float(config.monthly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        force_var_stability=bool(config.force_var_stability),
        var_stability_shrink=float(config.var_stability_shrink),
        P0_mode=str(config.P0_mode),
        min_var=float(config.min_var),
        jitter=float(config.jitter),
        enforce_q_loading_constraint=bool(config.enforce_quarterly_loading_constraint),
        fix_quarterly_R=bool(config.fix_quarterly_R),
        blocks=blocks_arr,
        cache=em_cache,
    )
    build_kwargs = dict(
        nM=nM,
        nQ=nQ,
        r_by_block=tuple(int(x) for x in config.r_by_block),
        p=int(config.p),
        ppC=int(config.ppC),
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
        P0_mode=str(config.P0_mode),
    )

    em_out = run_em_loop(
        Y=Y,
        initial_params=params0,
        config=config,
        em_step=em_step_ml_fast,
        em_kwargs=em_kwargs,
        build_kwargs=build_kwargs,
        description="BM-DFM projected GEM (fast)",
        verbose=verbose,
    )
    params = em_out.params

    identification_info = None
    if str(config.identification_mode) == "sign_anchor":
        params, identification_info = identify_signs(
            params,
            r_by_block=tuple(int(x) for x in config.r_by_block),
            anchor_indices=config.identification_anchor_indices,
        )

    A, Q, C, R, a0, P0, state_index = build_state_space(
        params=params,
        a0_override=em_out.a0_override,
        P0_override=em_out.P0_override,
        **build_kwargs,
    )
    final_smooth = _smooth_with_final_parameters(
        Y, A=A, Q=Q, C=C, R=R, a0=a0, P0=P0
    )

    diagnostics = dict(em_out.diagnostics)
    diagnostics["final_recomputed_loglik"] = float(getattr(final_smooth, "loglik", diagnostics["final_loglik"]))
    diagnostics["identification_mode"] = str(config.identification_mode)
    if identification_info is not None:
        diagnostics["identification_signs"] = identification_info.signs.tolist()
        diagnostics["identification_anchor_indices"] = identification_info.anchor_indices.tolist()

    return BMDfmResult(
        params=params,
        loglik_trace=em_out.loglik_trace,
        a_smooth=final_smooth.a_smooth,
        P_smooth=final_smooth.P_smooth,
        P_lag_smooth=final_smooth.P_lag_smooth,
        C=C,
        R=R,
        A=A,
        Q=Q,
        a0=a0,
        P0=P0,
        state_index=state_index,
        scaler=scaler,
        config=config,
        converged=em_out.converged,
        em_cache=em_cache,
        diagnostics=diagnostics,
    )
