from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np

from ..state_builder import BMParams, StateIndex, build_state_space as _build_state_space
from ..spec import BMDfmConfig
from .em_fast import EMStepCache, build_em_cache

__all__ = [
    "BMParams",
    "StateIndex",
    "EMStepCache",
    "build_em_cache",
    "build_state_space",
    "build_state_space_from_config",
]


def build_state_space(
    *,
    params: BMParams,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    p: int,
    ppC: int,
    mm_style: str,
    quarterly_meas_var_floor: float,
    idio_ar1: bool,
    jitter: float,
    P0_mode: str = "diffuse",
    a0_override: Optional[np.ndarray] = None,
    P0_override: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StateIndex]:
    """
    Backward-compatible re-export of the canonical builder in dfm_bm_ml.state_builder.
    """
    return _build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=r_by_block,
        p=p,
        ppC=ppC,
        mm_style=mm_style,
        quarterly_meas_var_floor=quarterly_meas_var_floor,
        idio_ar1=idio_ar1,
        jitter=jitter,
        P0_mode=P0_mode,
        a0_override=a0_override,
        P0_override=P0_override,
    )


def build_state_space_from_config(
    *,
    params: BMParams,
    nM: int,
    config: BMDfmConfig,
    a0_override: Optional[np.ndarray] = None,
    P0_override: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StateIndex]:
    """
    Convenience wrapper to build the state space using BMDfmConfig fields.
    """
    config.validate()
    nQ = int(config.n_quarterly)
    return build_state_space(
        params=params,
        nM=nM,
        nQ=nQ,
        r_by_block=tuple(int(x) for x in config.r_by_block),
        p=int(config.p),
        ppC=5,
        mm_style=str(config.mm_weight_style),
        quarterly_meas_var_floor=float(config.quarterly_meas_var_floor),
        idio_ar1=bool(config.idio_ar1),
        jitter=float(config.jitter),
        P0_mode=str(getattr(config, "P0_mode", "diffuse")),
        a0_override=a0_override,
        P0_override=P0_override,
    )
