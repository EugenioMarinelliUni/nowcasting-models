"""State-space builders used by the fast BM-DFM code.

Two call patterns are supported:

1) Fitters call:
       ss = build_state_space(params=params, config=config)

2) Fast EM code calls:
       Tm, Qm, C, R, a0, P0, idx = build_state_space(
           params=..., nM=..., nQ=..., r_by_block=..., p=..., ppC=..., mm_style=...,
           quarterly_meas_var_floor=..., idio_ar1=..., jitter=..., P0_mode=...,
           a0_override=..., P0_override=...
       )

Internally we delegate to dfm_pipeline.dfm_bm_ml.state_builder.build_state_space,
and adapt outputs/order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple

import numpy as np

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams
from dfm_pipeline.dfm_bm_ml.state_builder import build_state_space as _build_ss_core


@dataclass
class BMStateSpace:
    Tm: np.ndarray
    Qm: np.ndarray
    C: np.ndarray
    R: np.ndarray
    a0: np.ndarray
    P0: np.ndarray
    idx: Any
    Z: Optional[np.ndarray] = None

    def __iter__(self):
        yield self.Tm
        yield self.Qm
        yield self.C
        yield self.R
        yield self.a0
        yield self.P0
        yield self.idx


def build_state_space(
    *,
    params: BMParams,
    config: Optional[BMDfmConfig] = None,
    blocks: Optional[Sequence[np.ndarray]] = None,
    nM: Optional[int] = None,
    nQ: Optional[int] = None,
    r_by_block: Optional[Tuple[int, ...]] = None,
    p: Optional[int] = None,
    ppC: int = 5,
    mm_style: str = "toolbox",
    quarterly_meas_var_floor: float = 1e-4,
    idio_ar1: bool = True,
    jitter: float = 1e-8,
    P0_mode: str = "steady_state",
    a0_override: Optional[np.ndarray] = None,
    P0_override: Optional[np.ndarray] = None,
) -> BMStateSpace:
    if config is None:
        if nM is None or nQ is None or r_by_block is None or p is None:
            raise TypeError("build_state_space: provide either config=... or (nM,nQ,r_by_block,p,...)")
        config = BMDfmConfig(
            r_by_block=tuple(r_by_block),
            p=int(p),
            n_monthly=int(nM),
            n_quarterly=int(nQ),
            ppC=int(ppC),
            idio_ar1=bool(idio_ar1),
            mm_style=str(mm_style),
            P0_mode=str(P0_mode),
        )

    C, Tm, Qm, R, Z, a0, P0, idx = _build_ss_core(
        params=params,
        config=config,
        blocks=blocks,
        quarterly_meas_var_floor=float(quarterly_meas_var_floor),
        idio_ar1=bool(idio_ar1),
        jitter=float(jitter),
        P0_mode=str(P0_mode),
        a0_override=a0_override,
        P0_override=P0_override,
    )
    return BMStateSpace(Tm=Tm, Qm=Qm, C=C, R=R, a0=a0, P0=P0, idx=idx, Z=Z)