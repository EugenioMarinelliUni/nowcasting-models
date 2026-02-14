from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .spec import BMDfmConfig
from .scaling import PanelScaler
from .state_builder import BMParams, StateIndex

try:
    from .fast.em_fast import EMStepCache
except Exception:  # pragma: no cover
    EMStepCache = Any  # type: ignore[assignment]


@dataclass
class BMDfmResult:
    """
    Unified result container for BM-DFM fits (fast and non-fast).

    Keep this minimal and stable: other code should only rely on these fields.
    """
    params: BMParams
    loglik_trace: list[float]

    # Core state outputs (present in your current implementation)
    a_smooth: Any
    P_smooth: Any
    P_lag_smooth: Any

    # Measurement / transition matrices (names used in your project)
    C: Any
    R: Any
    A: Any
    Q: Any

    # Metadata + optional speedups
    state_index: Optional[StateIndex] = None
    scaler: Optional[PanelScaler] = None
    config: Optional[BMDfmConfig] = None
    converged: bool = False
    em_cache: Optional[EMStepCache] = None


__all__ = [
    "BMDfmConfig",
    "PanelScaler",
    "BMParams",
    "StateIndex",
    "EMStepCache",
    "BMDfmResult",
]