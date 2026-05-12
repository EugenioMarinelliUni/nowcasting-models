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

    # Core state outputs
    a_smooth: Any
    P_smooth: Any
    P_lag_smooth: Any

    # State-space matrices. Canonical API:
    #   A: transition matrix, shape (n_state, n_state)
    #   Q: transition-innovation covariance, shape (n_state, n_state)
    #   C: measurement matrix, shape (n_observed, n_state)
    #   R: measurement-error covariance, shape (n_observed, n_observed)
    A: Any
    Q: Any
    C: Any
    R: Any
    a0: Any = None
    P0: Any = None

    # Metadata + optional speedups
    state_index: Optional[StateIndex] = None
    scaler: Optional[PanelScaler] = None
    config: Optional[BMDfmConfig] = None
    converged: bool = False
    em_cache: Optional[EMStepCache] = None

    @property
    def T(self) -> Any:
        """Alias for the transition matrix A."""
        return self.A

    @property
    def transition(self) -> Any:
        """Alias for the transition matrix A."""
        return self.A

    @property
    def Z(self) -> Any:
        """Alias for the measurement matrix C."""
        return self.C

    @property
    def measurement(self) -> Any:
        """Alias for the measurement matrix C."""
        return self.C


__all__ = [
    "BMDfmConfig",
    "PanelScaler",
    "BMParams",
    "StateIndex",
    "EMStepCache",
    "BMDfmResult",
]
