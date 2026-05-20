from __future__ import annotations

from dataclasses import dataclass

VALID_WEIGHT_SCHEMES = {"beta", "exp_almon", "equal", "unrestricted"}
VALID_COMBINATIONS = {"mean", "median", "trimmed_mean", "inverse_rmse", "top_k"}


@dataclass
class MIDASConfig:
    predictors: list[str]

    n_monthly_lags: int = 6
    n_y_lags: int = 2
    min_train_rows: int = 36

    weight_scheme: str = "beta"
    combination: str = "mean"

    trimmed_alpha: float = 0.10
    top_k: int | None = None

    max_iter: int = 200
    tol: float = 1e-6
    n_starts: int = 4
    parameter_bound: float = 3.0
    fallback_weight_scheme: str = "equal"
    ridge_alpha: float = 0.0

    # New controls
    validation_tail_rows: int = 0
    moq_specific: bool = False
    warm_start: bool = True

    def validate(self) -> None:
        if not self.predictors:
            raise ValueError("predictors cannot be empty")

        if self.n_monthly_lags <= 0:
            raise ValueError("n_monthly_lags must be positive")

        if self.n_y_lags < 0:
            raise ValueError("n_y_lags cannot be negative")

        if self.min_train_rows <= 0:
            raise ValueError("min_train_rows must be positive")

        if self.weight_scheme not in VALID_WEIGHT_SCHEMES:
            raise ValueError(
                f"Invalid weight_scheme={self.weight_scheme!r}. "
                f"Expected one of {sorted(VALID_WEIGHT_SCHEMES)}"
            )

        if self.combination not in VALID_COMBINATIONS:
            raise ValueError(
                f"Invalid combination={self.combination!r}. "
                f"Expected one of {sorted(VALID_COMBINATIONS)}"
            )

        if not (0.0 <= self.trimmed_alpha < 0.5):
            raise ValueError("trimmed_alpha must be in [0, 0.5)")

        if self.combination == "top_k":
            if self.top_k is None or self.top_k <= 0:
                raise ValueError("top_k must be positive when combination='top_k'")

        if self.top_k is not None and self.top_k <= 0:
            raise ValueError("top_k must be positive when provided")

        if self.max_iter <= 0:
            raise ValueError("max_iter must be positive")

        if self.tol <= 0:
            raise ValueError("tol must be positive")

        if self.n_starts <= 0:
            raise ValueError("n_starts must be positive")

        if self.parameter_bound <= 0:
            raise ValueError("parameter_bound must be positive")

        if self.fallback_weight_scheme != "equal":
            raise ValueError("Only fallback_weight_scheme='equal' is currently supported")

        if self.ridge_alpha < 0:
            raise ValueError("ridge_alpha cannot be negative")

        if self.validation_tail_rows < 0:
            raise ValueError("validation_tail_rows cannot be negative")