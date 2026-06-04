from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def normalize_max_features(value: Any) -> str | float | int | None:
    """Normalize max_features for sklearn and quantile-forest estimators.

    Accepted inputs:
    - None, "none", "null" -> None
    - "sqrt" or "log2" -> string passthrough
    - positive integers -> int
    - floats in (0, 1] -> float fraction of features

    Whole-number floats greater than 1 are converted to integers. This keeps
    values such as 2.0 usable when they are produced by CSV/JSON parsing, while
    still rejecting invalid non-integer floats greater than 1.
    """
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        lower = text.lower()

        if lower in {"none", "null", ""}:
            return None

        if lower in {"sqrt", "log2"}:
            return lower

        try:
            if any(ch in lower for ch in (".", "e")):
                parsed = float(lower)
            else:
                parsed = int(lower)
        except ValueError as exc:
            raise ValueError(
                "max_features must be one of: sqrt, log2, None, "
                "a positive integer, or a float in (0, 1]."
            ) from exc

        return normalize_max_features(parsed)

    if isinstance(value, bool):
        raise ValueError("max_features cannot be boolean")

    if isinstance(value, int):
        if value <= 0:
            raise ValueError("integer max_features must be positive")
        return int(value)

    if isinstance(value, float):
        if not (value > 0.0):
            raise ValueError("float max_features must be positive")

        if value <= 1.0:
            return float(value)

        if value.is_integer():
            return int(value)

        raise ValueError(
            "float max_features greater than 1 is invalid unless it is a whole-number "
            "integer value. Use an int like 2 or a fraction in (0, 1]."
        )

    raise TypeError(f"Unsupported max_features type: {type(value).__name__}")


def max_features_label(value: Any) -> str:
    """Stable label for output directory names."""
    value = normalize_max_features(value)

    if value is None:
        return "none"

    if isinstance(value, str):
        return value

    if isinstance(value, int):
        return f"mf{value}"

    text = f"{float(value):g}"
    return "mf" + text.replace(".", "")


@dataclass
class QRFConfig:
    predictors: list[str]
    n_lags: int = 3
    n_y_lags: int = 2
    min_train_rows: int = 36

    n_estimators: int = 500
    min_samples_leaf: int = 10
    max_features: str | float | int | None = "sqrt"
    random_state: int = 0
    n_jobs: int | None = -1

    # "rf_point" = point RandomForestRegressor benchmark
    # "qrf" = true Quantile Regression Forest backend
    backend: str = "rf_point"

    # Used when backend == "qrf"
    quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)

    # If True, the pseudo-RT runner stores feature-importance diagnostics
    # in pred_df.attrs["feature_importance"].
    save_feature_importance: bool = True

    def __post_init__(self) -> None:
        self.max_features = normalize_max_features(self.max_features)

        self.n_lags = int(self.n_lags)
        self.n_y_lags = int(self.n_y_lags)
        self.min_train_rows = int(self.min_train_rows)
        self.n_estimators = int(self.n_estimators)
        self.min_samples_leaf = int(self.min_samples_leaf)
        self.random_state = int(self.random_state)

        if self.n_lags <= 0:
            raise ValueError("n_lags must be positive")
        if self.n_y_lags < 0:
            raise ValueError("n_y_lags cannot be negative")
        if self.min_train_rows <= 0:
            raise ValueError("min_train_rows must be positive")
        if self.n_estimators <= 0:
            raise ValueError("n_estimators must be positive")
        if self.min_samples_leaf <= 0:
            raise ValueError("min_samples_leaf must be positive")

        if self.n_jobs is not None:
            self.n_jobs = int(self.n_jobs)

        self.backend = str(self.backend)
        if self.backend not in {"rf_point", "qrf"}:
            raise ValueError("backend must be either 'rf_point' or 'qrf'")

        self.quantiles = tuple(float(q) for q in self.quantiles)
        if not self.quantiles:
            raise ValueError("quantiles cannot be empty")
        if any(q <= 0.0 or q >= 1.0 for q in self.quantiles):
            raise ValueError("quantiles must be strictly between 0 and 1")
        if tuple(sorted(self.quantiles)) != self.quantiles:
            raise ValueError("quantiles must be sorted increasingly")
