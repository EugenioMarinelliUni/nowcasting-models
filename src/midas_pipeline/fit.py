from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from .lag_weights import equal_weights, lag_weights
from .model import MIDASFitDiagnostics, MIDASFitResult, MIDASModel


def _xlag_columns(K: int) -> list[str]:
    return [f"xlag_{i}" for i in range(1, K + 1)]


def _ylag_columns(p: int) -> list[str]:
    return [f"ylag_{i}" for i in range(1, p + 1)]


def _prepare_xy(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_monthly_lags: int,
    n_y_lags: int,
) -> tuple[pd.DataFrame, pd.Series, list[str], list[str]]:
    x_cols = _xlag_columns(n_monthly_lags)
    y_cols = _ylag_columns(n_y_lags)

    required = x_cols + y_cols
    missing = [c for c in required if c not in X_train.columns]
    if missing:
        raise ValueError(f"Missing MIDAS feature columns: {missing}")

    X = X_train[required].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(y_train, errors="coerce")

    mask = np.isfinite(y.to_numpy(dtype=float))
    mask &= np.isfinite(X.to_numpy(dtype=float)).all(axis=1)

    X = X.loc[mask].reset_index(drop=True)
    y = y.loc[mask].reset_index(drop=True)

    if X.empty or y.empty:
        raise ValueError("No finite MIDAS training rows after sanitization")

    return X, y, x_cols, y_cols


def _ridge_lstsq(Z: np.ndarray, y: np.ndarray, ridge_alpha: float) -> np.ndarray:
    if ridge_alpha <= 0:
        beta, *_ = np.linalg.lstsq(Z, y, rcond=None)
        return beta

    eye = np.eye(Z.shape[1])
    eye[0, 0] = 0.0

    lhs = Z.T @ Z + ridge_alpha * eye
    rhs = Z.T @ y

    return np.linalg.solve(lhs, rhs)


def _fit_linear_design(
    Z: np.ndarray,
    y: np.ndarray,
    ridge_alpha: float,
) -> tuple[np.ndarray, float, float]:
    beta = _ridge_lstsq(Z, y, ridge_alpha=ridge_alpha)
    resid = y - Z @ beta
    sse = float(np.dot(resid, resid))
    rmse = float(np.sqrt(np.mean(resid**2)))
    return beta, sse, rmse


def _weighted_design(
    X: pd.DataFrame,
    weights: np.ndarray,
    x_cols: list[str],
    y_cols: list[str],
) -> np.ndarray:
    x_mat = X[x_cols].to_numpy(dtype=float)
    x_weighted = x_mat @ weights

    parts = [
        np.ones(len(X), dtype=float),
        x_weighted,
    ]

    for col in y_cols:
        parts.append(X[col].to_numpy(dtype=float))

    return np.column_stack(parts)


def _unrestricted_design(
    X: pd.DataFrame,
    x_cols: list[str],
    y_cols: list[str],
) -> np.ndarray:
    parts = [np.ones(len(X), dtype=float)]

    for col in x_cols + y_cols:
        parts.append(X[col].to_numpy(dtype=float))

    return np.column_stack(parts)


def _params_from_theta(scheme: str, theta: Sequence[float]) -> tuple[np.ndarray, dict[str, float]]:
    theta_arr = np.asarray(theta, dtype=float)

    if scheme == "beta":
        a = float(np.exp(theta_arr[0]))
        b = float(np.exp(theta_arr[1]))
        return np.array([a, b], dtype=float), {"a": a, "b": b}

    if scheme == "exp_almon":
        theta1 = float(theta_arr[0])
        theta2 = float(theta_arr[1])
        return np.array([theta1, theta2], dtype=float), {"theta1": theta1, "theta2": theta2}

    raise ValueError(f"Unsupported nonlinear scheme: {scheme}")


def _theta_from_params(scheme: str, params: dict[str, float] | None) -> np.ndarray | None:
    if not params:
        return None

    if scheme == "beta":
        if "a" not in params or "b" not in params:
            return None
        a = float(params["a"])
        b = float(params["b"])
        if a <= 0 or b <= 0:
            return None
        return np.log(np.array([a, b], dtype=float))

    if scheme == "exp_almon":
        if "theta1" not in params or "theta2" not in params:
            return None
        return np.array([float(params["theta1"]), float(params["theta2"])], dtype=float)

    return None


def _beta_starts(n_starts: int, initial_theta: np.ndarray | None) -> list[np.ndarray]:
    starts = []

    if initial_theta is not None and len(initial_theta) == 2 and np.isfinite(initial_theta).all():
        starts.append(np.asarray(initial_theta, dtype=float))

    starts.extend(
        [
            np.log(np.array([1.5, 1.5])),
            np.log(np.array([1.0, 1.0])),
            np.log(np.array([2.0, 1.0])),
            np.log(np.array([1.0, 2.0])),
            np.log(np.array([2.0, 2.0])),
        ]
    )

    return starts[: max(1, n_starts)]


def _exp_almon_starts(n_starts: int, initial_theta: np.ndarray | None) -> list[np.ndarray]:
    starts = []

    if initial_theta is not None and len(initial_theta) == 2 and np.isfinite(initial_theta).all():
        starts.append(np.asarray(initial_theta, dtype=float))

    starts.extend(
        [
            np.array([0.0, 0.0]),
            np.array([-0.10, 0.00]),
            np.array([0.10, 0.00]),
            np.array([0.00, -0.01]),
            np.array([0.05, -0.01]),
        ]
    )

    return starts[: max(1, n_starts)]


def _fit_equal_or_weighted(
    X: pd.DataFrame,
    y: pd.Series,
    predictor: str,
    requested_weight_scheme: str,
    actual_weight_scheme: str,
    weights: np.ndarray,
    weight_params: dict[str, float],
    n_monthly_lags: int,
    n_y_lags: int,
    x_cols: list[str],
    y_cols: list[str],
    ridge_alpha: float,
    used_fallback: bool,
    status_message: str,
) -> MIDASFitResult:
    y_arr = y.to_numpy(dtype=float)
    Z = _weighted_design(X, weights=weights, x_cols=x_cols, y_cols=y_cols)

    beta, sse, rmse = _fit_linear_design(Z, y_arr, ridge_alpha=ridge_alpha)

    model = MIDASModel(
        predictor=predictor,
        weight_scheme=actual_weight_scheme,
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
        beta=beta,
        lag_weights=weights,
        weight_params=weight_params,
        theta=None,
        is_unrestricted=False,
    )

    diagnostics = MIDASFitDiagnostics(
        predictor=predictor,
        requested_weight_scheme=requested_weight_scheme,
        actual_weight_scheme=actual_weight_scheme,
        converged=True,
        used_fallback=used_fallback,
        objective_value=sse,
        in_sample_rmse=rmse,
        n_iter=0,
        n_train_rows=int(len(y)),
        n_features=int(Z.shape[1]),
        status_message=status_message,
        weight_params=weight_params,
        best_theta=None,
        best_start_index=None,
        all_start_objectives=[],
    )

    return MIDASFitResult(model=model, diagnostics=diagnostics)


def _fit_unrestricted(
    X: pd.DataFrame,
    y: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
    x_cols: list[str],
    y_cols: list[str],
    ridge_alpha: float,
) -> MIDASFitResult:
    y_arr = y.to_numpy(dtype=float)
    Z = _unrestricted_design(X, x_cols=x_cols, y_cols=y_cols)

    beta, sse, rmse = _fit_linear_design(Z, y_arr, ridge_alpha=ridge_alpha)

    x_beta = beta[1 : 1 + n_monthly_lags]
    abs_sum = float(np.sum(np.abs(x_beta)))
    if abs_sum > 0 and np.isfinite(abs_sum):
        implied_weights = np.abs(x_beta) / abs_sum
    else:
        implied_weights = equal_weights(n_monthly_lags)

    model = MIDASModel(
        predictor=predictor,
        weight_scheme="unrestricted",
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
        beta=beta,
        lag_weights=implied_weights,
        weight_params={},
        theta=None,
        is_unrestricted=True,
    )

    diagnostics = MIDASFitDiagnostics(
        predictor=predictor,
        requested_weight_scheme="unrestricted",
        actual_weight_scheme="unrestricted",
        converged=True,
        used_fallback=False,
        objective_value=sse,
        in_sample_rmse=rmse,
        n_iter=0,
        n_train_rows=int(len(y)),
        n_features=int(Z.shape[1]),
        status_message="unrestricted fit completed",
        weight_params={},
        best_theta=None,
        best_start_index=None,
        all_start_objectives=[],
    )

    return MIDASFitResult(model=model, diagnostics=diagnostics)


def _fit_nonlinear_weighted(
    X: pd.DataFrame,
    y: pd.Series,
    predictor: str,
    scheme: str,
    n_monthly_lags: int,
    n_y_lags: int,
    x_cols: list[str],
    y_cols: list[str],
    max_iter: int,
    tol: float,
    n_starts: int,
    parameter_bound: float,
    ridge_alpha: float,
    fallback_weight_scheme: str,
    initial_params: dict[str, float] | None,
) -> MIDASFitResult:
    y_arr = y.to_numpy(dtype=float)
    initial_theta = _theta_from_params(scheme, initial_params)

    def objective(theta: np.ndarray) -> float:
        try:
            params, _ = _params_from_theta(scheme, theta)
            weights = lag_weights(n_monthly_lags, scheme=scheme, params=params)
            Z = _weighted_design(X, weights=weights, x_cols=x_cols, y_cols=y_cols)
            _beta, sse, _rmse = _fit_linear_design(Z, y_arr, ridge_alpha=ridge_alpha)
            if not np.isfinite(sse):
                return 1e30
            return float(sse)
        except Exception:
            return 1e30

    if scheme == "beta":
        starts = _beta_starts(n_starts, initial_theta=initial_theta)
    elif scheme == "exp_almon":
        starts = _exp_almon_starts(n_starts, initial_theta=initial_theta)
    else:
        raise ValueError(f"Unsupported nonlinear scheme: {scheme}")

    bounds = [(-parameter_bound, parameter_bound), (-parameter_bound, parameter_bound)]

    best_res = None
    best_val = float("inf")
    best_start_index = None
    all_start_objectives: list[float] = []

    for start_index, start in enumerate(starts):
        res = minimize(
            objective,
            x0=start,
            method="L-BFGS-B",
            bounds=bounds,
            options={"maxiter": int(max_iter), "ftol": float(tol)},
        )

        val = float(res.fun) if np.isfinite(res.fun) else float("inf")
        all_start_objectives.append(val)

        if val < best_val:
            best_val = val
            best_res = res
            best_start_index = start_index

    if best_res is not None and np.isfinite(best_val):
        params, param_dict = _params_from_theta(scheme, best_res.x)
        weights = lag_weights(n_monthly_lags, scheme=scheme, params=params)
        Z = _weighted_design(X, weights=weights, x_cols=x_cols, y_cols=y_cols)
        beta, sse, rmse = _fit_linear_design(Z, y_arr, ridge_alpha=ridge_alpha)

        model = MIDASModel(
            predictor=predictor,
            weight_scheme=scheme,
            n_monthly_lags=n_monthly_lags,
            n_y_lags=n_y_lags,
            beta=beta,
            lag_weights=weights,
            weight_params=param_dict,
            theta=np.asarray(best_res.x, dtype=float),
            is_unrestricted=False,
        )

        diagnostics = MIDASFitDiagnostics(
            predictor=predictor,
            requested_weight_scheme=scheme,
            actual_weight_scheme=scheme,
            converged=bool(best_res.success),
            used_fallback=False,
            objective_value=sse,
            in_sample_rmse=rmse,
            n_iter=int(getattr(best_res, "nit", 0)),
            n_train_rows=int(len(y)),
            n_features=int(Z.shape[1]),
            status_message=str(getattr(best_res, "message", "")),
            weight_params=param_dict,
            best_theta=[float(x) for x in np.asarray(best_res.x, dtype=float)],
            best_start_index=best_start_index,
            all_start_objectives=[float(x) for x in all_start_objectives],
        )

        return MIDASFitResult(model=model, diagnostics=diagnostics)

    if fallback_weight_scheme != "equal":
        raise RuntimeError(
            f"MIDAS optimization failed for {predictor}; unsupported fallback={fallback_weight_scheme}"
        )

    return _fit_equal_or_weighted(
        X=X,
        y=y,
        predictor=predictor,
        requested_weight_scheme=scheme,
        actual_weight_scheme="equal",
        weights=equal_weights(n_monthly_lags),
        weight_params={},
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
        x_cols=x_cols,
        y_cols=y_cols,
        ridge_alpha=ridge_alpha,
        used_fallback=True,
        status_message=f"fallback to equal weights after failed {scheme} optimization",
    )


def fit_univariate_midas_with_diagnostics(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
    *,
    weight_scheme: str = "beta",
    max_iter: int = 200,
    tol: float = 1e-6,
    n_starts: int = 4,
    parameter_bound: float = 3.0,
    fallback_weight_scheme: str = "equal",
    ridge_alpha: float = 0.0,
    initial_params: dict[str, float] | None = None,
) -> MIDASFitResult:
    scheme = weight_scheme.lower()

    X, y, x_cols, y_cols = _prepare_xy(
        X_train=X_train,
        y_train=y_train,
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
    )

    if scheme == "equal":
        return _fit_equal_or_weighted(
            X=X,
            y=y,
            predictor=predictor,
            requested_weight_scheme="equal",
            actual_weight_scheme="equal",
            weights=equal_weights(n_monthly_lags),
            weight_params={},
            n_monthly_lags=n_monthly_lags,
            n_y_lags=n_y_lags,
            x_cols=x_cols,
            y_cols=y_cols,
            ridge_alpha=ridge_alpha,
            used_fallback=False,
            status_message="equal-weight MIDAS fit completed",
        )

    if scheme == "unrestricted":
        return _fit_unrestricted(
            X=X,
            y=y,
            predictor=predictor,
            n_monthly_lags=n_monthly_lags,
            n_y_lags=n_y_lags,
            x_cols=x_cols,
            y_cols=y_cols,
            ridge_alpha=ridge_alpha,
        )

    if scheme in {"beta", "exp_almon"}:
        return _fit_nonlinear_weighted(
            X=X,
            y=y,
            predictor=predictor,
            scheme=scheme,
            n_monthly_lags=n_monthly_lags,
            n_y_lags=n_y_lags,
            x_cols=x_cols,
            y_cols=y_cols,
            max_iter=max_iter,
            tol=tol,
            n_starts=n_starts,
            parameter_bound=parameter_bound,
            ridge_alpha=ridge_alpha,
            fallback_weight_scheme=fallback_weight_scheme,
            initial_params=initial_params,
        )

    raise ValueError(f"Unknown MIDAS weight_scheme: {weight_scheme}")


def fit_univariate_midas(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    predictor: str,
    n_monthly_lags: int,
    n_y_lags: int,
    *,
    weight_scheme: str = "beta",
    max_iter: int = 200,
    tol: float = 1e-6,
    n_starts: int = 4,
    parameter_bound: float = 3.0,
    fallback_weight_scheme: str = "equal",
    ridge_alpha: float = 0.0,
    initial_params: dict[str, float] | None = None,
) -> MIDASModel:
    result = fit_univariate_midas_with_diagnostics(
        X_train=X_train,
        y_train=y_train,
        predictor=predictor,
        n_monthly_lags=n_monthly_lags,
        n_y_lags=n_y_lags,
        weight_scheme=weight_scheme,
        max_iter=max_iter,
        tol=tol,
        n_starts=n_starts,
        parameter_bound=parameter_bound,
        fallback_weight_scheme=fallback_weight_scheme,
        ridge_alpha=ridge_alpha,
        initial_params=initial_params,
    )
    return result.model