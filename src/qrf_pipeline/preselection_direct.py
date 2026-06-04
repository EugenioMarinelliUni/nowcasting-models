from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DirectPreselectionConfig:
    n_lags: int = 3
    n_y_lags: int = 2
    top_k: int = 20
    include_ar_y: bool = True
    hac_lags: int = 4
    min_obs: int = 36


def _month_start_index(obj: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    out = obj.copy()
    out.index = pd.DatetimeIndex(pd.to_datetime(out.index)).to_period("M").to_timestamp(how="start")
    return out.sort_index()


def build_lagged_direct_design(
    X: pd.DataFrame,
    y: pd.Series,
    predictors: list[str] | None,
    cfg: DirectPreselectionConfig,
) -> tuple[pd.DataFrame, pd.Series]:
    X = _month_start_index(X)
    y = _month_start_index(y)

    if predictors is None:
        predictors = list(X.columns)

    features = {}

    for var in predictors:
        if var not in X.columns:
            continue
        for lag in range(1, cfg.n_lags + 1):
            features[f"{var}__xlag{lag}"] = X[var].shift(lag - 1)

    for lag in range(1, cfg.n_y_lags + 1):
        features[f"y__ylag{lag}"] = y.shift(3 * lag)

    F = pd.DataFrame(features, index=X.index)
    common = F.index.intersection(y.index)
    F = F.loc[common]
    target = y.loc[common]

    Z = pd.concat([target.rename("y"), F], axis=1).dropna()
    return Z.iloc[:, 1:], Z.iloc[:, 0]


def _ols_beta_tstat_hac(y: np.ndarray, X: np.ndarray, coef_index: int, hac_lags: int) -> tuple[float, float]:
    n, k = X.shape
    if n <= k + 5:
        return float("nan"), float("nan")

    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ beta
    xtx_inv = np.linalg.pinv(X.T @ X)

    meat = np.zeros((k, k))

    for t in range(n):
        xt = X[t : t + 1, :]
        meat += float(resid[t] ** 2) * (xt.T @ xt)

    for lag in range(1, hac_lags + 1):
        weight = 1.0 - lag / (hac_lags + 1.0)
        gamma = np.zeros((k, k))
        for t in range(lag, n):
            xt = X[t : t + 1, :]
            xl = X[t - lag : t - lag + 1, :]
            gamma += float(resid[t] * resid[t - lag]) * (xt.T @ xl)
        meat += weight * (gamma + gamma.T)

    cov = xtx_inv @ meat @ xtx_inv
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))

    coef = float(beta[coef_index])
    tstat = float(coef / se[coef_index]) if se[coef_index] > 1e-12 else float("nan")
    return coef, tstat


def rank_features_hac_tstat(
    F: pd.DataFrame,
    y: pd.Series,
    cfg: DirectPreselectionConfig,
) -> pd.DataFrame:
    rows = []
    ar_controls = [c for c in F.columns if cfg.include_ar_y and c.startswith("y__ylag")]

    for col in F.columns:
        if col.startswith("y__ylag"):
            continue

        cols = [col] + ar_controls
        Z = pd.concat([y.rename("y"), F[cols]], axis=1).dropna()

        predictor = col.split("__xlag")[0]
        lag = int(col.split("__xlag")[1])

        if len(Z) < cfg.min_obs:
            rows.append(
                {
                    "feature": col,
                    "predictor": predictor,
                    "lag": lag,
                    "coef": np.nan,
                    "tstat": np.nan,
                    "abs_tstat": np.nan,
                    "n_obs": int(len(Z)),
                }
            )
            continue

        yy = Z["y"].to_numpy(dtype=float)
        XX = Z[cols].to_numpy(dtype=float)
        XX = np.column_stack([np.ones(len(XX)), XX])

        coef, tstat = _ols_beta_tstat_hac(yy, XX, coef_index=1, hac_lags=cfg.hac_lags)

        rows.append(
            {
                "feature": col,
                "predictor": predictor,
                "lag": lag,
                "coef": coef,
                "tstat": tstat,
                "abs_tstat": abs(tstat) if np.isfinite(tstat) else np.nan,
                "n_obs": int(len(Z)),
            }
        )

    out = pd.DataFrame(rows).sort_values("abs_tstat", ascending=False, na_position="last")
    out["rank"] = np.arange(1, len(out) + 1)
    return out.reset_index(drop=True)


def select_predictors_from_ranked_features(rank_df: pd.DataFrame, top_k: int) -> list[str]:
    selected: list[str] = []

    for predictor in rank_df["predictor"].tolist():
        if predictor not in selected:
            selected.append(str(predictor))
        if len(selected) >= top_k:
            break

    return selected


def run_direct_hac_preselection(
    X: pd.DataFrame,
    y: pd.Series,
    predictors: list[str] | None,
    cfg: DirectPreselectionConfig,
) -> tuple[pd.DataFrame, list[str]]:
    F, yy = build_lagged_direct_design(X, y, predictors=predictors, cfg=cfg)
    rank_df = rank_features_hac_tstat(F, yy, cfg=cfg)
    selected = select_predictors_from_ranked_features(rank_df, top_k=cfg.top_k)
    return rank_df, selected


def write_direct_preselection_outputs(
    rank_df: pd.DataFrame,
    selected: list[str],
    outdir: str | Path,
) -> tuple[Path, Path]:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    rank_path = outdir / "rank_features.csv"
    selected_path = outdir / "selected_predictors.json"

    rank_df.to_csv(rank_path, index=False)
    with open(selected_path, "w", encoding="utf-8") as f:
        json.dump(selected, f, indent=2)

    return rank_path, selected_path
