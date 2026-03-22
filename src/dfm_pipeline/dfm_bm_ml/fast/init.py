from __future__ import annotations

from typing import Literal, Optional, Sequence, Tuple

import numpy as np

from ..blocks import normalize_blocks
from ..constraints import mm_sum_sq, mm_weights
from ..spec import BMDfmConfig
from ..stability import enforce_var_stability
from ..state_builder import BMParams
from ..toolbox_nanfill import dfm_remnans_spline_method2

PcaFill = Literal["mean", "ffill", "toolbox_spline"]


def _fill_for_pca(Y: np.ndarray, method: Literal["mean", "ffill"]) -> np.ndarray:
    Y = np.asarray(Y, dtype=float)
    X = Y.copy()

    col_means = np.nanmean(X, axis=0)
    col_means = np.where(np.isfinite(col_means), col_means, 0.0)

    if method == "mean":
        inds = np.where(np.isnan(X))
        X[inds] = col_means[inds[1]]
        return X

    if method == "ffill":
        T, n = X.shape
        last = np.full(n, np.nan, dtype=float)
        for t in range(T):
            row = X[t]
            mask = np.isfinite(row)
            last[mask] = row[mask]
            X[t, ~mask] = last[~mask]

        inds = np.where(np.isnan(X))
        X[inds] = col_means[inds[1]]
        return X

    raise ValueError(f"Unsupported fill method: {method!r}")


def _svd_pca(X: np.ndarray, r: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    X = np.asarray(X, dtype=float)
    Xc = X - X.mean(axis=0, keepdims=True)
    U, S, Vt = np.linalg.svd(Xc, full_matrices=False)

    r = int(r)
    if r <= 0:
        raise ValueError("r must be positive")

    if min(U.shape[1], Vt.shape[0]) < r:
        raise ValueError(
            f"PCA rank r={r} exceeds available rank={min(U.shape[1], Vt.shape[0])}."
        )

    U_r = U[:, :r]
    S_r = S[:r]
    F = U_r * S_r
    Lambda = Vt[:r, :].T
    return U_r, S_r, F, Lambda


def pca_init_factors(
    Y_monthly: np.ndarray, r_total: int, fill: Literal["mean", "ffill"]
) -> Tuple[np.ndarray, np.ndarray]:
    X = _fill_for_pca(Y_monthly, fill)
    _, _, F0, Lambda0 = _svd_pca(X, r=int(r_total))
    return F0, Lambda0


def block_pca_deflation_init(
    Y_monthly_filled: np.ndarray,
    *,
    r_by_block: Sequence[int],
    blocks_mask: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    X = np.asarray(Y_monthly_filled, dtype=float)
    if X.ndim != 2:
        raise ValueError("Y_monthly_filled must be 2D")

    Tn, nM = X.shape

    r_by_block = tuple(int(x) for x in r_by_block)
    n_blocks = len(r_by_block)
    if blocks_mask.shape != (int(nM), int(n_blocks)):
        raise ValueError(
            f"blocks_mask shape must be (nM, n_blocks)=({nM},{n_blocks}), got {blocks_mask.shape}."
        )

    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive")

    X_res = X - X.mean(axis=0, keepdims=True)

    F_list: list[np.ndarray] = []
    Lambda_m0 = np.zeros((int(nM), int(r_total)), dtype=float)

    cursor = 0
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        if rb == 0:
            continue

        cols = np.where(blocks_mask[:, b] == 1)[0]
        if cols.size == 0:
            raise ValueError(f"Block {b} has rb={rb} but no monthly series assigned.")

        Xb = X_res[:, cols]
        U_r, S_r, F_b, Lambda_b = _svd_pca(Xb, r=rb)

        Lambda_m0[cols, cursor:cursor + rb] = Lambda_b

        X_res = X_res - (U_r @ (U_r.T @ X_res))

        F_list.append(F_b)
        cursor += rb

    if cursor != int(r_total):
        raise RuntimeError("Internal error: factor cursor mismatch.")

    F0 = np.concatenate(F_list, axis=1) if len(F_list) else np.zeros((Tn, 0), dtype=float)
    return F0, Lambda_m0


def _fit_var_ols(F: np.ndarray, p: int) -> Tuple[list[np.ndarray], np.ndarray]:
    F = np.asarray(F, dtype=float)
    T, r = F.shape
    p = int(p)

    if p <= 0:
        return [], np.eye(r)

    Y = F[p:, :]
    X = []
    for lag in range(1, p + 1):
        X.append(F[p - lag : T - lag, :])
    X = np.concatenate(X, axis=1)

    B, *_ = np.linalg.lstsq(X, Y, rcond=None)
    resid = Y - X @ B

    Q = np.cov(resid.T, bias=True)
    if Q.ndim == 0:
        Q = np.array([[float(Q)]], dtype=float)
    Q = (Q + Q.T) / 2.0

    Phi_lags = []
    for lag in range(p):
        Phi_lags.append(B[lag * r : (lag + 1) * r, :].T)
    return Phi_lags, Q


def _quarterly_regression_init(
    *,
    F0: np.ndarray,
    y_q_nan: np.ndarray,
    mm_style: str,
) -> Tuple[np.ndarray, float]:
    y_q_nan = np.asarray(y_q_nan, dtype=float).reshape(-1)
    obs = np.where(np.isfinite(y_q_nan))[0]

    if obs.size == 0 or F0.shape[1] == 0:
        Lambda_q0 = np.zeros((1, int(F0.shape[1])), dtype=float)
        if F0.shape[1] > 0:
            Lambda_q0[0, 0] = 1.0
        return Lambda_q0, 1.0

    w = mm_weights(str(mm_style)).astype(float)
    if w.shape != (5,):
        raise ValueError("mm_weights must return shape (5,)")

    obs = obs[obs >= 4]
    if obs.size < int(F0.shape[1]):
        Xq = F0[np.isfinite(y_q_nan), :]
        yq = y_q_nan[np.isfinite(y_q_nan)]
        if Xq.shape[0] >= Xq.shape[1]:
            beta, *_ = np.linalg.lstsq(Xq, yq, rcond=None)
            resid = yq - Xq @ beta
            return beta.reshape(1, -1), float(np.nanvar(resid))
        Lambda_q0 = np.zeros((1, int(F0.shape[1])), dtype=float)
        Lambda_q0[0, 0] = 1.0
        return Lambda_q0, 1.0

    Xq = np.zeros((int(obs.size), int(F0.shape[1])), dtype=float)
    for ii, t in enumerate(obs):
        acc = np.zeros((int(F0.shape[1]),), dtype=float)
        for k in range(5):
            acc += float(w[k]) * F0[int(t - k), :]
        Xq[ii, :] = acc

    yq = y_q_nan[obs]

    beta, *_ = np.linalg.lstsq(Xq, yq, rcond=None)
    resid = yq - Xq @ beta
    R_q0 = float(np.nanvar(resid))
    return beta.reshape(1, -1), R_q0


def init_params_pca(
    Y_scaled: np.ndarray,
    *,
    nM: int,
    config: BMDfmConfig,
    blocks: Optional[np.ndarray] = None,
) -> BMParams:
    Y_scaled = np.asarray(Y_scaled, dtype=float)
    if Y_scaled.ndim != 2:
        raise ValueError("Y_scaled must be 2D (T, nM+1).")
    if Y_scaled.shape[1] != int(nM) + 1:
        raise ValueError(f"Expected Y_scaled.shape[1]==nM+1 ({nM+1}), got {Y_scaled.shape[1]}.")

    r_by_block = tuple(int(x) for x in config.r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be > 0.")

    n_blocks = len(r_by_block)
    nQ = int(getattr(config, "n_quarterly", 1))

    if blocks is None and getattr(config, "blocks", None) is not None:
        blocks = normalize_blocks(
            getattr(config, "blocks"),
            nM=int(nM),
            n_blocks=int(n_blocks),
            nQ=int(nQ),
        )

    if blocks is not None and blocks.shape != (int(nM), int(n_blocks)):
        raise ValueError(
            f"blocks must have shape (nM, n_blocks)=({nM},{n_blocks}), got {blocks.shape}."
        )

    ppC = max(int(config.p), 5)

    if getattr(config, "pca_fill", "mean") == "toolbox_spline":
        k = int(getattr(config, "pca_spline_k", 3))
        frac = float(getattr(config, "pca_spline_trim_row_missing_frac", 0.8))

        fill_res = dfm_remnans_spline_method2(Y_scaled, k=k, row_missing_frac=frac)
        X_bal_filled = fill_res.X_bal_filled
        indNaN_bal = fill_res.indNaN_bal

        X_bal_nan = X_bal_filled.copy()
        X_bal_nan[indNaN_bal] = np.nan

        Y_m_filled = X_bal_filled[:, :nM]
        Y_m_nan = X_bal_nan[:, :nM]
        y_q_nan = X_bal_nan[:, nM]

        if blocks is None:
            F0, Lambda_m0 = pca_init_factors(Y_m_filled, r_total=r_total, fill="mean")
        else:
            F0, Lambda_m0 = block_pca_deflation_init(
                Y_m_filled, r_by_block=r_by_block, blocks_mask=blocks
            )
    else:
        Y_m_nan = Y_scaled[:, :nM]
        y_q_nan = Y_scaled[:, nM]

        Y_m_filled = _fill_for_pca(Y_m_nan, method=str(config.pca_fill))  # type: ignore[arg-type]

        if blocks is None:
            F0, Lambda_m0 = pca_init_factors(
                Y_m_nan, r_total=r_total, fill=str(config.pca_fill)  # type: ignore[arg-type]
            )
        else:
            F0, Lambda_m0 = block_pca_deflation_init(
                Y_m_filled, r_by_block=r_by_block, blocks_mask=blocks
            )

    blocks_F: list[np.ndarray] = []
    s = 0
    for rb in r_by_block:
        blocks_F.append(F0[:, s : s + int(rb)])
        s += int(rb)

    Phi_blocks: list[list[np.ndarray]] = []
    Q_f_blocks: list[np.ndarray] = []
    for bF in blocks_F:
        Phi_lags, Qb = _fit_var_ols(bF, p=config.p)
        if config.force_var_stability and config.p > 0:
            Phi_lags = enforce_var_stability(
                Phi_lags,
                ppC=ppC,
                shrink=config.var_stability_shrink,
                max_iter=getattr(config, "var_stability_max_iter", 50),
            )
        Phi_blocks.append(Phi_lags)
        Q_f_blocks.append(Qb)

    Lambda_q0, R_q0 = _quarterly_regression_init(
        F0=F0, y_q_nan=y_q_nan, mm_style=str(config.mm_weight_style)
    )

    R_q0 = max(float(R_q0), float(config.quarterly_meas_var_floor))
    R_diag_q0 = np.full(int(nQ), float(R_q0), dtype=float)

    if config.idio_ar1:
        R_m0 = np.full(int(nM), float(config.monthly_meas_var_floor), dtype=float)
    else:
        resid_m = Y_m_nan - (F0 @ Lambda_m0.T)
        R_m0 = np.nanvar(resid_m, axis=0)
        R_m0 = np.where(np.isfinite(R_m0), R_m0, float(config.monthly_meas_var_floor))
        R_m0 = np.maximum(R_m0, float(config.monthly_meas_var_floor))

    rho_m0 = np.full(int(nM), float(config.rho_idio_init), dtype=float)
    sig2_m0 = np.full(int(nM), 1.0 - float(config.rho_idio_init) ** 2, dtype=float)
    sig2_m0 = np.maximum(sig2_m0, float(config.min_var))

    rho_q0 = np.zeros(int(nQ), dtype=float)

    sig2_q_scalar = 1.0 / float(mm_sum_sq(config.mm_weight_style))
    sig2_q_scalar = max(sig2_q_scalar, float(config.min_var))
    sig2_q0 = np.full(int(nQ), float(sig2_q_scalar), dtype=float)

    return BMParams(
        Phi_blocks=Phi_blocks,
        Q_f_blocks=Q_f_blocks,
        rho_m=rho_m0,
        sig2_m=sig2_m0,
        rho_q=rho_q0,
        sig2_q=sig2_q0,
        Lambda_m=Lambda_m0,
        Lambda_q=Lambda_q0,
        R_diag_m=R_m0,
        R_diag_q=R_diag_q0,
    )