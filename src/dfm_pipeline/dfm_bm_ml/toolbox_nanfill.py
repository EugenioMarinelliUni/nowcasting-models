from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np


@dataclass(frozen=True)
class ToolboxSplineFillResult:
    X_bal_filled: np.ndarray      # balanced (rows trimmed) and filled
    indNaN_bal: np.ndarray        # NaN pattern on the balanced data (True where original was NaN)
    keep_mask: np.ndarray         # boolean mask of rows kept from original X (len=T_original)


def _moving_average_filter_toolbox(x: np.ndarray, k: int) -> np.ndarray:
    """
    Replicate MATLAB:
      x_MA = filter(ones(2k+1,1)/(2k+1), 1, [x(1)*ones(k,1); x; x(end)*ones(k,1)]);
      x_MA = x_MA(2k+1:end);
    which yields a length-T output.
    """
    x = np.asarray(x, dtype=float)
    T = x.shape[0]
    if T == 0:
        return x.copy()
    if k <= 0:
        return x.copy()

    w = np.ones(2 * k + 1, dtype=float) / float(2 * k + 1)
    pad = np.concatenate([np.full(k, x[0]), x, np.full(k, x[-1])], axis=0)
    y_full = np.convolve(pad, w, mode="full")[: pad.shape[0]]
    y = y_full[2 * k :]  # 0-based slice corresponding to (2k+1):end in MATLAB
    if y.shape[0] != T:
        # defensive; should not happen
        y = y[:T]
    return y


def _cubic_spline_fill_segment(x: np.ndarray, idx_obs: np.ndarray, idx_eval: np.ndarray) -> np.ndarray:
    """
    MATLAB spline(...) equivalent on integer grid. Uses SciPy CubicSpline if available,
    otherwise falls back to linear interpolation.
    """
    x_obs = x[idx_obs]
    if idx_obs.size == 1:
        return np.full(idx_eval.size, float(x_obs[0]), dtype=float)

    # Try SciPy cubic spline
    try:
        from scipy.interpolate import CubicSpline  # type: ignore
        cs = CubicSpline(idx_obs.astype(float), x_obs.astype(float), bc_type="not-a-knot")
        return cs(idx_eval.astype(float)).astype(float)
    except Exception:
        # Fallback: linear
        return np.interp(idx_eval.astype(float), idx_obs.astype(float), x_obs.astype(float)).astype(float)


def dfm_remnans_spline_method2(
    X: np.ndarray,
    *,
    k: int = 3,
    row_missing_frac: float = 0.8,
) -> ToolboxSplineFillResult:
    """
    Python rendition of Nowcasting Toolbox:
      DFM_remNaNs_spline(X, optNaN) with optNaN.method = 2, optNaN.k = 3.

    Steps (toolbox method 2):
      1) Identify rows with > N * row_missing_frac NaNs.
      2) Remove leading and trailing stretches of such rows (trim sample).
      3) For each series:
         - spline interpolate between first and last observed values
         - for remaining NaNs (outside observed span), set to series median
         - apply moving average filter and overwrite those remaining-NaN positions with MA values
    """
    X = np.asarray(X, dtype=float)
    if X.ndim != 2:
        raise ValueError("X must be 2D (T, N).")

    T, N = X.shape
    indNaN = np.isnan(X)

    # --- Trim leading/trailing rows with too many NaNs (toolbox method 2) ---
    rem1 = (np.sum(indNaN, axis=1) > (N * float(row_missing_frac)))

    # nanLead: cumsum(rem1) == (1:T)'
    nanLead = (np.cumsum(rem1).astype(int) == np.arange(1, T + 1))
    # nanEnd: same on reversed
    nanEnd_rev = (np.cumsum(rem1[::-1]).astype(int) == np.arange(1, T + 1))
    nanEnd = nanEnd_rev[::-1]

    nanLE = nanLead | nanEnd
    keep_mask = ~nanLE

    X_bal = X[keep_mask, :].copy()
    indNaN_bal = np.isnan(X_bal)

    Tb = X_bal.shape[0]
    if Tb == 0:
        # degenerate: all rows trimmed; return zeros
        X_bal_filled = np.zeros((0, N), dtype=float)
        return ToolboxSplineFillResult(X_bal_filled=X_bal_filled, indNaN_bal=indNaN_bal, keep_mask=keep_mask)

    # --- Fill each column ---
    X_filled = X_bal.copy()
    for i in range(N):
        x = X_filled[:, i]
        isnan0 = np.isnan(x)

        if np.all(isnan0):
            # All missing: fill zeros (toolbox would struggle; keep deterministic)
            X_filled[:, i] = 0.0
            continue

        idx_obs = np.where(~isnan0)[0]
        t1 = int(idx_obs.min())
        t2 = int(idx_obs.max())

        # spline fill within [t1, t2]
        idx_eval = np.arange(t1, t2 + 1)
        x_seg = x.copy()
        x_seg[idx_eval] = _cubic_spline_fill_segment(x, idx_obs, idx_eval)

        # mask for NaNs remaining after spline (typically outside [t1,t2])
        isnan_after = np.isnan(x_seg)

        # median of observed (after spline, but excluding NaNs)
        med = float(np.median(x_seg[~isnan_after]))
        x_seg[isnan_after] = med

        # moving average and overwrite those originally "remaining NaNs after spline"
        x_ma = _moving_average_filter_toolbox(x_seg, k=k)
        x_seg[isnan_after] = x_ma[isnan_after]

        X_filled[:, i] = x_seg

    return ToolboxSplineFillResult(X_bal_filled=X_filled, indNaN_bal=indNaN_bal, keep_mask=keep_mask)
