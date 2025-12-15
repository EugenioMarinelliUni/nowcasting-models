import numpy as np
from typing import Tuple, List

from .em_dfm import em_dfm_full, DynDFMParams

# Optional tqdm progress bar
try:
    from tqdm import trange
except ImportError:  # fallback if tqdm is not installed
    trange = range


def rolling_dfm_nowcast(
    X: np.ndarray,
    y: np.ndarray,
    r: int,
    p: int,
    start_idx: int,
    max_iter_em: int = 50,
    tol_em: float = 1e-4,
    diag_R: bool = True,
    use_tqdm: bool = True,
) -> Tuple[np.ndarray, List[DynDFMParams]]:
    """
    Very simple Bańbura–Modugno style rolling nowcast:

        1. For each t in [start_idx, T-1]:
           - Estimate DFM on X[:t+1, :]
           - Run OLS of y[:t+1] on smoothed factors F[:t+1]
           - Use F[t] to predict y_hat[t]
        2. Returns the nowcast series and the list of DFM parameter objects.

    X:
        (T, n) panel, np.nan allowed.
    y:
        (T,) target series (e.g. monthly GDP proxy or quarterly-monthly mixed indicator,
        already aligned to the monthly index).
    r, p:
        DFM parameters (dynamic factors, VAR order).
    start_idx:
        first t at which you want a nowcast (e.g. number of in-sample points).
    use_tqdm:
        if True, show a tqdm progress bar over rolling dates (if tqdm is installed).
    """
    X = np.asarray(X, float)
    y = np.asarray(y, float)
    Tn = X.shape[0]
    if y.shape[0] != Tn:
        raise ValueError("X and y must have the same number of time points.")
    if start_idx < 0 or start_idx >= Tn:
        raise ValueError("start_idx must be in [0, T-1].")

    y_hat = np.full(Tn, np.nan)
    models: List[DynDFMParams] = []

    iterator = trange(start_idx, Tn) if use_tqdm else range(start_idx, Tn)

    for t in iterator:
        # estimation window: [0, t]
        X_t = X[: t + 1, :]
        y_t = y[: t + 1]

        dfm_params = em_dfm_full(
            X=X_t,
            r=r,
            p=p,
            max_iter=max_iter_em,
            tol=tol_em,
            verbose=False,
            diag_R=diag_R,
            use_tqdm=False,  # inner EM bar usually too noisy inside rolling bar
        )
        models.append(dfm_params)

        F = dfm_params.factors_smooth         # (t+1, r)
        mask_y = ~np.isnan(y_t)
        if mask_y.sum() <= r:
            # insufficient target data to estimate bridge regression
            if use_tqdm and hasattr(iterator, "set_postfix"):
                try:
                    iterator.set_postfix(nowcast="nan")
                except Exception:
                    pass
            continue

        X_reg = F[mask_y, :]                  # (T_eff, r)
        y_reg = y_t[mask_y]                   # (T_eff,)

        beta, _, _, _ = np.linalg.lstsq(X_reg, y_reg, rcond=None)
        y_hat_t = F[-1] @ beta
        y_hat[t] = y_hat_t

        if use_tqdm and hasattr(iterator, "set_postfix"):
            try:
                iterator.set_postfix(nowcast=f"{y_hat_t:.4f}")
            except Exception:
                pass

    return y_hat, models
