# src/dfm_pipeline/dfm_simple/backtest.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Tuple, Dict, Any, Literal, Optional

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_simple.nowcast import nowcast_current_quarter


@dataclass(frozen=True)
class DFMBacktestConfigSimple:
    """
    Configuration for a simple DFM recursive pseudo-real-time backtest.

    At a vintage T in [eval_start, eval_end], the estimation sample is
    [train_start, T] (expanding window).

    ragged_mode:
      - "none": use full X up to T (no additional masking).
      - "mask": apply a release-pattern mask from 'mask_path' to build a
                pseudo-real-time ragged-edge panel at each vintage.
    """
    panel_id: str
    X_path: Path
    y_path: Path
    train_start: str          # e.g. "1990-01-01"
    eval_start: str           # e.g. "1995-01-01"
    eval_end: str             # e.g. "2019-12-01"
    monthly_freq: str = "MS"  # "MS" or "ME"
    grid: Sequence[Tuple[int, int, int]] = ()
    mask_path: Path | None = None
    ragged_mode: str = "none"   # "none" or "mask"


def _normalize_month_index(idx: pd.Index, monthly_freq: str) -> pd.DatetimeIndex:
    # Pandas stubs are noisy here; idx can be many index types.
    di_any = pd.to_datetime(idx, errors="coerce")  # type: ignore[arg-type]
    di = pd.DatetimeIndex(di_any)
    how: Literal["start", "end"]
    if str(monthly_freq).upper() == "MS":
        how = "start"
    else:
        how = "end"
    return di.to_period("M").to_timestamp(how=how)


def load_X_y_simple(
    X_path: Path,
    y_path: Path,
    monthly_freq: str = "MS",
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Load full standardized X panel and full standardized y target.

    Tries to detect the date column:
      - prefers 'Date', then 'sasdate', then 'date',
      - falls back to the first column if it looks like dates.

    Returns X (DataFrame) and y (Series) with a harmonized monthly index.
    """

    def _load_any_panel(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, low_memory=False)
        date_col: str | None = None

        # Try common names first
        for cand in ["Date", "sasdate", "date"]:
            if cand in df.columns:
                date_col = cand
                break

        if date_col is None:
            # fallback: try first column as dates
            first = df.columns[0]
            dt = pd.to_datetime(df[first], errors="coerce")
            if dt.notna().mean() > 0.9:
                date_col = first
            else:
                raise ValueError(
                    f"No obvious date column in {path}. "
                    f"Columns: {list(df.columns)[:8]}"
                )

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
        return df

    # X
    X_df = _load_any_panel(X_path)
    X = X_df.select_dtypes(include="number")

    # y
    y_df = _load_any_panel(y_path)
    if "y" in y_df.columns:
        y = y_df["y"]
    else:
        num_cols = y_df.select_dtypes(include="number").columns
        if not len(num_cols):
            raise ValueError(f"No numeric target column found in {y_path}.")
        y = y_df[num_cols[0]]

    # Normalize monthly index and align
    X.index = _normalize_month_index(X.index, monthly_freq=monthly_freq)
    y.index = _normalize_month_index(y.index, monthly_freq=monthly_freq)

    idx = X.index.intersection(y.index)
    X = X.loc[idx]
    y = y.loc[idx]

    return X, y


def load_mask_simple(
    mask_path: Path,
    monthly_freq: str = "MS",
) -> pd.DataFrame:
    """
    Load a ragged-edge mask CSV with a 'Date' column and the same series
    columns as X. Entries should be 0/1 or False/True; will be cast to bool.
    """
    M = (
        pd.read_csv(mask_path, parse_dates=["Date"])
        .set_index("Date")
        .sort_index()
    )
    M.index = _normalize_month_index(M.index, monthly_freq=monthly_freq)
    return M


def build_eval_vintages_simple(
    cfg: DFMBacktestConfigSimple,
    idx: pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    all_months = pd.date_range(
        start=pd.to_datetime(cfg.train_start),
        end=pd.to_datetime(cfg.eval_end),
        freq="MS",
    )
    eval_mask = (
        (all_months >= pd.to_datetime(cfg.eval_start))
        & (all_months <= pd.to_datetime(cfg.eval_end))
    )
    eval_months = all_months[eval_mask]
    eval_months = eval_months[eval_months.isin(idx)]
    return eval_months


def build_quarterly_target_from_monthly(y: pd.Series) -> pd.Series:
    """
    From a monthly target with one non-NaN value per quarter, build a
    quarterly series indexed by (year, quarter), taking the non-NaN entry
    in each quarter.

    This is used as the evaluation reference y_Q, so that each monthly
    nowcast for quarter Q is compared to the same quarterly value.
    """
    # Ensure DatetimeIndex
    idx: pd.DatetimeIndex = pd.DatetimeIndex(y.index)

    years = idx.year
    quarters = ((idx.month - 1) // 3) + 1

    # Work in a small DataFrame for clarity
    df = pd.DataFrame(
        {
            "y": y.values,
            "year": years,
            "quarter": quarters,
        },
        index=idx,
    )

    # Keep only months where y is observed
    df_non = df.dropna(subset=["y"])

    # For each (year, quarter), take the first non-NaN y in that quarter
    y_q = (
        df_non
        .groupby(["year", "quarter"])["y"]
        .first()
        .astype(float)
    )

    # y_q has a MultiIndex (year, quarter), so y_q.loc[(year, quarter)] works
    return y_q


def quarter_id_from_timestamp(T: pd.Timestamp) -> tuple[int, int]:
    """
    Map a monthly timestamp T to a (year, quarter) identifier.
    """
    return T.year, int((T.month - 1) // 3 + 1)


def apply_ragged_mask_to_X(
    X_full: pd.DataFrame,
    mask_full: pd.DataFrame,
    T: pd.Timestamp,
    train_start_ts: pd.Timestamp,
) -> pd.DataFrame:
    """
    Apply a ragged-edge mask at vintage T:

      - restrict to [train_start, T],
      - reindex the mask to match X_T,
      - set entries with mask=False to NaN.

    Assumes X_full and mask_full share the same (Date x series) layout.
    """
    X_T = X_full.loc[train_start_ts:T].copy()
    M_T = mask_full.loc[train_start_ts:T].reindex_like(X_T)
    M_bool = M_T.astype(bool)
    X_T[~M_bool] = np.nan
    return X_T


def estimate_dfm_and_nowcast_single_simple(
    X: pd.DataFrame,
    y: pd.Series,
    T: pd.Timestamp,
    q: int,
    r: int,
    p: int,
) -> float:
    """
    Simple wrapper around nowcast_current_quarter.

    Here:
      - q: number of static factors (EM-PCA).
      - r: ignored (placeholder for dynamic dimension).
      - p: VAR order for factor dynamics.
    """
    y_now = nowcast_current_quarter(
        X=X,
        y=y,
        n_factors=q,
        p_var=p,
        T_vintage=T,
        use_smoothed_factors=True,
    )
    if y_now is None:
        return np.nan
    return float(y_now)


def run_backtest_single_spec_simple(
    cfg: DFMBacktestConfigSimple,
    X: pd.DataFrame,
    y: pd.Series,
    y_q: pd.Series,
    mask: Optional[pd.DataFrame],
    q: int,
    r: int,
    p: int,
    min_obs: int = 30,
) -> Dict[str, Any]:
    """
    Run the simple DFM for one (q, r, p) over the evaluation window.

    Evaluation:
      - For each month T in [eval_start, eval_end], we:
          * build X_T and y_T on [train_start, T],
          * compute a nowcast for the current quarter Q(T),
          * compare it to the *quarterly* value y_q[Q(T)].

      - This produces one error per evaluation month, so RMSE is
        effectively a monthly RMSE (each quarter appears up to 3 times,
        once per month of the quarter).

    Extended metrics:
      - Global RMSE and FDA over all eval months.
      - RMSE and FDA by month-in-quarter (rmse_m1/2/3, fda_m1/2/3).
    """
    eval_months = build_eval_vintages_simple(cfg, pd.DatetimeIndex(X.index))
    train_start_ts = pd.to_datetime(cfg.train_start)

    forecasts: list[float] = []
    reals: list[float] = []
    vintages: list[pd.Timestamp] = []
    miqs: list[int] = []

    for T in eval_months:
        # Build X_T (ragged or not) and y_T
        if cfg.ragged_mode == "mask" and mask is not None:
            X_T = apply_ragged_mask_to_X(X, mask, T, train_start_ts)
        else:
            X_T = X.loc[train_start_ts:T]

        y_T = y.loc[train_start_ts:T]

        if X_T.shape[0] < min_obs:
            continue

        # Quarterly reference value for the quarter that contains T
        q_id = quarter_id_from_timestamp(T)
        if q_id not in y_q.index:
            continue

        y_real_Q = float(y_q.loc[q_id])
        if np.isnan(y_real_Q):
            continue

        y_hat_T = estimate_dfm_and_nowcast_single_simple(
            X_T,
            y_T,
            T,
            q=q,
            r=r,
            p=p,
        )

        if np.isnan(y_hat_T):
            continue

        forecasts.append(float(y_hat_T))
        reals.append(y_real_Q)
        vintages.append(T)
        miqs.append(int(((T.month - 1) % 3) + 1))

    if not forecasts:
        rmse_global = np.nan
        fda_global = np.nan
        rmse_m1 = rmse_m2 = rmse_m3 = np.nan
        fda_m1 = fda_m2 = fda_m3 = np.nan
        n_eval = 0
    else:
        f = np.array(forecasts, dtype=float)
        rvals = np.array(reals, dtype=float)
        miqs_arr = np.array(miqs, dtype=int)
        err = f - rvals

        # Global RMSE
        rmse_global = float(np.sqrt(np.mean(err**2)))

        # Global FDA on monthly sequence
        if len(rvals) >= 2:
            dy_real = np.sign(np.diff(rvals))
            dy_pred = np.sign(np.diff(f))
            fda_global = float(np.mean(dy_real == dy_pred))
        else:
            fda_global = np.nan

        # Helper for RMSE by month-in-quarter
        def _rmse_masked(e: np.ndarray, mask_like: Any) -> float:
            mask_arr = np.asarray(mask_like, dtype=bool)
            if not bool(mask_arr.any()):
                return float("nan")
            return float(np.sqrt(np.mean(e[mask_arr] ** 2)))

        # Helper for FDA by month-in-quarter
        def _fda_masked(rvals_all: np.ndarray, f_all: np.ndarray, mask_like: Any) -> float:
            mask_arr = np.asarray(mask_like, dtype=bool)
            idx = np.where(mask_arr)[0]
            if idx.size < 2:
                return float("nan")
            v_real = rvals_all[idx]
            v_pred = f_all[idx]
            if v_real.size < 2:
                return float("nan")
            dy_real_loc = np.sign(np.diff(v_real))
            dy_pred_loc = np.sign(np.diff(v_pred))
            return float(np.mean(dy_real_loc == dy_pred_loc))

        # masks as proper boolean ndarrays
        mask1 = np.asarray(miqs_arr == 1, dtype=bool)
        mask2 = np.asarray(miqs_arr == 2, dtype=bool)
        mask3 = np.asarray(miqs_arr == 3, dtype=bool)

        rmse_m1 = _rmse_masked(err, mask1)
        rmse_m2 = _rmse_masked(err, mask2)
        rmse_m3 = _rmse_masked(err, mask3)

        fda_m1 = _fda_masked(rvals, f, mask1)
        fda_m2 = _fda_masked(rvals, f, mask2)
        fda_m3 = _fda_masked(rvals, f, mask3)

        n_eval = len(forecasts)

    return {
        "panel_id": cfg.panel_id,
        "X_path": str(cfg.X_path),
        "y_path": str(cfg.y_path),
        "train_start": cfg.train_start,
        "eval_start": cfg.eval_start,
        "eval_end": cfg.eval_end,
        "ragged_mode": cfg.ragged_mode,
        "mask_path": str(cfg.mask_path) if cfg.mask_path is not None else "",
        "q": q,
        "r": r,
        "p": p,
        "rmse_nowcast": rmse_global,
        "fda_nowcast": fda_global,
        "rmse_m1": rmse_m1,
        "rmse_m2": rmse_m2,
        "rmse_m3": rmse_m3,
        "fda_m1": fda_m1,
        "fda_m2": fda_m2,
        "fda_m3": fda_m3,
        "n_eval": n_eval,
    }


try:
    from tqdm import tqdm  # type: ignore[import]
except ImportError:  # pragma: no cover
    tqdm = None


def collect_forecast_panel_single_spec_simple(
    cfg: DFMBacktestConfigSimple,
    X: pd.DataFrame,
    y: pd.Series,
    y_q: pd.Series,
    mask: Optional[pd.DataFrame],
    q: int,
    r: int,
    p: int,
    min_obs: int = 30,
) -> pd.DataFrame:
    """
    For a single (q, r, p), collect the per-vintage nowcast panel:

      - one row per evaluation month T,
      - 'y_hat'       = nowcast of the *current quarter* Q(T) at month T,
      - 'y_real_Q'    = realized quarterly target for Q(T),
      - 'month_in_quarter' = 1,2,3 for Jan/Apr/Jul/Oct etc.

    Returns a DataFrame with columns:

      Date, year, quarter, month_in_quarter,
      y_real_Q, y_hat, err, sq_err,

      # change-based FDA (Linzenich & Meunier style)
      dy_real_qoq, dy_hat_qoq, sign_correct_qoq, dir_mag_score_qoq,

      # level-based FDA (your definition)
      sign_correct_level, dir_mag_score_level
    """
    # evaluation months
    eval_months_idx = build_eval_vintages_simple(cfg, pd.DatetimeIndex(X.index))
    eval_months = list(eval_months_idx)
    train_start_ts = pd.to_datetime(cfg.train_start)

    rows: list[dict[str, Any]] = []

    # progress bar over evaluation months
    if tqdm is not None:
        iterator = tqdm(
            eval_months,
            desc=f"DFM single spec q={q}, p={p}",
            total=len(eval_months),
            unit="month",
        )
    else:
        iterator = eval_months

    for T in iterator:
        # Build X_T (ragged or not) and y_T
        if cfg.ragged_mode == "mask" and mask is not None:
            X_T = apply_ragged_mask_to_X(X, mask, T, train_start_ts)
        else:
            X_T = X.loc[train_start_ts:T]

        y_T = y.loc[train_start_ts:T]

        if X_T.shape[0] < min_obs:
            continue

        # Quarterly reference value for the quarter that contains T
        q_id = quarter_id_from_timestamp(T)
        if q_id not in y_q.index:
            continue

        y_real_Q = float(y_q.loc[q_id])
        if np.isnan(y_real_Q):
            continue

        y_hat_T = estimate_dfm_and_nowcast_single_simple(
            X_T,
            y_T,
            T,
            q=q,
            r=r,
            p=p,
        )
        if np.isnan(y_hat_T):
            continue

        # Month-in-quarter: 1,2,3
        miq = int(((T.month - 1) % 3) + 1)

        rows.append(
            {
                "Date": T,
                "year": int(q_id[0]),
                "quarter": int(q_id[1]),
                "month_in_quarter": miq,
                "y_real_Q": y_real_Q,
                "y_hat": float(y_hat_T),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "Date",
                "year",
                "quarter",
                "month_in_quarter",
                "y_real_Q",
                "y_hat",
                "err",
                "sq_err",
                "dy_real_qoq",
                "dy_hat_qoq",
                "sign_correct_qoq",
                "dir_mag_score_qoq",
                "sign_correct_level",
                "dir_mag_score_level",
            ]
        )

    # Base panel
    df = (
        pd.DataFrame.from_records(rows)
        .sort_values(["year", "quarter", "month_in_quarter", "Date"])
        .reset_index(drop=True)
    )

    # Level errors
    df["err"] = df["y_hat"] - df["y_real_Q"]
    df["sq_err"] = df["err"] ** 2

    # ---------- CHANGE-BASED FDA (Linzenich & Meunier style) ----------
    df["dy_real_qoq"] = np.nan
    df["dy_hat_qoq"] = np.nan
    df["sign_correct_qoq"] = np.nan
    df["dir_mag_score_qoq"] = 0.0  # default 0

    # directional-magnitude tuning constants
    eps = 1e-8
    c_qoq = 1.0

    for miq in [1, 2, 3]:
        m = df["month_in_quarter"] == miq
        sub = df.loc[m].sort_values(["year", "quarter"])

        # QoQ changes of realized and forecast at this month-in-quarter
        dy_real = sub["y_real_Q"].diff()
        dy_hat = sub["y_hat"].diff()

        # sign correctness only where both diffs exist and dy_real != 0
        same_sign = (
            dy_real.notna()
            & dy_hat.notna()
            & (dy_real != 0.0)
            & (np.sign(dy_real) == np.sign(dy_hat))
        )

        df.loc[sub.index, "dy_real_qoq"] = dy_real
        df.loc[sub.index, "dy_hat_qoq"] = dy_hat
        df.loc[sub.index, "sign_correct_qoq"] = same_sign.astype(float)

        # magnitude-aware directional score on QoQ changes
        dy_real_arr = dy_real.to_numpy(dtype=float)
        dy_hat_arr = dy_hat.to_numpy(dtype=float)

        score = np.zeros_like(dy_real_arr, dtype=float)
        mask_arr = same_sign.to_numpy(dtype=bool)

        if bool(mask_arr.any()):
            rel_err = np.abs(dy_real_arr[mask_arr] - dy_hat_arr[mask_arr]) / (
                c_qoq * np.abs(dy_real_arr[mask_arr]) + eps
            )
            score[mask_arr] = np.maximum(0.0, 1.0 - rel_err)

        df.loc[sub.index, "dir_mag_score_qoq"] = score

    # ---------- LEVEL-BASED FDA (your definition) ----------
    # sign correctness of the *growth rate* itself: expansion vs contraction
    df["sign_correct_level"] = np.nan
    df["dir_mag_score_level"] = 0.0

    mask_level = df["y_real_Q"].notna() & (df["y_real_Q"] != 0.0)
    if mask_level.any():
        same_sign_level = (
            np.sign(df.loc[mask_level, "y_hat"].to_numpy())
            == np.sign(df.loc[mask_level, "y_real_Q"].to_numpy())
        )
        same_sign_level_arr = np.asarray(same_sign_level, dtype=bool)
        df.loc[mask_level, "sign_correct_level"] = same_sign_level_arr.astype(float)

        # magnitude-aware score on levels
        c_level = 1.0
        y_real_arr = df.loc[mask_level, "y_real_Q"].to_numpy(dtype=float)
        y_hat_arr = df.loc[mask_level, "y_hat"].to_numpy(dtype=float)

        score_level = np.zeros_like(y_real_arr, dtype=float)
        lvl_mask = same_sign_level_arr  # only where sign is correct

        if bool(lvl_mask.any()):
            rel_err_lvl = np.abs(y_real_arr[lvl_mask] - y_hat_arr[lvl_mask]) / (
                c_level * np.abs(y_real_arr[lvl_mask]) + eps
            )
            score_level[lvl_mask] = np.maximum(0.0, 1.0 - rel_err_lvl)

        df.loc[mask_level, "dir_mag_score_level"] = score_level

    return df


def run_dfm_grid_simple(cfg: DFMBacktestConfigSimple) -> pd.DataFrame:
    """
    Run the simple DFM grid backtest for all (q, r, p) in cfg.grid.

    - Loads X, y once.
    - Builds quarterly target y_q once.
    - Optionally loads a ragged-edge mask and applies it at each vintage.
    - Displays a progress bar over the grid if tqdm is installed.
    """
    X, y = load_X_y_simple(cfg.X_path, cfg.y_path, monthly_freq=cfg.monthly_freq)

    mask: Optional[pd.DataFrame] = None
    if cfg.ragged_mode == "mask" and cfg.mask_path is not None:
        mask = load_mask_simple(cfg.mask_path, monthly_freq=cfg.monthly_freq)

    y_q = build_quarterly_target_from_monthly(y)

    grid_list = list(cfg.grid)
    records: list[Dict[str, Any]] = []

    # Wrap the grid iterator in a tqdm progress bar if available
    if tqdm is not None:
        iterator = tqdm(
            grid_list,
            desc=f"DFM grid {cfg.panel_id}",
            total=len(grid_list),
            unit="spec",
        )
    else:
        iterator = grid_list

    for (q, r, p) in iterator:
        if r > q:
            continue

        rec = run_backtest_single_spec_simple(
            cfg,
            X,
            y,
            y_q,
            mask,
            q=q,
            r=r,
            p=p,
        )
        records.append(rec)

    return pd.DataFrame.from_records(records)
