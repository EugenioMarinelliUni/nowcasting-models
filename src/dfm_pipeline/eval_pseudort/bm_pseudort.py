from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_bm_ml.fit import fit_bm_dfm
from dfm_pipeline.dfm_bm_ml.state_builder import build_state_space
from dfm_pipeline.dfm_bm_ml.types import BMDfmResult
from dfm_pipeline.dfm_dyn.state_space_new import kalman_filter, kalman_smoother
from dfm_pipeline.utils.threadpool import limit_blas_threads


@dataclass
class PseudoRTEvalConfig:
    eval_start: str
    eval_end: str

    delay_style: str = "none"  # none|trailing_nan|json_map
    delay_json: str | None = None

    gdp_rel: int = 0  # release lag for quarterly target (months)

    # horizons in quarter-terms:
    # bac = backcast (previous quarter), now = nowcast (current quarter), for = forecast (next quarter)
    horizons: tuple[str, ...] = ("bac", "now", "for")


def _rmse(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    return float(np.sqrt(np.mean(x * x)))


def _directional_accuracy(pred: np.ndarray, actual: np.ndarray) -> float:
    mask = np.isfinite(pred) & np.isfinite(actual)
    pred = pred[mask]
    actual = actual[mask]
    if pred.size < 2:
        return float("nan")
    dp = np.diff(pred)
    da = np.diff(actual)
    ok = (dp >= 0) == (da >= 0)
    return float(np.mean(ok))


def compute_scores(pred_df: pd.DataFrame) -> dict:
    pred_col = "pred_raw" if "pred_raw" in pred_df.columns else "pred"
    actual_col = "actual_raw" if "actual_raw" in pred_df.columns else "actual"

    out: dict = {}
    df = pred_df.copy()

    df["err"] = df[pred_col] - df[actual_col]

    for h in sorted(df["horizon"].unique()):
        sub = df[df["horizon"] == h]
        err = sub["err"].to_numpy(dtype=float)
        pred = sub[pred_col].to_numpy(dtype=float)
        actual = sub[actual_col].to_numpy(dtype=float)

        out[h] = {
            "rmse": _rmse(err),
            "directional_accuracy": _directional_accuracy(pred, actual),
            "n_obs": int(np.isfinite(actual).sum()),
        }

    return out


def _quarter_end_months(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return index[index.month.isin([3, 6, 9, 12])]


def _horizon_target_date(eval_date: pd.Timestamp, horizon: str) -> pd.Timestamp:
    q = eval_date.to_period("Q")
    if horizon == "now":
        tgt_q = q
    elif horizon == "bac":
        tgt_q = q - 1
    elif horizon == "for":
        tgt_q = q + 1
    else:
        raise ValueError(f"Unknown horizon={horizon!r}")
    return tgt_q.end_time.to_period("M").start_time


def _month_of_quarter(eval_date: pd.Timestamp) -> int:
    m = int(eval_date.month)
    return ((m - 1) % 3) + 1


def _apply_delay_mask(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    style: str,
    delay_map: dict[str, int] | None,
) -> pd.DataFrame:
    if style == "none":
        return X

    Xw = X.copy()
    if style == "trailing_nan":
        Xw.loc[Xw.index > eval_date, :] = np.nan
        return Xw

    if style == "json_map":
        if not delay_map:
            raise ValueError("delay_style='json_map' requires delay_json providing a dict series->delay_months")
        for col, d in delay_map.items():
            if col not in Xw.columns:
                continue
            if d <= 0:
                continue
            cutoff = eval_date - pd.offsets.MonthBegin(d - 1)
            Xw.loc[cutoff:eval_date, col] = np.nan
        return Xw

    raise ValueError(f"Unknown delay_style={style!r}")


def _apply_quarterly_release_mask(y: pd.Series, eval_date: pd.Timestamp, gdp_rel: int) -> pd.Series:
    if gdp_rel <= 0:
        return y
    y2 = y.copy()
    qends = _quarter_end_months(y2.index)
    for t_q in qends:
        release_date = t_q + pd.offsets.MonthBegin(gdp_rel)
        if eval_date < release_date:
            y2.loc[t_q] = np.nan
    return y2


def _maybe_scale_observations_for_smoother(Y_stack: np.ndarray, res: BMDfmResult) -> np.ndarray:
    scaler = getattr(res, "scaler", None)
    if scaler is None:
        return Y_stack
    return scaler.transform(Y_stack)


def _rebuild_state_space_from_result(res: BMDfmResult, model_config: Any, nM: int, nQ: int = 1):
    Tm, Qm, C_meas, R_meas, a0, P0, state_index = build_state_space(
        params=res.params,
        nM=int(nM),
        nQ=int(nQ),
        r_by_block=tuple(int(x) for x in getattr(model_config, "r_by_block")),
        p=int(getattr(model_config, "p")),
        ppC=int(getattr(model_config, "ppC", 5)),
        mm_style=str(getattr(model_config, "mm_weight_style", "toolbox")),
        quarterly_meas_var_floor=float(getattr(model_config, "quarterly_meas_var_floor", 1e-6)),
        idio_ar1=bool(getattr(model_config, "idio_ar1", True)),
        jitter=float(getattr(model_config, "jitter", 1e-8)),
        P0_mode=str(getattr(model_config, "P0_mode", "diffuse")),
        a0_override=None,
        P0_override=None,
    )
    return SimpleNamespace(T=Tm, Q=Qm, C=C_meas, R=R_meas, a0=a0, P0=P0, state_index=state_index)


def run_pseudo_rt_eval(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    model_config,
    eval_cfg: PseudoRTEvalConfig,
    outdir: str | Path | None = None,
    warm_start: bool = False,
    blas_threads: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    if blas_threads is not None and int(blas_threads) > 0:
        limit_blas_threads(int(blas_threads))

    out_path = Path(outdir) if outdir is not None else None
    if out_path is not None:
        out_path.mkdir(parents=True, exist_ok=True)

    eval_start = pd.Timestamp(eval_cfg.eval_start)
    eval_end = pd.Timestamp(eval_cfg.eval_end)

    idx = X_full.index
    eval_months = idx[(idx >= eval_start) & (idx <= eval_end)]
    if eval_months.empty:
        raise ValueError("No eval months in requested window after alignment with panel index.")

    delay_map = None
    if eval_cfg.delay_style == "json_map":
        if not eval_cfg.delay_json:
            raise ValueError("delay_style='json_map' requires delay_json.")
        delay_map = json.loads(Path(eval_cfg.delay_json).read_text(encoding="utf-8"))

    rows: list[dict] = []

    init_params = None
    em_cache = None

    for t in eval_months:
        X_work = X_full.loc[:t].copy()
        y_work = y_full.loc[:t].copy()

        X_work = _apply_delay_mask(X_work, t, eval_cfg.delay_style, delay_map)
        y_work = _apply_quarterly_release_mask(y_work, t, eval_cfg.gdp_rel)

        res = fit_bm_dfm(
            X_monthly=X_work.to_numpy(dtype=float),
            y_quarterly=y_work.to_numpy(dtype=float),
            config=model_config,
            init_params=init_params if warm_start else None,
            em_cache=em_cache if warm_start else None,
        )

        if warm_start:
            init_params = res.params
            em_cache = res.em_cache

        Y_stack = np.column_stack([X_full.to_numpy(dtype=float), y_full.to_numpy(dtype=float)])
        Y_stack_scaled = _maybe_scale_observations_for_smoother(Y_stack, res)

        ss = _rebuild_state_space_from_result(res, model_config=model_config, nM=X_full.shape[1], nQ=1)
        kf = kalman_filter(Y=Y_stack_scaled, T=ss.T, Z=ss.C, R=ss.R, Q=ss.Q, a0=ss.a0, P0=ss.P0)
        ks = kalman_smoother(kf)

        a_smooth = ks["a_smooth"]
        C = ss.C

        t_moq = _month_of_quarter(t)

        scaler = getattr(res, "scaler", None)
        mu_y = float(getattr(scaler, "mu", np.array([0.0]))[-1]) if scaler is not None else 0.0
        sd_y = float(getattr(scaler, "sd", np.array([1.0]))[-1]) if scaler is not None else 1.0

        for h in eval_cfg.horizons:
            target_date = _horizon_target_date(t, h)
            if target_date not in idx:
                pred_scaled = float("nan")
                actual_raw = float("nan")
            else:
                target_idx = idx.get_loc(target_date)
                Cq = C[-1, :]
                pred_scaled = float(Cq @ a_smooth[target_idx, :])
                actual_raw = float(y_full.loc[target_date])

            pred_raw = float(pred_scaled * sd_y + mu_y) if np.isfinite(pred_scaled) else float("nan")
            actual_scaled = (
                float((actual_raw - mu_y) / sd_y) if np.isfinite(actual_raw) and sd_y != 0 else float("nan")
            )

            rows.append(
                {
                    "eval_date": t,
                    "moq": int(t_moq),
                    "horizon": h,
                    "target_date": target_date,
                    "pred_scaled": pred_scaled,
                    "actual_scaled": actual_scaled,
                    "pred_raw": pred_raw,
                    "actual_raw": actual_raw,
                    "pred": pred_raw,
                    "actual": actual_raw,
                    "scaling_mode": str(getattr(model_config, "scaling_mode", "")),
                }
            )

    pred_df = pd.DataFrame(rows).sort_values(["eval_date", "horizon", "moq"]).reset_index(drop=True)
    scores = compute_scores(pred_df)

    if out_path is not None:
        pred_df.to_csv(out_path / "predictions.csv", index=False)
        with open(out_path / "scores.json", "w", encoding="utf-8") as f:
            json.dump(scores, f, indent=2)

    return pred_df, scores
