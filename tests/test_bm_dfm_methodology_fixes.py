from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.dfm_bm_ml.ar1 import update_ar1_stationary_moments
from dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba import fit_bm_dfm_fast_numba
from dfm_pipeline.dfm_bm_ml.scaling import scale_panel
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig, run_pseudo_rt_eval_fast


def _quarterly_target(index: pd.DatetimeIndex) -> pd.Series:
    y = pd.Series(np.nan, index=index, name="y")
    q_end = index[index.month.isin([3, 6, 9, 12])]
    y.loc[q_end] = np.arange(1, len(q_end) + 1, dtype=float)
    return y


def _fake_scaled_fit(call_kwargs: list[dict]):
    def fit_fn(Y_monthly, y_quarterly, config, verbose=False, **kwargs):
        call_kwargs.append(dict(kwargs))
        raw = np.column_stack([Y_monthly, y_quarterly])
        _, scaler = scale_panel(raw, mode="internal_per_run")
        n_obs = raw.shape[1]
        C = np.zeros((n_obs, 1), dtype=float)
        C[-1, 0] = 1.0
        return SimpleNamespace(
            A=np.eye(1),
            C=C,
            a_smooth=np.full((raw.shape[0], 1), 0.5, dtype=float),
            scaler=scaler,
            params=object(),
        )

    return fit_fn


def test_pseudort_uses_each_vintage_scaler_not_full_sample_target_moments():
    idx = pd.date_range("2019-01-01", periods=36, freq="MS")
    X = pd.DataFrame({"x": np.linspace(-1.0, 1.0, len(idx))}, index=idx)
    y_a = _quarterly_target(idx)
    y_b = y_a.copy()
    y_b.loc["2021-12-01"] = 1_000_000.0  # unavailable after the evaluation dates

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=1,
        scaling_mode="internal_per_run",
    )
    eval_cfg = EvalConfig(eval_start="2021-01-01", eval_end="2021-02-01")

    calls_a: list[dict] = []
    pred_a, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y_a,
        fit_fn=_fake_scaled_fit(calls_a),
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=True,
    )
    calls_b: list[dict] = []
    pred_b, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y_b,
        fit_fn=_fake_scaled_fit(calls_b),
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=True,
    )

    assert np.allclose(pred_a["pred_raw"], pred_b["pred_raw"])
    assert np.allclose(pred_a["scaler_mu_y"], pred_b["scaler_mu_y"])
    assert np.allclose(pred_a["scaler_sd_y"], pred_b["scaler_sd_y"])
    assert not pred_a["warm_start_used"].any()
    assert all("init_params" not in kwargs for kwargs in calls_a + calls_b)

    expected = pred_a["scaler_mu_y"] + 0.5 * pred_a["scaler_sd_y"]
    assert np.allclose(pred_a["pred_raw"], expected)


def test_fixed_params_internal_scaling_reuses_training_scaler():
    rng = np.random.default_rng(7)
    idx = pd.date_range("2019-01-01", periods=36, freq="MS")
    X = pd.DataFrame(rng.normal(size=(len(idx), 3)), index=idx, columns=["x1", "x2", "x3"])
    y = _quarterly_target(idx)

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=1,
        tol=0.0,
        scaling_mode="internal_per_run",
        P0_mode="steady_state",
    )
    eval_cfg = EvalConfig(eval_start="2021-01-01", eval_end="2021-03-01")

    pred, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=fit_bm_dfm_fast_numba,
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=False,
        fixed_params=True,
        train_end="2020-12-01",
        train_max_iter=1,
        blas_threads=1,
    )

    # With the mandatory one-month GDP release lag, the 2020-Q4 value dated
    # 2020-12 is not yet available in the 2020-12 training vintage.
    y_train = y.loc[:"2020-09-01"].dropna().to_numpy(dtype=float)
    assert np.allclose(pred["scaler_mu_y"], np.mean(y_train))
    assert np.allclose(pred["scaler_sd_y"], np.std(y_train, ddof=0))
    assert np.allclose(
        pred["actual_scaled"],
        (pred["actual_raw"] - np.mean(y_train)) / np.std(y_train, ddof=0),
    )


def test_ar1_update_returns_stationary_variance_from_innovation_residuals():
    Ezz = np.array([2.0, 3.0, 4.0])
    P_lag = np.array([0.0, 1.0, 2.0])
    a = np.zeros(3)

    rho, stationary_var = update_ar1_stationary_moments(Ezz, P_lag, a, min_var=1e-12)

    expected_rho = (1.0 + 2.0) / (2.0 + 3.0)
    q = np.mean(
        [
            3.0 - 2.0 * expected_rho * 1.0 + expected_rho**2 * 2.0,
            4.0 - 2.0 * expected_rho * 2.0 + expected_rho**2 * 3.0,
        ]
    )
    assert rho == pytest.approx(expected_rho)
    assert stationary_var == pytest.approx(q / (1.0 - expected_rho**2))


def test_numba_q_update_uses_post_stability_phi(monkeypatch):
    import dfm_pipeline.dfm_bm_ml.fast.em_fast_numba as em_mod

    rng = np.random.default_rng(11)
    T = 24
    Y = rng.normal(size=(T, 3))
    y = np.full(T, np.nan)
    y[2::3] = rng.normal(size=len(y[2::3]))

    seen_phi: list[np.ndarray] = []
    original_accumulate = em_mod.accumulate_Q_acc

    def force_zero_phi(phi_lags, **kwargs):
        return [np.zeros_like(phi) for phi in phi_lags]

    def capture_q(Ezz, P_lag, a, f0, lag_stack, phi_stack):
        seen_phi.append(np.asarray(phi_stack).copy())
        return original_accumulate(Ezz, P_lag, a, f0, lag_stack, phi_stack)

    monkeypatch.setattr(em_mod, "enforce_var_stability", force_zero_phi)
    monkeypatch.setattr(em_mod, "accumulate_Q_acc", capture_q)

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=1,
        tol=0.0,
        scaling_mode="external_frozen",
        force_var_stability=True,
    )
    fit_bm_dfm_fast_numba(Y, y, cfg)

    assert seen_phi
    assert all(np.allclose(phi, 0.0) for phi in seen_phi)


def test_factor_innovation_covariance_is_positive_semidefinite():
    rng = np.random.default_rng(19)
    T = 30
    Y = rng.normal(size=(T, 5))
    y = np.full(T, np.nan)
    y[2::3] = rng.normal(size=len(y[2::3]))
    cfg = BMDfmConfig(
        r_by_block=(2,),
        p=1,
        max_iter=2,
        scaling_mode="external_frozen",
    )

    res = fit_bm_dfm_fast_numba(Y, y, cfg)
    for Q in res.params.Q_f_blocks:
        assert np.min(np.linalg.eigvalsh(Q)) >= cfg.min_var * 0.99


def test_ppc_must_equal_five():
    with pytest.raises(ValueError, match="must equal 5"):
        BMDfmConfig(r_by_block=(1,), p=1, ppC=6).validate()
