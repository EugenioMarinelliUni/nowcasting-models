from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.dfm_bm_ml.constraints import constrained_ls, toolbox_R_mat
from dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba import fit_bm_dfm_fast_numba
from dfm_pipeline.dfm_bm_ml.identification import identify_signs
from dfm_pipeline.dfm_bm_ml.estimation import factor_var_is_stable
from dfm_pipeline.dfm_bm_ml.scaling import PanelScaler, TargetOutputScaler
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig, run_pseudo_rt_eval_fast
from dfm_pipeline.eval_pseudort.vintages import LongFormatVintageProvider


def _params_two_factor() -> BMParams:
    return BMParams(
        Phi_blocks=[[np.array([[0.5, 0.1], [0.0, 0.4]])]],
        Q_f_blocks=[np.array([[1.0, 0.2], [0.2, 0.8]])],
        rho_m=np.zeros(3),
        sig2_m=np.ones(3),
        rho_q=np.zeros(1),
        sig2_q=np.ones(1),
        Lambda_m=np.array([[-2.0, 0.2], [0.5, -3.0], [0.1, 0.4]]),
        Lambda_q=np.array([[-0.7, 1.2]]),
        R_diag_m=np.full(3, 0.1),
        R_diag_q=np.full(1, 0.1),
    )


def test_default_initial_state_policy_is_stationary_and_coherent():
    cfg = BMDfmConfig(r_by_block=(1,), p=1)
    cfg.validate()
    assert cfg.P0_mode == "steady_state"
    assert not cfg.update_initial_state_each_iter

    legacy = BMDfmConfig(
        r_by_block=(1,), p=1, P0_mode="steady_state", update_initial_state_each_iter=True
    )
    legacy.validate()
    assert legacy.P0_mode == "estimated"
    assert legacy.update_initial_state_each_iter


def test_exact_constrained_ml_is_explicitly_reserved():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(18, 2))
    y = np.full(18, np.nan)
    y[2::3] = rng.normal(size=6)
    cfg = BMDfmConfig(
        r_by_block=(1,), p=1, max_iter=1, estimation_mode="exact_constrained_ml"
    )
    with pytest.raises(NotImplementedError, match="exact_constrained_ml"):
        fit_bm_dfm_fast_numba(X, y, cfg)


def test_sign_identification_preserves_common_component():
    params = _params_two_factor()
    identified, info = identify_signs(params, r_by_block=(2,))
    assert np.all(identified.Lambda_m[info.anchor_indices, np.arange(2)] >= 0.0)

    f_old = np.array([0.4, -0.7])
    f_new = info.signs * f_old
    assert np.allclose(params.Lambda_m @ f_old, identified.Lambda_m @ f_new)
    assert np.allclose(params.Lambda_q @ f_old, identified.Lambda_q @ f_new)


def test_kkt_quarterly_loading_solver_satisfies_constraints():
    D = np.diag([2.0, 3.0, 4.0, 5.0, 6.0])
    n = np.array([1.0, -2.0, 0.5, 1.5, -0.4])
    R, q = toolbox_R_mat()
    c = constrained_ls(D, n, R, q)
    assert np.max(np.abs(R @ c - q)) < 1e-10


def _quarterly_target(index: pd.DatetimeIndex) -> pd.Series:
    y = pd.Series(np.nan, index=index, dtype=float)
    qends = index[index.month.isin([3, 6, 9, 12])]
    y.loc[qends] = np.linspace(-1.0, 1.0, len(qends))
    return y


def _probabilistic_fake_fit(Y_monthly, y_quarterly, config, verbose=False, **kwargs):
    T = len(y_quarterly)
    n_obs = Y_monthly.shape[1] + 1
    C = np.zeros((n_obs, 1))
    C[-1, 0] = 1.0
    return SimpleNamespace(
        A=np.array([[0.8]]),
        Q=np.array([[0.25]]),
        C=C,
        R=np.diag(np.r_[np.zeros(n_obs - 1), 0.04]),
        a_smooth=np.full((T, 1), 0.5),
        P_smooth=np.full((T, 1, 1), 0.36),
        scaler=PanelScaler("external_frozen", np.zeros(n_obs), np.ones(n_obs)),
        params=object(),
        converged=True,
        loglik_trace=[-1.0, -0.9],
        diagnostics={"iterations": 2, "final_loglik": -0.9, "minimum_loglik_increment": 0.1},
    )


def test_predictive_intervals_and_explicit_output_scaling():
    idx = pd.date_range("2019-01-01", periods=36, freq="MS")
    X = pd.DataFrame({"x": np.arange(len(idx), dtype=float)}, index=idx)
    y = _quarterly_target(idx)
    cfg = BMDfmConfig(r_by_block=(1,), p=1)
    eval_cfg = EvalConfig(
        eval_start="2021-03-01",
        eval_end="2021-03-01",
        prediction_interval_levels=(0.90,),
    )
    pred, scores = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=_probabilistic_fake_fit,
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=False,
        target_output_scaler=TargetOutputScaler(mean=10.0, std=2.0, label="GDP"),
    )
    row = pred.iloc[0]
    assert row["pred_model_scale"] == pytest.approx(0.5)
    assert row["pred_raw"] == pytest.approx(11.0)
    assert row["pred_sd_raw"] > 0.0
    assert row["pi90_lower_raw"] < row["pred_raw"] < row["pi90_upper_raw"]
    assert "by_moq" in scores


def test_long_format_vintage_provider_selects_latest_available_release():
    data = pd.DataFrame(
        {
            "series": ["x", "x", "gdp", "gdp"],
            "reference_date": ["2020-01-01", "2020-01-01", "2020-03-01", "2020-03-01"],
            "vintage_date": ["2020-02-01", "2020-04-01", "2020-04-15", "2020-05-15"],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    provider = LongFormatVintageProvider(data, target_series="gdp", predictor_order=["x"])
    early = provider.get_vintage(pd.Timestamp("2020-04-30"))
    late = provider.get_vintage(pd.Timestamp("2020-05-31"))
    assert early.X.loc[pd.Timestamp("2020-01-01"), "x"] == pytest.approx(2.0)
    assert early.y.loc[pd.Timestamp("2020-03-01")] == pytest.approx(3.0)
    assert late.y.loc[pd.Timestamp("2020-03-01")] == pytest.approx(4.0)


def test_projected_gem_trace_is_monotone_on_small_panel():
    rng = np.random.default_rng(22)
    T = 30
    f = np.zeros(T)
    for t in range(1, T):
        f[t] = 0.6 * f[t - 1] + rng.normal(scale=0.4)
    X = np.column_stack([f + rng.normal(scale=0.2, size=T), -0.5 * f + rng.normal(scale=0.3, size=T)])
    y = np.full(T, np.nan)
    y[2::3] = f[2::3] + rng.normal(scale=0.2, size=len(f[2::3]))

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=5,
        tol=0.0,
        scaling_mode="internal_per_run",
        gem_likelihood_guard=True,
    )
    res = fit_bm_dfm_fast_numba(X, y, cfg)
    diffs = np.diff(np.asarray(res.loglik_trace))
    assert np.all(diffs >= -cfg.gem_ll_tolerance - 1e-10)
    assert res.diagnostics["likelihood_guard"] is True


def test_gem_feasibility_check_rejects_unstable_var():
    stable = _params_two_factor()
    assert factor_var_is_stable(stable, ppC=5)

    unstable = BMParams(
        Phi_blocks=[[np.array([[1.2, 0.0], [0.0, 1.1]])]],
        Q_f_blocks=stable.Q_f_blocks,
        rho_m=stable.rho_m,
        sig2_m=stable.sig2_m,
        rho_q=stable.rho_q,
        sig2_q=stable.sig2_q,
        Lambda_m=stable.Lambda_m,
        Lambda_q=stable.Lambda_q,
        R_diag_m=stable.R_diag_m,
        R_diag_q=stable.R_diag_q,
    )
    assert not factor_var_is_stable(unstable, ppC=5)
