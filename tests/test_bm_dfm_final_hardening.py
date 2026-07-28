from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.dfm_bm_ml.constraints import (
    constrained_ls,
    mm_proportionality_R_mat,
    mm_weights,
)
from dfm_pipeline.dfm_bm_ml.fast.fit_fast import fit_bm_dfm_fast
from dfm_pipeline.dfm_bm_ml.fast.init import init_params_pca
from dfm_pipeline.dfm_bm_ml.identification import identify_anchor_triangular
from dfm_pipeline.dfm_bm_ml.scaling import PanelScaler
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import (
    EvalConfig,
    run_pseudo_rt_eval_fast,
)


def _two_factor_params() -> BMParams:
    return BMParams(
        Phi_blocks=[[np.array([[0.5, 0.1], [0.0, 0.4]])]],
        Q_f_blocks=[np.array([[1.0, 0.2], [0.2, 0.8]])],
        rho_m=np.zeros(4),
        sig2_m=np.ones(4),
        rho_q=np.zeros(1),
        sig2_q=np.ones(1),
        Lambda_m=np.array(
            [
                [1.0, 2.0],
                [2.0, -0.5],
                [-0.2, 1.5],
                [0.7, 0.1],
            ]
        ),
        Lambda_q=np.array([[0.5, -0.7]]),
        R_diag_m=np.full(4, 0.1),
        R_diag_q=np.full(1, 0.1),
    )


def _quarterly_target(index: pd.DatetimeIndex) -> pd.Series:
    y = pd.Series(np.nan, index=index, dtype=float)
    qends = index[index.month.isin([3, 6, 9, 12])]
    y.loc[qends] = np.linspace(-1.0, 1.0, len(qends))
    return y


def _fake_result(*, converged: bool):
    return SimpleNamespace(
        A=np.array([[0.8]]),
        Q=np.array([[0.2]]),
        C=np.array([[0.0], [1.0]]),
        R=np.array([1e-4, 0.1]),
        a_smooth=np.zeros((24, 1)),
        P_smooth=np.ones((24, 1, 1)) * 0.2,
        scaler=PanelScaler("external_frozen", np.zeros(2), np.ones(2)),
        params=object(),
        converged=converged,
        loglik_trace=[-2.0, -1.0],
        diagnostics={
            "iterations": 2,
            "final_loglik": -1.0,
            "minimum_loglik_increment": 1.0,
            "rejected_steps": 0,
        },
    )


def test_scaled_mm_constraints_are_supported_and_proportional():
    R, q = mm_proportionality_R_mat("scaled")
    w = mm_weights("scaled")
    c = np.concatenate(([2.3 * w[0]], 2.3 * w[1:]))
    assert np.allclose(R @ c, q)

    D = np.diag(np.linspace(1.0, 2.0, 5))
    n = np.array([0.4, -0.2, 0.7, 0.3, -0.1])
    solution = constrained_ls(D, n, R, q)
    assert np.max(np.abs(R @ solution - q)) < 1e-10

    rng = np.random.default_rng(3)
    X = rng.normal(size=(36, 3))
    y = np.full(36, np.nan)
    y[2::3] = rng.normal(size=12)
    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        mm_weight_style="scaled",
        enforce_quarterly_loading_constraint=True,
        max_iter=2,
        tol=1e9,
    )
    result = fit_bm_dfm_fast(X, y, cfg)
    assert np.isfinite(result.diagnostics["final_recomputed_loglik"])


def test_ar1_initialisation_uses_stationary_residual_variance():
    rng = np.random.default_rng(10)
    X = rng.normal(size=(48, 4))
    y = np.full(48, np.nan)
    y[2::3] = rng.normal(size=16)
    Y = np.column_stack([X, y])
    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        rho_idio_init=0.5,
        monthly_meas_var_floor=1e-4,
        scaling_mode="external_frozen",
    )
    params = init_params_pca(Y, nM=4, config=cfg)
    # The stored quantity is a stationary variance estimated from PCA residuals,
    # not the old innovation-like constant 1-rho^2.
    assert np.all(params.sig2_m > 0.0)
    assert not np.allclose(params.sig2_m, 1.0 - cfg.rho_idio_init**2)


def test_anchor_triangular_identification_removes_rotation_and_preserves_common_component():
    params = _two_factor_params()
    identified, info = identify_anchor_triangular(params, r_by_block=(2,))
    anchors = info.anchor_indices
    anchor_loadings = identified.Lambda_m[anchors, :]
    assert np.max(np.abs(np.triu(anchor_loadings, k=1))) < 1e-8
    assert np.all(np.diag(anchor_loadings) > 0.0)

    H = info.rotations[0]
    f_old = np.array([0.4, -0.7])
    f_new = H.T @ f_old
    assert np.allclose(params.Lambda_m @ f_old, identified.Lambda_m @ f_new)
    assert np.allclose(params.Lambda_q @ f_old, identified.Lambda_q @ f_new)


def test_sign_anchor_is_rejected_for_multifactor_blocks():
    cfg = BMDfmConfig(
        r_by_block=(2,),
        p=1,
        identification_mode="sign_anchor",
    )
    with pytest.raises(ValueError, match="rotational ambiguity"):
        cfg.validate()


def test_removed_require_convergence_flag_is_rejected():
    cfg = EvalConfig(
        eval_start="2020-01-01",
        eval_end="2020-03-01",
        require_convergence=False,
    )
    with pytest.raises(ValueError, match="was removed"):
        cfg.validate()


@pytest.mark.parametrize("policy, expected_rows", [("skip", 0), ("keep", 3)])
def test_nonconvergence_policy_is_always_enforced(policy: str, expected_rows: int):
    idx = pd.date_range("2019-01-01", periods=24, freq="MS")
    X = pd.DataFrame({"x": np.arange(24, dtype=float)}, index=idx)
    y = _quarterly_target(idx)

    def fake_fit(Y_monthly, y_quarterly, config, **kwargs):
        result = _fake_result(converged=False)
        T = len(y_quarterly)
        result.a_smooth = np.zeros((T, 1))
        result.P_smooth = np.ones((T, 1, 1)) * 0.2
        return result

    model_cfg = BMDfmConfig(r_by_block=(1,), p=1)
    eval_cfg = EvalConfig(
        eval_start="2020-01-01",
        eval_end="2020-03-01",
        on_nonconvergence=policy,
    )
    pred, _scores = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=fake_fit,
        model_config=model_cfg,
        eval_cfg=eval_cfg,
        warm_start=False,
    )
    assert len(pred) == expected_rows
    if policy == "keep":
        assert not pred["em_converged"].any()
        assert set(pred["on_nonconvergence"]) == {"keep"}


def test_nonconvergence_raise_policy_raises():
    idx = pd.date_range("2019-01-01", periods=24, freq="MS")
    X = pd.DataFrame({"x": np.arange(24, dtype=float)}, index=idx)
    y = _quarterly_target(idx)

    def fake_fit(Y_monthly, y_quarterly, config, **kwargs):
        result = _fake_result(converged=False)
        T = len(y_quarterly)
        result.a_smooth = np.zeros((T, 1))
        result.P_smooth = np.ones((T, 1, 1)) * 0.2
        return result

    with pytest.raises(RuntimeError, match="did not converge"):
        run_pseudo_rt_eval_fast(
            X_full=X,
            y_full=y,
            fit_fn=fake_fit,
            model_config=BMDfmConfig(r_by_block=(1,), p=1),
            eval_cfg=EvalConfig(
                eval_start="2020-01-01",
                eval_end="2020-03-01",
                on_nonconvergence="raise",
            ),
            warm_start=False,
        )


def test_fixed_mode_rejects_train_end_at_or_after_eval_start():
    idx = pd.date_range("2019-01-01", periods=30, freq="MS")
    X = pd.DataFrame({"x": np.arange(30, dtype=float)}, index=idx)
    y = _quarterly_target(idx)

    with pytest.raises(ValueError, match="earlier than eval_start"):
        run_pseudo_rt_eval_fast(
            X_full=X,
            y_full=y,
            fit_fn=lambda **kwargs: _fake_result(converged=True),
            model_config=BMDfmConfig(r_by_block=(1,), p=1),
            eval_cfg=EvalConfig(
                eval_start="2020-01-01",
                eval_end="2020-03-01",
                parameter_mode="fixed",
            ),
            train_end="2020-01-01",
            warm_start=False,
        )


def test_fixed_mode_refuses_nonconverged_training_even_with_keep_policy():
    idx = pd.date_range("2018-01-01", periods=36, freq="MS")
    X = pd.DataFrame({"x": np.arange(36, dtype=float)}, index=idx)
    y = _quarterly_target(idx)

    def fake_fit(Y_monthly, y_quarterly, config, **kwargs):
        result = _fake_result(converged=False)
        T = len(y_quarterly)
        result.a_smooth = np.zeros((T, 1))
        result.P_smooth = np.ones((T, 1, 1)) * 0.2
        return result

    with pytest.raises(RuntimeError, match="refusing to freeze"):
        run_pseudo_rt_eval_fast(
            X_full=X,
            y_full=y,
            fit_fn=fake_fit,
            model_config=BMDfmConfig(r_by_block=(1,), p=1),
            eval_cfg=EvalConfig(
                eval_start="2020-01-01",
                eval_end="2020-03-01",
                parameter_mode="fixed",
                on_nonconvergence="keep",
            ),
            train_end="2019-12-01",
            warm_start=False,
        )


def test_all_legacy_dfm_scripts_are_disabled():
    root = Path(__file__).resolve().parents[1]
    legacy_scripts = sorted((root / "scripts" / "dfm").glob("*.py")) + sorted(
        (root / "scripts" / "dfm_bm").glob("*.py")
    )
    assert legacy_scripts
    for script in legacy_scripts:
        text = script.read_text(encoding="utf-8")
        assert "This legacy DFM entry point is disabled" in text
        assert "scripts/dfm_bm_ml" in text
