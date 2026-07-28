from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.dfm_bm_ml.fast.init_toolbox import prepare_init_panel
from dfm_pipeline.dfm_bm_ml.scaling import PanelScaler
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import (
    EvalConfig,
    _apply_delay_mask,
    _validate_delay_map,
    run_pseudo_rt_eval_fast,
)


def test_numba_entrypoint_fails_explicitly_when_numba_is_missing(monkeypatch):
    import dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba as fit_mod

    monkeypatch.setattr(fit_mod, "NUMBA_AVAILABLE", False)
    X = np.zeros((12, 2), dtype=float)
    y = np.full(12, np.nan)
    y[2::3] = 0.0
    cfg = BMDfmConfig(r_by_block=(1,), p=1, max_iter=1)

    with pytest.raises(RuntimeError, match="call fit_bm_dfm_fast instead"):
        fit_mod.fit_bm_dfm_fast_numba(X, y, cfg)


def test_pca_initialization_fills_monthly_predictors_but_preserves_sparse_gdp():
    Y = np.array(
        [
            [1.0, np.nan, np.nan],
            [np.nan, 2.0, np.nan],
            [3.0, np.nan, 10.0],
            [4.0, 4.0, np.nan],
            [np.nan, 5.0, np.nan],
            [6.0, 6.0, 20.0],
        ],
        dtype=float,
    )
    out = prepare_init_panel(Y, nM=2, method="linear_interp")

    assert np.isfinite(out[:, :2]).all()
    assert np.array_equal(np.isnan(out[:, 2]), np.isnan(Y[:, 2]))
    assert np.allclose(out[np.isfinite(Y[:, 2]), 2], Y[np.isfinite(Y[:, 2]), 2])


def test_json_delay_map_requires_exact_coverage_and_valid_nonnegative_integer_lags():
    cols = ["x1", "x2"]
    assert _validate_delay_map({"x1": 0, "x2": 2}, cols) == {"x1": 0, "x2": 2}

    with pytest.raises(ValueError, match="missing predictors"):
        _validate_delay_map({"x1": 0}, cols)
    with pytest.raises(ValueError, match="unknown predictors"):
        _validate_delay_map({"x1": 0, "x2": 1, "x3": 1}, cols)
    with pytest.raises(TypeError, match="non-negative integer"):
        _validate_delay_map({"x1": 0.0, "x2": 1}, cols)
    with pytest.raises(ValueError, match="non-negative"):
        _validate_delay_map({"x1": -1, "x2": 1}, cols)


def test_json_delay_mask_preserves_natural_missingness_and_adds_only_release_nans():
    idx = pd.date_range("2020-01-01", periods=4, freq="MS")
    X = pd.DataFrame(
        {
            "x0": [1.0, np.nan, 3.0, 4.0],
            "x1": [10.0, 11.0, np.nan, 13.0],
        },
        index=idx,
    )
    out = _apply_delay_mask(
        X,
        eval_date=pd.Timestamp("2020-04-01"),
        delay_style="json_map",
        delay_map={"x0": 0, "x1": 1},
    )

    # Natural missing values remain missing.
    assert np.isnan(out.loc["2020-02-01", "x0"])
    assert np.isnan(out.loc["2020-03-01", "x1"])
    # Lag zero retains April x0; lag one masks April x1.
    assert out.loc["2020-04-01", "x0"] == 4.0
    assert np.isnan(out.loc["2020-04-01", "x1"])
    # The source panel is not modified.
    assert X.loc["2020-04-01", "x1"] == 13.0


def _fake_state_space_fit(Y_monthly, y_quarterly, config, verbose=False, **kwargs):
    n_obs = Y_monthly.shape[1] + 1
    C = np.zeros((n_obs, 1), dtype=float)
    C[-1, 0] = 1.0
    return SimpleNamespace(
        A=np.array([[0.8]], dtype=float),
        Q=np.array([[0.1]], dtype=float),
        C=C,
        R=np.full(n_obs, 0.1, dtype=float),
        a_smooth=np.zeros((Y_monthly.shape[0], 1), dtype=float),
        P_smooth=np.ones((Y_monthly.shape[0], 1, 1), dtype=float),
        scaler=PanelScaler(
            mode="external_frozen",
            mu=np.zeros(n_obs),
            sd=np.ones(n_obs),
        ),
        params=object(),
        converged=True,
        diagnostics={"iterations": 1, "final_loglik": 0.0},
    )


def test_backcast_is_emitted_only_before_previous_quarter_gdp_is_released():
    idx = pd.date_range("2020-01-01", periods=15, freq="MS")
    X = pd.DataFrame({"x": np.arange(len(idx), dtype=float)}, index=idx)
    y = pd.Series(np.nan, index=idx, dtype=float)
    y.loc[idx[idx.month.isin([3, 6, 9, 12])]] = [1.0, 2.0, 3.0, 4.0, 5.0]

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=1,
        scaling_mode="external_frozen",
    )
    eval_cfg = EvalConfig(
        eval_start="2021-01-01",
        eval_end="2021-02-01",
        horizons=("bac",),
        delay_style="none",
        gdp_rel=1,
        vintage_as_of_rule="month_start",
        parameter_mode="recursive",
    )

    pred, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=_fake_state_space_fit,
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=False,
    )

    # Q4 GDP is released during January. It is unavailable at January month-start,
    # so one genuine backcast is produced; by February it is observed and skipped.
    assert list(pd.to_datetime(pred["eval_date"])) == [pd.Timestamp("2021-01-01")]
    assert list(pd.to_datetime(pred["target_date"])) == [pd.Timestamp("2020-12-01")]
    assert not pred["target_available_in_information_set"].any()
    assert (pred["parameter_mode"] == "recursive").all()


def test_fixed_mode_rejects_estimated_initial_state_policy():
    idx = pd.date_range("2020-01-01", periods=15, freq="MS")
    X = pd.DataFrame({"x": np.arange(len(idx), dtype=float)}, index=idx)
    y = pd.Series(np.nan, index=idx, dtype=float)
    y.loc[idx[idx.month.isin([3, 6, 9, 12])]] = [1.0, 2.0, 3.0, 4.0, 5.0]
    cfg = BMDfmConfig(r_by_block=(1,), p=1, P0_mode="estimated")
    eval_cfg = EvalConfig(
        eval_start="2021-01-01",
        eval_end="2021-01-01",
        parameter_mode="fixed",
    )

    with pytest.raises(ValueError, match="DFM-fixed does not support P0_mode='estimated'"):
        run_pseudo_rt_eval_fast(
            X_full=X,
            y_full=y,
            fit_fn=_fake_state_space_fit,
            model_config=cfg,
            eval_cfg=eval_cfg,
            train_end="2020-12-01",
        )


def test_recursive_refits_each_vintage_while_fixed_estimates_once(monkeypatch):
    import dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast as eval_mod

    idx = pd.date_range("2019-01-01", periods=27, freq="MS")
    X = pd.DataFrame({"x": np.arange(len(idx), dtype=float)}, index=idx)
    y = pd.Series(np.nan, index=idx, dtype=float)
    y.loc[idx[idx.month.isin([3, 6, 9, 12])]] = np.arange(
        1, len(idx[idx.month.isin([3, 6, 9, 12])]) + 1, dtype=float
    )
    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        max_iter=1,
        scaling_mode="external_frozen",
        P0_mode="steady_state",
    )

    recursive_calls = {"fit": 0}

    def recursive_fit(*args, **kwargs):
        recursive_calls["fit"] += 1
        return _fake_state_space_fit(*args, **kwargs)

    recursive_cfg = EvalConfig(
        eval_start="2021-01-01",
        eval_end="2021-03-01",
        horizons=("now",),
        parameter_mode="recursive",
    )
    recursive_pred, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=recursive_fit,
        model_config=cfg,
        eval_cfg=recursive_cfg,
        warm_start=False,
    )
    assert recursive_calls["fit"] == 3
    assert (recursive_pred["parameter_mode"] == "recursive").all()

    fixed_calls = {"fit": 0, "smooth": 0}

    def training_fit(*args, **kwargs):
        fixed_calls["fit"] += 1
        return _fake_state_space_fit(*args, **kwargs)

    def fixed_smooth(*, X_v, y_v, **kwargs):
        fixed_calls["smooth"] += 1
        return _fake_state_space_fit(
            X_v.to_numpy(dtype=float),
            y_v.to_numpy(dtype=float),
            cfg,
        )

    monkeypatch.setattr(eval_mod, "_smooth_fixed_params", fixed_smooth)
    fixed_cfg = EvalConfig(
        eval_start="2021-01-01",
        eval_end="2021-03-01",
        horizons=("now",),
        parameter_mode="fixed",
    )
    fixed_pred, _ = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=training_fit,
        model_config=cfg,
        eval_cfg=fixed_cfg,
        train_end="2020-12-01",
        warm_start=True,
    )
    assert fixed_calls == {"fit": 1, "smooth": 3}
    assert (fixed_pred["parameter_mode"] == "fixed").all()
    assert fixed_pred["fixed_params"].all()
    assert not fixed_pred["warm_start_used"].any()
