import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba import fit_bm_dfm_fast_numba
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig, run_pseudo_rt_eval_fast
from dfm_pipeline.preprocessing.bm_inputs import (
    load_bm_inputs_from_csv,
    validate_quarter_end_target_alignment,
)


def test_quarter_end_target_alignment_accepts_mar_jun_sep_dec():
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    y = pd.Series(np.nan, index=idx)
    y.loc[["2020-03-01", "2020-06-01", "2020-09-01", "2020-12-01"]] = [1.0, 2.0, 3.0, 4.0]

    validate_quarter_end_target_alignment(y, name="y")


def test_quarter_end_target_alignment_rejects_quarter_start_observations():
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    y = pd.Series(np.nan, index=idx)
    y.loc[["2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01"]] = [1.0, 2.0, 3.0, 4.0]

    with pytest.raises(ValueError, match="outside quarter-end months"):
        validate_quarter_end_target_alignment(y, name="bad_y")


def test_load_bm_inputs_can_require_quarter_end_target(tmp_path):
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    X = pd.DataFrame({"date": idx, "x1": np.arange(12, dtype=float), "x2": np.arange(12, dtype=float) + 1.0})

    y_bad = pd.DataFrame({"date": idx, "y": np.nan})
    y_bad.loc[y_bad["date"].dt.month.isin([1, 4, 7, 10]), "y"] = [1.0, 2.0, 3.0, 4.0]

    x_path = tmp_path / "X.csv"
    y_path = tmp_path / "y_bad.csv"
    X.to_csv(x_path, index=False)
    y_bad.to_csv(y_path, index=False)

    with pytest.raises(ValueError, match="outside quarter-end months"):
        load_bm_inputs_from_csv(
            str(x_path),
            str(y_path),
            monthly_date_col="date",
            quarterly_date_col="date",
            quarterly_value_col="y",
            require_quarter_end_target=True,
        )


def test_fixed_params_pseudort_path_uses_matrix_api_and_returns_predictions():
    rng = np.random.default_rng(123)
    idx = pd.date_range("2020-01-01", periods=24, freq="MS")

    X = pd.DataFrame(
        rng.normal(size=(len(idx), 3)),
        index=idx,
        columns=["x1", "x2", "x3"],
    )

    y = pd.Series(np.nan, index=idx, name="y")
    q_end = y.index[y.index.month.isin([3, 6, 9, 12])]
    y.loc[q_end] = rng.normal(size=len(q_end))

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        n_quarterly=1,
        max_iter=2,
        tol=1e9,
        scaling_mode="external_frozen",
        P0_mode="steady_state",
        update_initial_state_each_iter=True,
    )

    eval_cfg = EvalConfig(
        eval_start="2021-01-01",
        eval_end="2021-03-01",
        no_qe_leak=True,
    )

    pred_df, scores = run_pseudo_rt_eval_fast(
        X_full=X,
        y_full=y,
        fit_fn=fit_bm_dfm_fast_numba,
        model_config=cfg,
        eval_cfg=eval_cfg,
        warm_start=False,
        fixed_params=True,
        train_end="2020-12-01",
        train_max_iter=2,
        blas_threads=1,
    )

    assert not pred_df.empty
    assert pred_df["fixed_params"].all()
    assert (pred_df["parameter_mode"] == "fixed").all()
    assert set(pred_df["moq"]) == {1, 2, 3}
    assert np.isfinite(pred_df["pred"].to_numpy(dtype=float)).all()
    assert np.isfinite(pred_df["actual"].to_numpy(dtype=float)).all()
    assert "rmse" in scores
    assert np.isfinite(scores["rmse"])
