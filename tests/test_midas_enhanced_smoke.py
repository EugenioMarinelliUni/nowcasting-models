import numpy as np
import pandas as pd

from midas_pipeline import MIDASConfig, MIDASPseudoRTConfig, run_midas_pseudort
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


def test_run_midas_enhanced_smoke():
    idx = pd.date_range("2020-01-01", periods=30, freq="MS")

    X = pd.DataFrame(
        {
            "ip": np.linspace(0.0, 10.0, len(idx)),
            "pmi": np.linspace(5.0, 15.0, len(idx)),
        },
        index=idx,
    )

    y = pd.Series(np.nan, index=idx, name="y")
    quarter_end_idx = [ts for ts in idx if ts.month in (3, 6, 9, 12)]
    for i, ts in enumerate(quarter_end_idx, start=1):
        y.loc[ts] = float(i)

    vintage_cfg = VintageConfig(
        delay_style="none",
        delay_map=None,
        gdp_rel=0,
        no_qe_leak=True,
    )

    sample_cfg = SampleBuilderConfig(
        vintage_cfg=vintage_cfg,
        min_train_rows=3,
        include_row_metadata=True,
    )

    model_cfg = MIDASConfig(
        predictors=["ip", "pmi"],
        n_monthly_lags=2,
        n_y_lags=1,
        min_train_rows=3,
        weight_scheme="exp_almon",
        combination="inverse_rmse",
        validation_tail_rows=1,
        max_iter=30,
        n_starts=1,
        warm_start=True,
    )

    eval_cfg = MIDASPseudoRTConfig(
        eval_start="2021-01-01",
        eval_end="2021-06-01",
        horizons=("now",),
        drop_invalid_rows=True,
        show_progress=False,
    )

    pred_df, scores, diagnostics_df, lag_weights_df, predictor_forecasts_df = run_midas_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
        return_details=True,
    )

    assert not pred_df.empty
    assert "rmse" in scores
    assert "mae" in scores

    assert not diagnostics_df.empty
    assert "converged" in diagnostics_df.columns
    assert "actual_weight_scheme" in diagnostics_df.columns
    assert "in_sample_rmse" in diagnostics_df.columns
    assert "validation_rmse" in diagnostics_df.columns
    assert "used_in_combination" in diagnostics_df.columns
    assert "combination_weight" in diagnostics_df.columns

    assert not lag_weights_df.empty
    assert "lag_index" in lag_weights_df.columns
    assert "weight" in lag_weights_df.columns

    assert not predictor_forecasts_df.empty
    assert "pred" in predictor_forecasts_df.columns
    assert "validation_rmse" in predictor_forecasts_df.columns
    assert "used_in_combination" in predictor_forecasts_df.columns

    finite_preds = np.isfinite(pred_df["pred"].to_numpy(dtype=float)).sum()
    assert finite_preds >= 1