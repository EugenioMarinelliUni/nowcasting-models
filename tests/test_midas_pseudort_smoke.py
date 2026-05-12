import numpy as np
import pandas as pd

from midas_pipeline import MIDASConfig, MIDASPseudoRTConfig, run_midas_pseudort
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


def test_run_midas_pseudort_smoke():
    idx = pd.date_range("2020-01-01", periods=24, freq="MS")

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
    )

    eval_cfg = MIDASPseudoRTConfig(
        eval_start="2021-01-01",
        eval_end="2021-06-01",
        horizons=("now",),
        drop_invalid_rows=True,
    )

    pred_df, scores = run_midas_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
    )

    assert not pred_df.empty
    assert "eval_date" in pred_df.columns
    assert "target_date" in pred_df.columns
    assert "horizon" in pred_df.columns
    assert "pred" in pred_df.columns
    assert "actual" in pred_df.columns
    assert "pred_raw" in pred_df.columns
    assert "actual_raw" in pred_df.columns
    assert "n_candidate_models" in pred_df.columns
    assert "n_models_used" in pred_df.columns

    assert "rmse" in scores
    assert "mae" in scores
    assert "n" in scores
    assert scores["n"] >= 1

    finite_preds = np.isfinite(pred_df["pred"].to_numpy(dtype=float)).sum()
    assert finite_preds >= 1