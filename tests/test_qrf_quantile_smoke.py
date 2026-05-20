import numpy as np
import pandas as pd
import pytest

pytest.importorskip("quantile_forest")

from qrf_pipeline import QRFConfig, QRFPseudoRTConfig, run_qrf_pseudort
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


def test_run_qrf_quantile_smoke():
    idx = pd.date_range("2020-01-01", periods=30, freq="MS")

    X = pd.DataFrame(
        {
            "ip": np.linspace(0.0, 10.0, len(idx)),
            "pmi": np.linspace(5.0, 15.0, len(idx)),
            "spread": np.sin(np.linspace(0.0, 4.0, len(idx))),
        },
        index=idx,
    )

    y = pd.Series(np.nan, index=idx, name="y")
    quarter_end_idx = [ts for ts in idx if ts.month in (3, 6, 9, 12)]
    for i, ts in enumerate(quarter_end_idx, start=1):
        y.loc[ts] = float(i) + 0.1 * np.sin(i)

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

    model_cfg = QRFConfig(
        predictors=["ip", "pmi", "spread"],
        n_lags=2,
        n_y_lags=1,
        min_train_rows=3,
        n_estimators=20,
        min_samples_leaf=1,
        max_features="sqrt",
        random_state=0,
        backend="qrf",
        quantiles=(0.10, 0.25, 0.50, 0.75, 0.90),
    )

    eval_cfg = QRFPseudoRTConfig(
        eval_start="2021-01-01",
        eval_end="2021-06-01",
        horizons=("now",),
        drop_invalid_rows=True,
    )

    pred_df, scores = run_qrf_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
    )

    assert not pred_df.empty

    for col in ["pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90"]:
        assert col in pred_df.columns

    assert "rmse" in scores
    assert "mae" in scores
    assert "quantile" in scores
    assert "pinball" in scores["quantile"]

    qcols = ["pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90"]
    finite = pred_df.dropna(subset=qcols)

    assert not finite.empty

    assert (finite["pred_q10"] <= finite["pred_q25"]).all()
    assert (finite["pred_q25"] <= finite["pred_q50"]).all()
    assert (finite["pred_q50"] <= finite["pred_q75"]).all()
    assert (finite["pred_q75"] <= finite["pred_q90"]).all()

    assert np.allclose(
        finite["pred"].to_numpy(dtype=float),
        finite["pred_q50"].to_numpy(dtype=float),
    )