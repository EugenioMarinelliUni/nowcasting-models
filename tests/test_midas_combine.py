import numpy as np

from midas_pipeline.combine import combine_midas_forecasts


def test_combine_mean():
    records = [
        {"predictor": "a", "pred": 1.0, "in_sample_rmse": 1.0},
        {"predictor": "b", "pred": 3.0, "in_sample_rmse": 2.0},
    ]

    pred, selected = combine_midas_forecasts(records, method="mean")

    assert pred == 2.0
    assert len(selected) == 2
    assert all(r["used_in_combination"] if "used_in_combination" in r else True for r in selected)
    assert np.isclose(sum(r["combination_weight"] for r in selected), 1.0)


def test_combine_median():
    records = [
        {"predictor": "a", "pred": 1.0, "in_sample_rmse": 1.0},
        {"predictor": "b", "pred": 100.0, "in_sample_rmse": 2.0},
        {"predictor": "c", "pred": 3.0, "in_sample_rmse": 2.0},
    ]

    pred, selected = combine_midas_forecasts(records, method="median")

    assert pred == 3.0
    assert len(selected) == 3


def test_combine_top_k():
    records = [
        {"predictor": "a", "pred": 1.0, "in_sample_rmse": 3.0},
        {"predictor": "b", "pred": 10.0, "in_sample_rmse": 1.0},
        {"predictor": "c", "pred": 20.0, "in_sample_rmse": 2.0},
    ]

    pred, selected = combine_midas_forecasts(records, method="top_k", top_k=2)

    assert pred == 15.0
    assert [r["predictor"] for r in selected] == ["b", "c"]
    assert np.isclose(sum(r["combination_weight"] for r in selected), 1.0)


def test_combine_inverse_rmse_uses_validation_rmse_when_available():
    records = [
        {"predictor": "a", "pred": 0.0, "in_sample_rmse": 1.0, "validation_rmse": 10.0},
        {"predictor": "b", "pred": 10.0, "in_sample_rmse": 10.0, "validation_rmse": 1.0},
    ]

    pred, selected = combine_midas_forecasts(records, method="inverse_rmse")

    assert pred > 5.0
    assert len(selected) == 2
    assert np.isclose(sum(r["combination_weight"] for r in selected), 1.0)


def test_combine_empty_returns_nan():
    pred, selected = combine_midas_forecasts([], method="mean")

    assert np.isnan(pred)
    assert selected == []