import pytest

from midas_pipeline.config import MIDASConfig


def test_midas_config_validates_good_config():
    cfg = MIDASConfig(
        predictors=["ip"],
        n_monthly_lags=3,
        n_y_lags=1,
        min_train_rows=10,
        weight_scheme="beta",
        combination="mean",
    )

    cfg.validate()


def test_midas_config_rejects_empty_predictors():
    cfg = MIDASConfig(predictors=[])

    with pytest.raises(ValueError):
        cfg.validate()


def test_midas_config_rejects_bad_weight_scheme():
    cfg = MIDASConfig(predictors=["ip"], weight_scheme="bad")

    with pytest.raises(ValueError):
        cfg.validate()


def test_midas_config_rejects_bad_combination():
    cfg = MIDASConfig(predictors=["ip"], combination="bad")

    with pytest.raises(ValueError):
        cfg.validate()


def test_midas_config_requires_top_k_for_top_k_combination():
    cfg = MIDASConfig(predictors=["ip"], combination="top_k", top_k=None)

    with pytest.raises(ValueError):
        cfg.validate()