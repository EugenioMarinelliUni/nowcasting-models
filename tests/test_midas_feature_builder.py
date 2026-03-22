import pandas as pd

from midas_pipeline.feature_builder import extract_midas_features


def test_extract_midas_features_basic():
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    X = pd.DataFrame({"ip": range(12)}, index=idx)
    y = pd.Series([1.0, 2.0, 3.0, 4.0], index=pd.date_range("2020-03-01", periods=4, freq="QS"))
    out = extract_midas_features(X, y, predictor="ip", n_monthly_lags=6, n_y_lags=2)
    assert out is not None