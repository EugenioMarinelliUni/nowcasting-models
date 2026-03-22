import pandas as pd

from qrf_pipeline.feature_builder import extract_qrf_features


def test_extract_qrf_features_basic():
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    X = pd.DataFrame({"ip": range(12), "pmi": range(10, 22)}, index=idx)
    y = pd.Series([1.0, 2.0, 3.0, 4.0], index=pd.date_range("2020-03-01", periods=4, freq="QS"))
    out = extract_qrf_features(X, y, idx[-1], predictors=["ip", "pmi"], n_lags=3, n_y_lags=2)
    assert out is not None