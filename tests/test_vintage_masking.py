import pandas as pd

from rt_benchmarks.vintage import apply_delay_mask


def test_apply_delay_mask_none():
    idx = pd.date_range("2020-01-01", periods=3, freq="MS")
    X = pd.DataFrame({"a": [1.0, 2.0, 3.0]}, index=idx)
    out = apply_delay_mask(X, idx[-1], delay_style="none", delay_map=None)
    assert out.shape == X.shape