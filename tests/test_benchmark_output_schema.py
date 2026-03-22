import pandas as pd

from rt_benchmarks.metrics import compute_basic_metrics


def test_compute_basic_metrics():
    df = pd.DataFrame(
        {
            "actual": [1.0, 2.0, 3.0],
            "pred": [1.1, 1.9, 3.2],
            "horizon": ["bac", "now", "for"],
        }
    )
    out = compute_basic_metrics(df)
    assert "rmse" in out
    assert "mae" in out