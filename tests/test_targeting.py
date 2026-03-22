import pandas as pd

from rt_benchmarks.targeting import month_of_quarter


def test_month_of_quarter():
    assert month_of_quarter(pd.Timestamp("2020-01-01")) == 1
    assert month_of_quarter(pd.Timestamp("2020-02-01")) == 2
    assert month_of_quarter(pd.Timestamp("2020-03-01")) == 3