import pandas as pd

from rt_benchmarks.sample_builder import (
    SampleBuilderConfig,
    build_current_feature_row,
    build_direct_training_sample,
)
from rt_benchmarks.vintage import VintageConfig


def test_sample_builder_basic():
    idx = pd.date_range("2020-01-01", periods=12, freq="MS")
    X = pd.DataFrame({"ip": range(12)}, index=idx)

    y_idx = pd.to_datetime(["2020-03-01", "2020-06-01", "2020-09-01", "2020-12-01"])
    y = pd.Series([1.0, 2.0, 3.0, 4.0], index=y_idx)

    def feature_fn(Xv, yv, vintage_date, horizon):
        vals = Xv["ip"].dropna()
        if len(vals) < 3:
            return None
        return {"ip_last": float(vals.iloc[-1])}

    cfg = SampleBuilderConfig(
        vintage_cfg=VintageConfig(
            delay_style="none",
            delay_map=None,
            gdp_rel=0,
            no_qe_leak=True,
        )
    )

    X_train, y_train, meta = build_direct_training_sample(
        X_full=X,
        y_full=y,
        eval_date=pd.Timestamp("2020-12-01"),
        horizon="now",
        cfg=cfg,
        feature_fn=feature_fn,
    )

    assert len(X_train) == len(y_train)
    assert meta is not None

    X_now = build_current_feature_row(
        X_full=X,
        y_full=y,
        eval_date=pd.Timestamp("2020-12-01"),
        horizon="now",
        cfg=cfg,
        feature_fn=feature_fn,
    )

    assert X_now is not None
    assert len(X_now) == 1