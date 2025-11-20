# tests/test_factors/test_ahn_horenstein.py
import numpy as np
import pandas as pd

from dfm_pipeline.factors.ahn_horenstein import choose_q_ahn_horenstein


def make_synthetic_panel(T=240, N=50, q_true=3, seed=123):
    rng = np.random.default_rng(seed)
    F = rng.standard_normal((T, q_true))
    L = rng.standard_normal((N, q_true))
    signal = F @ L.T
    noise = 0.5 * rng.standard_normal((T, N))
    X = signal + noise
    idx = pd.date_range("1980-01-01", periods=T, freq="MS")
    cols = [f"x{j:03d}" for j in range(N)]
    return pd.DataFrame(X, index=idx, columns=cols)


def test_er_smoke():
    X = make_synthetic_panel()
    res = choose_q_ahn_horenstein(X, r_max=10, variant="ER", standardize=True)
    assert 1 <= res.q_star <= 10
    assert "k" in res.grid.columns and "eig" in res.grid.columns
    assert res.grid["T"].iat[0] == X.shape[0]
    assert res.grid["N"].iat[0] == X.shape[1]


def test_gr_smoke():
    X = make_synthetic_panel(q_true=2)
    res = choose_q_ahn_horenstein(X, r_max=8, variant="GR", standardize=True)
    assert 1 <= res.q_star <= 8
