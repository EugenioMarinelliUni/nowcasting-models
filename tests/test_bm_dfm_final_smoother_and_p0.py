from types import SimpleNamespace

import numpy as np

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams, build_state_space


def _small_bm_panel(T=24, nM=4, seed=0):
    rng = np.random.default_rng(seed)
    Y = rng.normal(size=(T, nM))
    y = np.full(T, np.nan, dtype=float)
    # Quarter-end months in a synthetic monthly index starting in January:
    # 0-based positions 2, 5, 8, ...
    y[2::3] = rng.normal(size=len(y[2::3]))
    return Y, y


def test_fit_numba_returns_post_em_final_smoother(monkeypatch):
    import dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba as fit_mod

    Y, y = _small_bm_panel()
    sentinel = 123.456
    calls = []

    def fake_final_smoother(Y_obs, ss):
        # This monkeypatch only replaces fit_fast_numba.kalman_filter_smoother,
        # not em_fast_numba.kalman_filter_smoother. Therefore it is called by
        # the final post-EM smoother, after the EM loop has completed.
        n_state = int(ss.T.shape[0])
        Tn = int(np.asarray(Y_obs).shape[0])
        calls.append((Tn, n_state))
        return SimpleNamespace(
            a_smooth=np.full((Tn, n_state), sentinel, dtype=float),
            P_smooth=np.full((Tn, n_state, n_state), 2.0, dtype=float),
            P_lag_smooth=np.full((Tn, n_state, n_state), 3.0, dtype=float),
        )

    monkeypatch.setattr(fit_mod, "kalman_filter_smoother", fake_final_smoother)

    cfg = BMDfmConfig(
        r_by_block=(1,),
        p=1,
        n_quarterly=1,
        max_iter=1,
        tol=0.0,
        scaling_mode="external_frozen",
        P0_mode="steady_state",
        update_initial_state_each_iter=True,
    )

    res = fit_mod.fit_bm_dfm_fast_numba(Y, y, cfg, verbose=False)

    assert len(calls) == 1
    assert calls[0] == (Y.shape[0], res.A.shape[0])
    assert np.all(res.a_smooth == sentinel)
    assert np.all(res.P_smooth == 2.0)
    assert np.all(res.P_lag_smooth == 3.0)
    assert res.C.shape[0] == Y.shape[1] + 1
    assert res.A.shape[0] == res.A.shape[1]


def test_P0_mode_steady_state_factor_diffuse_idio_is_implemented():
    params = BMParams(
        Phi_blocks=[[np.array([[0.5]], dtype=float)]],
        Q_f_blocks=[np.array([[1.0]], dtype=float)],
        rho_m=np.array([0.2, -0.1], dtype=float),
        sig2_m=np.array([2.0, 3.0], dtype=float),
        rho_q=np.array([0.1], dtype=float),
        sig2_q=np.array([4.0], dtype=float),
        Lambda_m=np.array([[1.0], [0.5]], dtype=float),
        Lambda_q=np.array([[0.8]], dtype=float),
        R_diag_m=np.array([1e-4, 1e-4], dtype=float),
        R_diag_q=np.array([1e-4], dtype=float),
    )

    A, Q, C, R, a0, P0, idx = build_state_space(
        params=params,
        nM=2,
        nQ=1,
        r_by_block=(1,),
        p=1,
        ppC=5,
        mm_style="toolbox",
        quarterly_meas_var_floor=1e-4,
        idio_ar1=True,
        jitter=1e-12,
        P0_mode="steady_state_factor_diffuse_idio",
    )

    assert A.shape == Q.shape == P0.shape
    assert C.shape == (3, A.shape[0])
    assert R.shape == (3, 3)
    assert a0.shape == (A.shape[0],)
    assert np.isfinite(P0).all()

    factor_diag = np.diag(P0[idx.idx_factors, idx.idx_factors])
    monthly_idio_diag = np.diag(P0[idx.idx_idio_monthly, idx.idx_idio_monthly])
    quarterly_idio_diag = np.diag(P0[idx.idx_idio_quarterly, idx.idx_idio_quarterly])

    assert np.all(factor_diag > 0.0)
    assert not np.any(np.isclose(factor_diag, 10000.0))
    assert np.allclose(monthly_idio_diag, 10000.0)
    assert np.allclose(quarterly_idio_diag, 10000.0)
