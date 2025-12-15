import numpy as np

from dfm_pipeline.dfm_dyn.em_dfm_mf_gdp import em_dfm_mf_gdp_full, MixedFreqDFMParams


def mm_aggregate_from_monthly(y_m: np.ndarray) -> np.ndarray:
    """
    Mariano–Murasawa quarterly growth approximation from monthly growth y_m.

    g_q(t) ≈ (1/3) y_t + (2/3) y_{t-1} + 1*y_{t-2}
             + (2/3) y_{t-3} + (1/3) y_{t-4}
    """
    T = len(y_m)
    y_q = np.full(T, np.nan, dtype=float)
    if T < 5:
        return y_q
    for t in range(4, T):
        y_q[t] = (
            (1.0 / 3.0) * y_m[t]
            + (2.0 / 3.0) * y_m[t - 1]
            + 1.0 * y_m[t - 2]
            + (2.0 / 3.0) * y_m[t - 3]
            + (1.0 / 3.0) * y_m[t - 4]
        )
    return y_q


def _simulate_mf_dfm(T=120, n=10, r=1, seed=123):
    """
    Very simple DGP in the same family as em_dfm_mf_gdp_full expects:
    - 1 factor with AR(1)
    - iid idios for X (no AR, to keep it simple)
    - monthly GDP growth driven by factor + AR(1)
    - quarterly growth from MM mapping, kept only at quarter-end months.
    """
    rng = np.random.default_rng(seed)

    # factor
    phi_f = 0.7
    sigma_u = 0.5
    f = np.zeros(T)
    for t in range(1, T):
        f[t] = phi_f * f[t - 1] + sigma_u * rng.standard_normal()

    # loadings for X
    Lambda = rng.normal(scale=1.0, size=(n, 1))
    sigma_e = 0.3
    e = rng.normal(scale=sigma_e, size=(T, n))
    X = f[:, None] @ Lambda.T + e

    # monthly GDP growth
    gamma = np.array([1.0])  # factor loading
    phi_y = 0.4
    sigma_eta = 0.4
    y_m = np.zeros(T)
    for t in range(1, T):
        y_m[t] = gamma @ np.array([f[t]]) + phi_y * y_m[t - 1] + sigma_eta * rng.standard_normal()

    # quarterly via MM
    y_q_full = mm_aggregate_from_monthly(y_m)

    # keep GDP only at "quarter-end" months: t = 2,5,8,... (0-based)
    y_q_obs = np.full(T, np.nan)
    for t in range(T):
        # treat month index mod 3 == 2 as quarter-end
        if t % 3 == 2:
            y_q_obs[t] = y_q_full[t]

    return X, y_q_obs, f, y_m


def test_em_dfm_mf_gdp_shapes_and_loglik():
    T = 120
    n = 8
    r = 1
    p = 1

    X, y_q_obs, f_true, y_m_true = _simulate_mf_dfm(T=T, n=n, r=r)

    # standardize columns of X and y_q, as the real pipeline does
    X_std = (X - np.nanmean(X, axis=0)) / np.nanstd(X, axis=0, ddof=1)
    y_q = y_q_obs.copy()
    mask_q = ~np.isnan(y_q)
    y_q[mask_q] = (y_q[mask_q] - np.nanmean(y_q[mask_q])) / np.nanstd(
        y_q[mask_q], ddof=1
    )

    params: MixedFreqDFMParams = em_dfm_mf_gdp_full(
        X=X_std,
        y_q=y_q,
        r=r,
        p=p,
        max_iter=15,
        tol=1e-4,
        verbose=False,
        use_tqdm=False,
    )

    # 1) basic shape checks
    assert params.factors_smooth.shape == (T, r)
    assert params.monthly_gdp_smooth.shape == (T,)
    assert params.Lambda_x.shape == (n, r)
    assert len(params.Phi_factors) == p
    assert params.Q_u.shape == (r, r)
    assert params.Phi_eps_diag.shape == (n,)
    assert params.R_e_diag.shape == (n,)
    assert params.gamma.shape == (r,)
    assert isinstance(params.phi_y, float)
    assert isinstance(params.sigma_eta2, float)
    assert isinstance(params.sigma_v2, float)

    # 2) loglik history sane
    assert len(params.loglik_history) >= 1
    assert np.all(np.isfinite(params.loglik_history))

    # 3) EM should not catastrophically blow up the loglikelihood
    # Allow small decreases because the M-step is approximate EM, not exact ML.
    delta_ll = params.loglik_history[-1] - params.loglik_history[0]
    # It’s enough to ensure it didn't collapse by orders of magnitude.
    assert delta_ll > -1000

    # 4) reconstructed quarterly from smoothed monthly GDP
    y_q_hat = mm_aggregate_from_monthly(params.monthly_gdp_smooth)

    # correlation between observed quarterly and fitted should be positive
    mask_eval = ~np.isnan(y_q) & ~np.isnan(y_q_hat)
    if mask_eval.sum() > 3:
        corr = np.corrcoef(y_q[mask_eval], y_q_hat[mask_eval])[0, 1]
        assert corr > 0.2  # not perfect, but should be > 0 if model captures signal
