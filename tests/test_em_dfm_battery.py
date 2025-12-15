import numpy as np

from dfm_pipeline.dfm_dyn.em_dfm import em_dfm_full


def _simulate_dfm(
    T: int,
    n: int,
    r: int,
    phi_scalar: float = 0.5,
    seed: int = 0,
):
    """
    Simple DFM generator:

        f_t = phi * f_{t-1} + u_t
        x_t = Lambda f_t + e_t

    r is kept small; for recovery tests we use r=1.
    """
    rng = np.random.default_rng(seed)

    Phi_true = [phi_scalar * np.eye(r)]
    Q_u_true = 0.1 * np.eye(r)
    Lambda_true = rng.normal(scale=1.0, size=(n, r))
    R_true = 0.05 * np.eye(n)

    f_true = np.zeros((T, r))
    for t in range(1, T):
        f_true[t] = (
            Phi_true[0] @ f_true[t - 1]
            + rng.multivariate_normal(np.zeros(r), Q_u_true)
        )

    X = np.empty((T, n))
    for t in range(T):
        X[t] = (
            Lambda_true @ f_true[t]
            + rng.multivariate_normal(np.zeros(n), R_true)
        )

    return X, f_true, Phi_true, Lambda_true, Q_u_true, R_true


def test_em_dfm_sanity_basic():
    """
    End-to-end smoke test:
    - synthetic DFM
    - random missingness
    - check shapes, PD-ness, finite outputs
    """
    T, n, r, p = 200, 5, 2, 1

    X, f_true, Phi_true, Lambda_true, Q_u_true, R_true = _simulate_dfm(
        T=T, n=n, r=r, phi_scalar=0.5, seed=0
    )

    rng = np.random.default_rng(1)
    mask = rng.random(X.shape) < 0.1
    X[mask] = np.nan

    params = em_dfm_full(
        X=X,
        r=r,
        p=p,
        max_iter=20,
        tol=1e-4,
        verbose=False,
        use_tqdm=False,
    )

    # shapes
    assert params.factors_smooth.shape == (T, r)
    assert params.states_smooth.shape == (T, r * p)
    assert params.Lambda.shape == (n, r)
    assert params.R.shape == (n, n)

    # finite numbers
    assert np.isfinite(params.factors_smooth).all()
    assert np.isfinite(params.Lambda).all()
    assert np.isfinite(params.R).all()

    # R positive definite
    eigvals_R = np.linalg.eigvalsh(params.R)
    assert np.all(eigvals_R > 0), eigvals_R

    # some likelihood history
    assert len(params.loglik_history) >= 1


def test_em_dfm_loglik_monotone():
    """
    EM log-likelihood should be (almost) non-decreasing.
    Allow tiny numerical noise.
    """
    T, n, r, p = 150, 5, 2, 1

    X, *_ = _simulate_dfm(T=T, n=n, r=r, phi_scalar=0.5, seed=2)

    params = em_dfm_full(
        X=X,
        r=r,
        p=p,
        max_iter=40,
        tol=0.0,          # force max_iter to see the path
        verbose=False,
        use_tqdm=False,
    )

    ll = np.asarray(params.loglik_history, float)
    assert ll.ndim == 1
    assert ll.size >= 2

    diffs = np.diff(ll)
    # ignore first step; allow small negative tolerance for numerical issues
    assert np.all(diffs[1:] >= -1e-3), diffs


def test_em_dfm_recovery_r1():
    """
    Recovery test for r=1 where identification is straightforward:
    - simulate 1-factor DFM
    - run EM
    - check high correlation between true and estimated factor,
      and between true and estimated loadings (up to sign).
    """
    T, n, r, p = 300, 8, 1, 1
    burn_in = 30

    X, f_true, Phi_true, Lambda_true, Q_u_true, R_true = _simulate_dfm(
        T=T, n=n, r=r, phi_scalar=0.7, seed=3
    )

    params = em_dfm_full(
        X=X,
        r=r,
        p=p,
        max_iter=50,
        tol=1e-5,
        verbose=False,
        use_tqdm=False,
    )

    f_est = params.factors_smooth[:, 0]               # (T,)
    lam_est = params.Lambda[:, 0]                     # (n,)

    # drop burn-in
    f_true_eff = f_true[burn_in:, 0]
    f_est_eff = f_est[burn_in:]

    # align sign: correlation is invariant up to sign for 1 factor
    corr_f = np.corrcoef(f_true_eff, f_est_eff)[0, 1]
    if corr_f < 0:
        f_est_eff = -f_est_eff
        lam_est = -lam_est
        corr_f = -corr_f

    # factor correlation
    assert corr_f > 0.7, corr_f

    # loadings correlation
    corr_lam = np.corrcoef(Lambda_true[:, 0], lam_est)[0, 1]
    assert corr_lam > 0.7 or corr_lam < -0.7, corr_lam
    # (sign may flip again during EM, but magnitude should be high)


def test_em_dfm_heavy_missing_still_converges():
    """
    Robustness to heavy missingness:
    - 40% missing entries at random
    - EM should still run, produce finite outputs, PD R, and a loglik path.
    """
    T, n, r, p = 200, 10, 2, 1

    X, *_ = _simulate_dfm(T=T, n=n, r=r, phi_scalar=0.6, seed=4)

    rng = np.random.default_rng(5)
    mask = rng.random(X.shape) < 0.4
    X[mask] = np.nan

    params = em_dfm_full(
        X=X,
        r=r,
        p=p,
        max_iter=40,
        tol=1e-4,
        verbose=False,
        use_tqdm=False,
    )

    # shapes and finiteness
    assert params.factors_smooth.shape == (T, r)
    assert np.isfinite(params.factors_smooth).all()
    assert np.isfinite(params.Lambda).all()
    assert np.isfinite(params.R).all()

    # R positive definite
    eigvals_R = np.linalg.eigvalsh(params.R)
    assert np.all(eigvals_R > 0), eigvals_R

    # likelihood path exists and doesn't explode
    ll = np.asarray(params.loglik_history, float)
    assert ll.size >= 1
    assert np.isfinite(ll).all()
