import numpy as np
from dfm_pipeline.dfm_dyn.em_dfm import em_dfm_full


def test_em_dfm_sanity():
    rng = np.random.default_rng(0)

    T, n = 200, 5
    r, p = 2, 1

    # True DFM parameters
    Phi_true = [0.5 * np.eye(r)]
    Q_u_true = 0.1 * np.eye(r)
    Lambda_true = rng.normal(scale=1.0, size=(n, r))
    R_true = 0.05 * np.eye(n)

    # Simulate factors
    f = np.zeros((T, r))
    for t in range(1, T):
        f[t] = Phi_true[0] @ f[t - 1] + rng.multivariate_normal(
            np.zeros(r), Q_u_true
        )

    # Simulate observed panel X
    X = np.empty((T, n))
    for t in range(T):
        X[t] = (
            Lambda_true @ f[t]
            + rng.multivariate_normal(np.zeros(n), R_true)
        )

    # Add missing values
    mask = rng.random(X.shape) < 0.1
    X[mask] = np.nan

    # Run EM-DFM
    params = em_dfm_full(
        X=X,
        r=r,
        p=p,
        max_iter=20,
        tol=1e-4,
        verbose=False,
    )

    # Basic sanity checks
    assert params.factors_smooth.shape == (T, r)
    assert params.states_smooth.shape == (T, r * p)
    assert params.Lambda.shape == (n, r)
    assert params.R.shape == (n, n)

    # R should be positive definite (numerically)
    eigvals_R = np.linalg.eigvalsh(params.R)
    assert np.all(eigvals_R > 0), eigvals_R

    # EM should have produced at least one log-likelihood value
    assert len(params.loglik_history) >= 1
