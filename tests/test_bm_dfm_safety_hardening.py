from __future__ import annotations

import importlib
import importlib.util

import numpy as np
import pytest

from dfm_pipeline.dfm_bm_ml.estimation import GEMAcceptance
from dfm_pipeline.dfm_bm_ml.fast.fit_fast import fit_bm_dfm_fast
from dfm_pipeline.dfm_bm_ml.fit_core import run_em_loop
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.state_builder import BMParams
from dfm_pipeline.eval_pseudort.cli import build_parser
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig


def _small_multiblock_panel() -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(123)
    T = 30
    f1 = np.zeros(T)
    f2 = np.zeros(T)
    for t in range(1, T):
        f1[t] = 0.55 * f1[t - 1] + rng.normal(scale=0.4)
        f2[t] = 0.35 * f2[t - 1] + rng.normal(scale=0.5)
    X = np.column_stack(
        [
            f1 + rng.normal(scale=0.2, size=T),
            0.7 * f1 + rng.normal(scale=0.2, size=T),
            f2 + rng.normal(scale=0.2, size=T),
            -0.6 * f2 + rng.normal(scale=0.2, size=T),
        ]
    )
    y = np.full(T, np.nan)
    y[2::3] = f1[2::3] + f2[2::3] + rng.normal(scale=0.2, size=len(y[2::3]))
    return X, y


@pytest.mark.parametrize(
    "blocks",
    [
        [0, 0, 1, 1],
        [[1, 0], [1, 0], [0, 1], [0, 1]],
    ],
)
def test_multiblock_is_optional_but_works_when_explicitly_requested(blocks):
    X, y = _small_multiblock_panel()
    cfg = BMDfmConfig(
        r_by_block=(1, 1),
        p=1,
        blocks=blocks,
        max_iter=1,
        tol=0.0,
        scaling_mode="internal_per_run",
    )
    res = fit_bm_dfm_fast(X, y, cfg)

    # Cross-block monthly loadings are structurally zero.
    assert np.allclose(res.params.Lambda_m[:2, 1], 0.0)
    assert np.allclose(res.params.Lambda_m[2:, 0], 0.0)


def test_multiblock_requires_explicit_membership_but_single_block_does_not():
    BMDfmConfig(r_by_block=(1,), p=1, blocks=None).validate()
    with pytest.raises(ValueError, match="require an explicit blocks"):
        BMDfmConfig(r_by_block=(1, 1), p=1, blocks=None).validate()


def test_var_order_is_explicitly_limited_to_five():
    BMDfmConfig(r_by_block=(1,), p=5).validate()
    with pytest.raises(ValueError, match="supported VAR orders are 1 through 5"):
        BMDfmConfig(r_by_block=(1,), p=6).validate()


def _minimal_params() -> BMParams:
    return BMParams(
        Phi_blocks=[[np.array([[0.5]])]],
        Q_f_blocks=[np.array([[1.0]])],
        rho_m=np.array([0.0]),
        sig2_m=np.array([1.0]),
        rho_q=np.array([0.0]),
        sig2_q=np.array([1.0]),
        Lambda_m=np.array([[1.0]]),
        Lambda_q=np.array([[1.0]]),
        R_diag_m=np.array([0.1]),
        R_diag_q=np.array([0.1]),
    )


def test_rejected_gem_candidate_is_not_reported_as_converged(monkeypatch):
    import dfm_pipeline.dfm_bm_ml.fit_core as fit_core

    params = _minimal_params()

    def fake_em_step(**kwargs):
        return params, -10.0, None, None, None, None, None

    def reject_candidate(**kwargs):
        return GEMAcceptance(params, -10.0, 0.0, 0, False, None, None)

    monkeypatch.setattr(fit_core, "accept_projected_gem_candidate", reject_candidate)
    cfg = BMDfmConfig(r_by_block=(1,), p=1, max_iter=5, tol=1e-6)
    out = run_em_loop(
        Y=np.zeros((3, 2)),
        initial_params=params,
        config=cfg,
        em_step=fake_em_step,
        em_kwargs={},
        build_kwargs={},
        description="test",
        verbose=False,
    )

    assert not out.converged
    assert out.diagnostics["stalled"] is True
    assert out.diagnostics["stop_reason"] == "rejected_candidate"
    assert out.diagnostics["rejected_steps"] == 1


def test_cli_has_no_implicit_release_policy_and_enforces_safe_gdp_defaults():
    parser = build_parser("test")
    delay_action = next(a for a in parser._actions if a.dest == "delay_style")
    gdp_action = next(a for a in parser._actions if a.dest == "gdp_rel")
    leak_action = next(a for a in parser._actions if a.dest == "no_qe_leak")

    assert delay_action.required is True
    assert gdp_action.default == 1
    assert leak_action.default is True

    EvalConfig(eval_start="2020-01-01", eval_end="2020-01-01").validate()
    with pytest.raises(ValueError, match="gdp_rel must be at least 1"):
        EvalConfig(
            eval_start="2020-01-01",
            eval_end="2020-01-01",
            gdp_rel=0,
        ).validate()
    with pytest.raises(ValueError, match="cannot be disabled"):
        EvalConfig(
            eval_start="2020-01-01",
            eval_end="2020-01-01",
            no_qe_leak=False,
        ).validate()


def test_only_canonical_state_space_implementation_remains(monkeypatch):
    monkeypatch.setenv("DFM_STATE_SPACE_IMPL", "old")
    import dfm_pipeline.dfm_dyn.state_space as state_space

    state_space = importlib.reload(state_space)
    assert state_space.kalman_filter_smoother.__module__.endswith("state_space_new")
    assert importlib.util.find_spec("dfm_pipeline.dfm_dyn.state_space_old") is None
    assert importlib.util.find_spec("dfm_pipeline.dfm_dyn.state_space_new_uni") is None
