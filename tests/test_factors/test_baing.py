# tests/test_factors/test_baing.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.factors.baing import bai_ng_criteria
from dfm_pipeline.paths import baing_artifacts, ensure_dir


def test_baing_selects_reasonable_r(tmp_path: Path) -> None:
    # synthetic panel: 2 factors + noise
    rng = np.random.default_rng(42)
    T, N, true_r = 120, 40, 2
    F = rng.normal(size=(T, true_r))
    L = rng.normal(size=(N, true_r))
    Z = F @ L.T + 0.3 * rng.normal(size=(T, N))

    dates = pd.date_range("1990-01-01", periods=T, freq="M")
    cols = [f"x{j+1}" for j in range(N)]
    X = pd.DataFrame(Z, index=dates, columns=cols)

    res = bai_ng_criteria(X, r_max=8, ic_type="ICp2", index=X.index, colnames=list(X.columns))

    assert 0 <= res.r_star <= 8
    # in most runs with low noise we expect r* >= true_r
    assert res.grid.shape[0] == 9  # r=0..8
    assert res.factors.shape[0] == T
    assert res.loadings.shape[0] == N

    # write artifacts to temp dir
    out = tmp_path / "variants" / "1960_noVIX" / "train1990_2019" / "factors" / "baing" / "sis" / "unittest"
    ensure_dir(out)
    arts = baing_artifacts("1960_noVIX", "train1990_2019", "sis", "unittest")
    # patch dir to tmp_path
    arts = {k: (Path(str(v)).name if k == "dir" else out / Path(v).name) for k, v in arts.items()}
    res.grid.to_csv(arts["grid_csv"], index=False)
    res.factors.to_csv(arts["factors_csv"], index=True)
    res.loadings.to_csv(arts["loadings_csv"], index=True)
    pd.DataFrame({"component": res.eigenvalues.index, "singular_value": res.eigenvalues.values}).to_csv(arts["eigen_csv"], index=False)

    assert arts["grid_csv"].is_file()
    assert arts["factors_csv"].is_file()
    assert arts["loadings_csv"].is_file()
    assert arts["eigen_csv"].is_file()
