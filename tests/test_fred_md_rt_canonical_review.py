from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    build_review_pairs,
    compute_rolling_pairwise_diagnostics,
    load_reference_series,
    review_reference_mapping,
    review_transition_references,
    transform_reference_series,
    validate_transition_review_spec,
)


def _spec(
    *,
    mode: str = "pairwise",
    tcode: int = 5,
    aux: str = "",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "X",
                "old_reference_series": "OLD",
                "new_reference_series": "NEW",
                "aux_reference_series": aux,
                "canonical_tcode": tcode,
                "review_mode": mode,
            }
        ]
    )


def _series(
    values: list[float],
    *,
    name: str,
) -> pd.Series:
    dates = pd.date_range(
        "2000-01-01",
        periods=len(values),
        freq="MS",
    )
    return pd.Series(
        values,
        index=dates,
        name=name,
        dtype=float,
    )


def test_validate_review_spec_normalizes_fields() -> None:
    spec = pd.DataFrame(
        [
            {
                "canonical_id": " X ",
                "old_reference_series": " OLD ",
                "new_reference_series": " NEW ",
                "aux_reference_series": np.nan,
                "canonical_tcode": "5",
                "review_mode": " pairwise ",
            }
        ]
    )

    out = validate_transition_review_spec(spec)

    assert out.loc[0, "canonical_id"] == "X"
    assert out.loc[0, "old_reference_series"] == "OLD"
    assert out.loc[0, "new_reference_series"] == "NEW"
    assert out.loc[0, "aux_reference_series"] == ""
    assert int(out.loc[0, "canonical_tcode"]) == 5
    assert out.loc[0, "review_mode"] == "pairwise"


def test_validate_review_spec_rejects_unknown_mode() -> None:
    spec = _spec(mode="automatic_accept")

    with pytest.raises(
        ValueError,
        match="Unsupported transition review modes",
    ):
        validate_transition_review_spec(spec)


def test_validate_review_spec_requires_aux_for_triangulation() -> None:
    spec = _spec(
        mode="triangulation",
        aux="",
    )

    with pytest.raises(
        ValueError,
        match="requires aux_reference_series",
    ):
        validate_transition_review_spec(spec)


def test_validate_review_spec_rejects_invalid_tcode() -> None:
    spec = _spec(tcode=99)

    with pytest.raises(
        ValueError,
        match="Invalid canonical t-code",
    ):
        validate_transition_review_spec(spec)


def test_load_reference_series_parses_and_sorts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "OLD.csv"

    pd.DataFrame(
        {
            "observation_date": [
                "2000-03-01",
                "2000-01-01",
                "2000-02-01",
            ],
            "OLD": [
                "3.0",
                "1.0",
                ".",
            ],
        }
    ).to_csv(path, index=False)

    series = load_reference_series(
        path,
        series_id="OLD",
    )

    assert series.index.tolist() == [
        pd.Timestamp("2000-01-01"),
        pd.Timestamp("2000-02-01"),
        pd.Timestamp("2000-03-01"),
    ]
    assert series.iloc[0] == 1.0
    assert np.isnan(series.iloc[1])
    assert series.iloc[2] == 3.0


def test_load_reference_series_rejects_duplicate_dates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "OLD.csv"

    pd.DataFrame(
        {
            "DATE": [
                "2000-01-01",
                "2000-01-01",
            ],
            "OLD": [
                1.0,
                2.0,
            ],
        }
    ).to_csv(path, index=False)

    with pytest.raises(
        ValueError,
        match="duplicate reference dates",
    ):
        load_reference_series(
            path,
            series_id="OLD",
        )


def test_transform_reference_series_uses_canonical_tcode() -> None:
    series = _series(
        [100.0, 110.0, 121.0],
        name="OLD",
    )

    transformed = transform_reference_series(
        series,
        tcode=5,
    )

    expected = np.log(series).diff()

    assert np.isnan(transformed.iloc[0])
    assert np.allclose(
        transformed.iloc[1:],
        expected.iloc[1:],
        atol=1e-12,
    )


def test_build_review_pairs_for_triangulation() -> None:
    row = _spec(
        mode="triangulation",
        aux="AUX",
    ).iloc[0]

    pairs = build_review_pairs(row)

    assert pairs == [
        ("old_vs_aux", "OLD", "AUX"),
        ("aux_vs_new", "AUX", "NEW"),
        ("old_vs_new", "OLD", "NEW"),
    ]


def test_review_pairwise_mapping_reuses_transformed_diagnostics() -> None:
    old = _series(
        [100.0, 108.0, 119.0, 131.0, 145.0, 160.0],
        name="OLD",
    )
    new = old.mul(10.0).rename("NEW")

    summary, aligned, rolling = review_reference_mapping(
        _spec().iloc[0],
        {
            "OLD": old,
            "NEW": new,
        },
        rolling_window_months=3,
    )

    assert len(summary) == 1
    row = summary.iloc[0]

    assert row["pair_role"] == "old_vs_new"
    assert row["diagnostic_status"] == "computed"
    assert int(row["n_common_valid"]) == 5
    assert np.isclose(row["pearson_corr"], 1.0)
    assert np.isclose(row["std_ratio_new_old"], 1.0)
    assert np.isclose(row["rmse_difference"], 0.0, atol=1e-12)

    assert len(aligned) == 5
    assert np.allclose(
        aligned["old_transformed"],
        aligned["new_transformed"],
        atol=1e-12,
    )

    assert not rolling.empty


def test_review_triangulation_produces_three_diagnostic_pairs() -> None:
    old = _series(
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        name="OLD",
    )
    aux = old.mul(2.0).rename("AUX")
    new = old.mul(4.0).rename("NEW")

    summary, aligned, _ = review_reference_mapping(
        _spec(
            mode="triangulation",
            tcode=1,
            aux="AUX",
        ).iloc[0],
        {
            "OLD": old,
            "AUX": aux,
            "NEW": new,
        },
        rolling_window_months=3,
    )

    assert summary["pair_role"].tolist() == [
        "old_vs_aux",
        "aux_vs_new",
        "old_vs_new",
    ]
    assert summary["diagnostic_status"].eq("computed").all()
    assert len(aligned) == 18

    assert "review_decision" not in summary.columns
    assert "accepted" not in summary.columns


def test_rolling_diagnostics_use_calendar_month_windows() -> None:
    dates = pd.to_datetime(
        [
            "2000-01-01",
            "2000-02-01",
            "2000-05-01",
        ]
    )

    aligned = pd.DataFrame(
        {
            "old_transformed": [1.0, 2.0, 5.0],
            "new_transformed": [2.0, 4.0, 10.0],
        },
        index=dates,
    )

    out = compute_rolling_pairwise_diagnostics(
        aligned,
        window_months=3,
    )

    assert len(out) == 1
    row = out.iloc[0]

    assert row["window_start_date"] == "2000-03-01"
    assert row["window_end_date"] == "2000-05-01"
    assert int(row["n_common_valid"]) == 1
    assert row["diagnostic_status"] == "insufficient_overlap"


def test_review_transition_references_runs_multiple_spec_rows() -> None:
    spec = pd.DataFrame(
        [
            {
                "canonical_id": "X",
                "old_reference_series": "OLD",
                "new_reference_series": "NEW",
                "aux_reference_series": "",
                "canonical_tcode": 1,
                "review_mode": "pairwise",
            },
            {
                "canonical_id": "Y",
                "old_reference_series": "OLD2",
                "new_reference_series": "NEW2",
                "aux_reference_series": "",
                "canonical_tcode": 1,
                "review_mode": "fred_md_splice",
            },
        ]
    )

    references = {
        "OLD": _series(
            [1.0, 2.0, 3.0, 4.0],
            name="OLD",
        ),
        "NEW": _series(
            [2.0, 4.0, 6.0, 8.0],
            name="NEW",
        ),
        "OLD2": _series(
            [10.0, 20.0, 30.0, 40.0],
            name="OLD2",
        ),
        "NEW2": _series(
            [100.0, 200.0, 300.0, 400.0],
            name="NEW2",
        ),
    }

    summary, aligned, rolling = review_transition_references(
        spec,
        references,
        rolling_window_months=3,
    )

    assert summary["canonical_id"].tolist() == ["X", "Y"]
    assert summary["review_mode"].tolist() == [
        "pairwise",
        "fred_md_splice",
    ]
    assert len(aligned) == 8
    assert not rolling.empty