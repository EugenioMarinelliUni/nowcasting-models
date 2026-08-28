from __future__ import annotations

import json
import os

import pandas as pd

from dfm_pipeline.preprocessing.tcode import (
    apply_tcode_transformations,
    standardize,
)


def transform_from_csv_and_json(
    csv_path: str,
    tcode_json_path: str,
    save_path: str | None = None,
    standardize_data: bool = True,
) -> pd.DataFrame:
    """
    Legacy convenience pipeline for one FRED-MD-style CSV.

    Transformation now delegates to the canonical implementation in
    ``dfm_pipeline.preprocessing.tcode``.  The historical behaviour of this
    helper is preserved: it skips the conventional second-row t-code marker and
    returns only the transformed/standardized DataFrame.

    Notes
    -----
    This helper is not the real-time vintage pipeline.  For research-grade
    pseudo-real-time work, use the vintage-aware ingestion and QC modules so
    each historical file supplies its own embedded t-code map.
    """
    df = pd.read_csv(
        csv_path,
        skiprows=[1],
        parse_dates=["sasdate"],
        index_col="sasdate",
    )

    with open(tcode_json_path, "r", encoding="utf-8") as handle:
        tcode_map = {
            key: int(value)
            for key, value in json.load(handle).items()
        }

    df_transformed = apply_tcode_transformations(df, tcode_map)

    if standardize_data:
        # Preserve the legacy pandas std convention (ddof=1) used by the old
        # utility while relying on the canonical standardization routine.
        df_final, _, _ = standardize(df_transformed, ddof=1)
    else:
        df_final = df_transformed

    if save_path:
        directory = os.path.dirname(save_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        df_final.to_csv(save_path)
        print(f"Transformed dataset saved to {save_path}")

    return df_final
