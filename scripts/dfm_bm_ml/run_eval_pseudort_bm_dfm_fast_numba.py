from dfm_pipeline.dfm_bm_ml.fast.fit_fast_numba import fit_bm_dfm_fast_numba
from dfm_pipeline.eval_pseudort.cli import main_with_fit


if __name__ == "__main__":
    main_with_fit(
        fit_bm_dfm_fast_numba,
        description="Leakage-safe pseudo/real-time evaluation of the BM-DFM (Numba backend).",
    )
