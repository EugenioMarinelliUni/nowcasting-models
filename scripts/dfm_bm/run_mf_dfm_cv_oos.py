from __future__ import annotations

"""Disabled legacy DFM command-line entry point.

The historical implementation is retained in Git history only for reproducibility.
Use the audited BM-DFM commands under ``scripts/dfm_bm_ml`` for new estimation
and pseudo-real-time evaluation.
"""


def main() -> None:
    raise SystemExit(
        "This legacy DFM entry point is disabled because it does not implement "
        "the current scaling, convergence, release-timing, and leakage safeguards. "
        "Use scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast.py or "
        "scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast_numba.py."
    )


if __name__ == "__main__":
    main()
