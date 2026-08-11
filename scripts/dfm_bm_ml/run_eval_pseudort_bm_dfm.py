from __future__ import annotations

"""Disabled legacy pseudo-real-time BM-DFM entry point.

This script previously truncated the revised panel by evaluation date but did not
apply the audited release-timing, target-leakage, parameter-mode, and vintage
safeguards.  It is intentionally disabled so that all pseudo-real-time DFM runs
go through the hardened evaluators.
"""


def main() -> None:
    raise SystemExit(
        "This pseudo-real-time DFM entry point is disabled because it bypasses "
        "the current release-timing, GDP-leakage, convergence, and vintage "
        "safeguards. Use "
        "scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast.py or "
        "scripts/dfm_bm_ml/run_eval_pseudort_bm_dfm_fast_numba.py."
    )


if __name__ == "__main__":
    main()
