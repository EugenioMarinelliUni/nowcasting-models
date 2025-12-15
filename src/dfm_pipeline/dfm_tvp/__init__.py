# src/dfm_pipeline/dfm_tvp/__init__.py

"""
Small-scale TVP and variance-switching utilities built on top of the
mixed-frequency DFM output (mf_dfm_oos_*.csv).

This subpackage is intentionally decoupled from the main EM–DFM estimation:
it takes as input the already-computed DFM-based nowcasts or factors
and fits a low-dimensional time-varying-parameter regression for quarterly GDP.
"""

from .tvp_regression import (
    TVPRegressionResult,
    em_tvp_regression,
)
