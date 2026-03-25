"""
Temporary alias for the cached backend.

Until a dedicated cached implementation is added inside dfm_dyn, selecting
DFM_STATE_SPACE_IMPL=new_cached resolves explicitly to the standard NEW
backend instead of silently attempting a cross-package import.
"""

from __future__ import annotations

from .state_space_new import *  # noqa: F401,F403
