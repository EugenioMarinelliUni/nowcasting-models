"""
Selector module for BM-ML state-space backend.

Resolution order (via env var DFM_STATE_SPACE_IMPL):
- "old"        -> dfm_pipeline.dfm_dyn.state_space_old
- "new_cached" -> dfm_pipeline.dfm_bm_ml.state_space_new_cached
- default      -> dfm_pipeline.dfm_dyn.state_space_new

Consumers should do:
    from dfm_pipeline.dfm_bm_ml.state_space import ss
and then call ss.<function>(...)
"""

from __future__ import annotations

import os

impl = os.getenv("DFM_STATE_SPACE_IMPL", "").strip().lower()

if impl == "old":
    from dfm_pipeline.dfm_dyn import state_space_old as ss
elif impl == "new_cached":
    from dfm_pipeline.dfm_bm_ml import state_space_new_cached as ss
else:
    from dfm_pipeline.dfm_dyn import state_space_new as ss

__all__ = ["ss", "impl"]
