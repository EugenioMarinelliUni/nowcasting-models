from __future__ import annotations

"""Block utilities for the BM-DFM.

The EM implementations assume (when blocks are used) a *membership mask*:
    blocks_mask: (nM, n_blocks) array with entries in {0,1}

Each monthly series i can load only on factors from blocks where blocks_mask[i, b] == 1.

Accepted input formats (normalized to the mask):

1) None
   -> returns None (non-block model)

2) 1D integer labels of length nM
   blocks[i] = b means series i belongs to block b (0-based)
   -> converted to one-hot mask (nM, n_blocks)

3) 2D mask-like array
   shape (nM, n_blocks) or (nM + nQ, n_blocks) where the first nM rows
   correspond to monthly series.
"""

from typing import Optional

import numpy as np


def normalize_blocks(
    blocks: object,
    *,
    nM: int,
    n_blocks: int,
    nQ: int = 0,
) -> Optional[np.ndarray]:
    """Normalize config.blocks to a (nM, n_blocks) {0,1} mask.

    Args:
        blocks: None, 1D labels (nM,), or 2D mask (nM, n_blocks) / (nM+nQ, n_blocks).
        nM: number of monthly series.
        n_blocks: number of factor blocks (= len(r_by_block)).
        nQ: number of quarterly series (optional; used only to allow (nM+nQ, n_blocks) input).

    Returns:
        None if blocks is None, else int array of shape (nM, n_blocks) with entries in {0,1}.
    """
    if blocks is None:
        return None

    arr = np.asarray(blocks, dtype=int)

    if arr.ndim == 1:
        if arr.shape[0] != int(nM):
            raise ValueError(f"blocks labels must have length nM={nM}, got {arr.shape[0]}.")
        if int(n_blocks) <= 0:
            raise ValueError("n_blocks must be positive when blocks are provided.")
        if np.any(arr < 0) or np.any(arr >= int(n_blocks)):
            bad = arr[(arr < 0) | (arr >= int(n_blocks))]
            raise ValueError(f"blocks labels must be in [0, n_blocks-1]; found {bad[:10]}.")
        mask = np.zeros((int(nM), int(n_blocks)), dtype=int)
        mask[np.arange(int(nM), dtype=int), arr.astype(int)] = 1
        return mask

    if arr.ndim == 2:
        if arr.shape[1] != int(n_blocks):
            raise ValueError(
                f"blocks mask must have n_blocks={n_blocks} columns, got {arr.shape[1]}."
            )
        if arr.shape[0] == int(nM):
            mask = arr
        elif int(nQ) > 0 and arr.shape[0] == int(nM) + int(nQ):
            mask = arr[: int(nM), :]
        else:
            raise ValueError(
                f"blocks mask must have shape (nM, n_blocks)=({nM},{n_blocks}) "
                f"or (nM+nQ, n_blocks)=({nM+nQ},{n_blocks}); got {arr.shape}."
            )
        mask = (mask != 0).astype(int)
        row_sum = mask.sum(axis=1)
        if np.any(row_sum == 0):
            idx = np.where(row_sum == 0)[0][:10]
            raise ValueError(
                "Each monthly series must belong to at least one block; "
                f"found {int((row_sum == 0).sum())} zero-membership rows (e.g. {idx.tolist()})."
            )
        return mask

    raise ValueError("blocks must be None, a 1D label vector, or a 2D membership mask.")