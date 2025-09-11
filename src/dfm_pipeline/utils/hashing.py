from __future__ import annotations

from pathlib import Path
import hashlib


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Compute the SHA-256 hash of a file. Useful for provenance metadata.

    Parameters
    ----------
    path : str | Path
        File path to hash.
    chunk_size : int
        Read size in bytes for streaming the file.

    Returns
    -------
    hex_digest : str
        Hex-encoded SHA-256 digest.
    """
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
