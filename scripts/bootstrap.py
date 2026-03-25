# scripts/bootstrap.py

from __future__ import annotations

import os
import sys


def add_src_to_path() -> None:
    """
    Ensure the project's 'src' directory is included in Python's module search path.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.abspath(os.path.join(current_dir, "..", "src"))

    if src_path not in sys.path:
        sys.path.insert(0, src_path)