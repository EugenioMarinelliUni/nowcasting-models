# src/dfm_pipeline/config/env.py
from __future__ import annotations
from pathlib import Path
import os

def _project_root() -> Path:
    # adjust depth if your package lives deeper
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / ".env").exists() or (p / ".git").exists() or (p / "pyproject.toml").exists():
            return p
    return here.parents[2]  # safe fallback

def load_dotenv_once() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    env_path = _project_root() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

# load on import
load_dotenv_once()

def get_env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)
