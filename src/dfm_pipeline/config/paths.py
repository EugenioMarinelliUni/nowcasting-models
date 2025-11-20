from __future__ import annotations
import json, os
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

_DEFAULT_CONFIG = "data/config/paths.json"
_ENV_VAR = "DFM_CONFIG_PATH"

class ConfigError(RuntimeError):
    pass

def _load_config(config_path: Optional[str]) -> Dict[str, Any]:
    p = Path(config_path or os.getenv(_ENV_VAR, _DEFAULT_CONFIG))
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    try:
        with p.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        raise ConfigError(f"Failed to parse JSON config at {p}: {e}") from e
    if not isinstance(cfg, dict):
        raise ConfigError("Config root must be an object")
    return cfg

def resolve_paths(panel_name: str, y_name: str, config_path: Optional[str]=None) -> Tuple[Path, Path]:
    cfg = _load_config(config_path)
    panels = cfg.get("panels", {})
    targets = cfg.get("targets", {})
    if panel_name not in panels:
        raise ConfigError(f"Panel name '{panel_name}' not in config.panels")
    if y_name not in targets:
        raise ConfigError(f"Target name '{y_name}' not in config.targets")
    raw_x = Path(panels[panel_name])
    raw_y = Path(targets[y_name])
    if not raw_x.exists():
        raise ConfigError(f"Configured raw X not found: {raw_x}")
    if not raw_y.exists():
        raise ConfigError(f"Configured raw Y not found: {raw_y}")
    return raw_x, raw_y
