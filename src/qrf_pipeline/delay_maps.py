from __future__ import annotations

from pathlib import Path
import json


DEFAULT_DELAY_BY_GROUP = {
    "Output & Income": 1,
    "Labor Market": 1,
    "Housing": 1,
    "Consumption / Inventories": 1,
    "Prices": 1,
    "Money & Credit": 0,
    "Interest Rates": 0,
    "Yield Spread": 0,
    "Stock Market": 0,
    "Exchange Rates": 0,
}


def build_delay_map_from_group_map(
    group_map: dict[str, str],
    *,
    default_delay: int = 1,
    override_by_group: dict[str, int] | None = None,
) -> dict[str, int]:
    rules = dict(DEFAULT_DELAY_BY_GROUP)
    if override_by_group:
        rules.update({str(k): int(v) for k, v in override_by_group.items()})

    return {
        str(var): int(rules.get(str(group), default_delay))
        for var, group in group_map.items()
    }


def write_delay_map(delay_map: dict[str, int], path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(delay_map, f, indent=2)
    return path


def load_group_map(path: str | Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    return {str(k): str(v) for k, v in obj.items()}
