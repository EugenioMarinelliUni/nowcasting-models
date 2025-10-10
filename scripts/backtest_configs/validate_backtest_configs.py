#!/usr/bin/env python3
from pathlib import Path
import json, sys

def validate_file(path): return Path(path).exists()

def validate_json(path):
    j = json.loads(Path(path).read_text(encoding="utf-8"))
    ok = True; errs=[]
    for k in ["schema_version","panel_id","time_spans","snapshot","standardization","masking","paths"]:
        if k not in j: ok=False; errs.append(f"Missing key: {k}")
    for pkey in ["mean_map_path","std_map_path"]:
        if pkey in j["standardization"]:
            p = j["standardization"][pkey]
            if not Path(p).parent.exists(): errs.append(f"Dir missing for {pkey}: {p}")
    lag = j["masking"]["lag_map_path"]
    if not Path(lag).exists(): errs.append(f"Missing lag_map_path: {lag}")
    grp = j["series_maps"]["group_map_path"]
    if not Path(grp).exists(): errs.append(f"Missing group_map_path: {grp}")
    return ok and not errs, errs

def main():
    base = Path("data/metadata/backtest")
    files = list(base.rglob("backtest_config_*.json"))
    if not files:
        print("No backtest configs found."); sys.exit(1)
    failed = 0
    for f in files:
        ok, errs = validate_json(f)
        print(f"[{'OK' if ok else 'FAIL'}] {f}")
        if not ok:
            failed += 1
            for e in errs: print("  -", e)
    sys.exit(0 if failed==0 else 2)

if __name__ == "__main__":
    main()