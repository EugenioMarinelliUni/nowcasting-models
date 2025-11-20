# tools/refactor_imports.py
import pathlib
import sys
import libcst as cst
from libcst.helpers import get_full_name_for_node

ROOT = pathlib.Path(__file__).resolve().parents[1]
TARGET_PKG_PREFIX = "src.dfm_pipeline"
REPLACEMENT_PREFIX = "dfm_pipeline"
SKIP_DIRS = {".venv", "venv", ".git", "__pycache__", "site-packages", "dist", "build"}

def full_name(node) -> str | None:
    if node is None:
        return None
    try:
        return get_full_name_for_node(node)
    except Exception:
        return None

class Rewrite(cst.CSTTransformer):
    def leave_Import(self, node: cst.Import, updated: cst.Import):
        new_names = []
        changed = False
        for alias in updated.names:
            mod = alias.name
            mod_full = full_name(mod)
            if mod_full and mod_full.startswith(TARGET_PKG_PREFIX):
                new_full = REPLACEMENT_PREFIX + mod_full[len(TARGET_PKG_PREFIX):]
                new_alias = alias.with_changes(name=cst.parse_expression(new_full))
                new_names.append(new_alias)
                changed = True
            else:
                new_names.append(alias)
        return updated.with_changes(names=new_names) if changed else updated

    def leave_ImportFrom(self, node: cst.ImportFrom, updated: cst.ImportFrom):
        mod_full = full_name(updated.module)
        if mod_full and mod_full.startswith(TARGET_PKG_PREFIX):
            new_full = REPLACEMENT_PREFIX + mod_full[len(TARGET_PKG_PREFIX):]
            return updated.with_changes(module=cst.parse_expression(new_full))
        return updated

def rewrite_file(path: pathlib.Path) -> int:
    src = path.read_text(encoding="utf-8")
    tree = cst.parse_module(src)
    new = tree.visit(Rewrite())
    if new.code != src:
        path.write_text(new.code, encoding="utf-8")
        return 1
    return 0

def should_skip(p: pathlib.Path) -> bool:
    for part in p.parts:
        if part in SKIP_DIRS:
            return True
    return False

def main() -> int:
    root = ROOT
    changed = 0
    for p in root.rglob("*.py"):
        if should_skip(p):
            continue
        changed += rewrite_file(p)
    print(f"Files modified: {changed}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
