"""
Equivalence check: NEW vs NEW_CACHED state-space backends.

This script is intended to catch any numerical differences introduced by caching,
while leaving the underlying math/logic unchanged.

Assumptions:
- Both implementations expose the same public API symbols used below.
- You have dfm_pipeline.dfm_dyn.state_space_new
- You have dfm_pipeline.dfm_bm_ml.state_space_new_cached
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

import numpy as np


@dataclass
class DiffReport:
    ok: bool
    max_abs: float
    mean_abs: float
    n_diff: int
    n_total: int


def _as_numpy(x: Any) -> np.ndarray:
    if isinstance(x, np.ndarray):
        return x
    return np.asarray(x)


def _finite_mask(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return np.isfinite(a) & np.isfinite(b)


def compare_arrays(a: Any, b: Any, *, atol: float, rtol: float) -> DiffReport:
    A = _as_numpy(a).astype(float, copy=False)
    B = _as_numpy(b).astype(float, copy=False)

    if A.shape != B.shape:
        raise ValueError(f"Shape mismatch: {A.shape} vs {B.shape}")

    m = _finite_mask(A, B)
    if m.size == 0:
        return DiffReport(ok=True, max_abs=0.0, mean_abs=0.0, n_diff=0, n_total=0)

    diff = np.zeros_like(A, dtype=float)
    diff[m] = np.abs(A[m] - B[m])

    max_abs = float(np.max(diff[m])) if np.any(m) else 0.0
    mean_abs = float(np.mean(diff[m])) if np.any(m) else 0.0

    tol = atol + rtol * np.abs(A)
    n_diff = int(np.sum((diff > tol) & m))
    n_total = int(np.sum(m))

    ok = (n_diff == 0)
    return DiffReport(ok=ok, max_abs=max_abs, mean_abs=mean_abs, n_diff=n_diff, n_total=n_total)


def _get_public_callables(mod) -> Dict[str, Any]:
    """
    Return a dict of public callables (functions) in the module.
    Filters out private names and non-callables.
    """
    out: Dict[str, Any] = {}
    for name in dir(mod):
        if name.startswith("_"):
            continue
        obj = getattr(mod, name)
        if callable(obj):
            out[name] = obj
    return out


def _pick_entrypoints(mod) -> Tuple[str, ...]:
    """
    Prefer comparing the key routines used by BM-DFM.
    Fall back to comparing all public callables if we can't identify them.
    """
    preferred = (
        "kalman_filter_only",
        "kalman_filter_smoother",
    )
    pub = _get_public_callables(mod)
    chosen = [n for n in preferred if n in pub]
    if chosen:
        return tuple(chosen)
    return tuple(sorted(pub.keys()))


def _call(fn, args: Dict[str, Any]):
    """
    Call function with kwargs; allows missing kwargs by filtering to signature.
    """
    import inspect

    sig = inspect.signature(fn)
    kw = {k: v for k, v in args.items() if k in sig.parameters}
    return fn(**kw)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--inputs-npz", required=True, help="Path to NPZ with inputs for state-space calls.")
    p.add_argument("--atol", type=float, default=1e-12)
    p.add_argument("--rtol", type=float, default=1e-10)
    p.add_argument("--json-out", default="", help="Optional path to write a JSON report.")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    # Load input bundle
    # Expected: a dict-like of arrays/scalars needed to call the entrypoints.
    # You create this NPZ once from your pipeline at a known iteration.
    bundle = np.load(args.inputs_npz, allow_pickle=True)
    call_kwargs: Dict[str, Any] = {}
    for k in bundle.files:
        call_kwargs[k] = bundle[k].item() if bundle[k].dtype == object else bundle[k]

    ss_new = importlib.import_module("dfm_pipeline.dfm_dyn.state_space_new")
    ss_cached = importlib.import_module("dfm_pipeline.dfm_bm_ml.state_space_new_cached")

    ep = _pick_entrypoints(ss_new)

    results: Dict[str, Any] = {"ok": True, "functions": {}}

    for name in ep:
        if not hasattr(ss_cached, name):
            results["ok"] = False
            results["functions"][name] = {"ok": False, "error": "missing_in_cached"}
            continue

        fn_a = getattr(ss_new, name)
        fn_b = getattr(ss_cached, name)

        try:
            out_a = _call(fn_a, call_kwargs)
            out_b = _call(fn_b, call_kwargs)
        except Exception as e:
            results["ok"] = False
            results["functions"][name] = {"ok": False, "error": f"call_failed: {type(e).__name__}: {e}"}
            continue

        # Outputs might be a tuple; compare elementwise
        if isinstance(out_a, tuple) and isinstance(out_b, tuple):
            if len(out_a) != len(out_b):
                results["ok"] = False
                results["functions"][name] = {"ok": False, "error": "tuple_len_mismatch"}
                continue

            per = []
            ok_all = True
            max_abs = 0.0
            mean_abs_acc = 0.0
            n_total_acc = 0
            n_diff_acc = 0

            for i, (xa, xb) in enumerate(zip(out_a, out_b)):
                rep = compare_arrays(xa, xb, atol=args.atol, rtol=args.rtol)
                per.append(
                    {
                        "idx": i,
                        "ok": rep.ok,
                        "max_abs": rep.max_abs,
                        "mean_abs": rep.mean_abs,
                        "n_diff": rep.n_diff,
                        "n_total": rep.n_total,
                    }
                )
                ok_all = ok_all and rep.ok
                max_abs = max(max_abs, rep.max_abs)
                mean_abs_acc += rep.mean_abs * rep.n_total
                n_total_acc += rep.n_total
                n_diff_acc += rep.n_diff

            mean_abs = (mean_abs_acc / n_total_acc) if n_total_acc else 0.0

            results["functions"][name] = {
                "ok": ok_all,
                "max_abs": max_abs,
                "mean_abs": mean_abs,
                "n_diff": n_diff_acc,
                "n_total": n_total_acc,
                "per_output": per,
            }
            results["ok"] = results["ok"] and ok_all

        else:
            rep = compare_arrays(out_a, out_b, atol=args.atol, rtol=args.rtol)
            results["functions"][name] = {
                "ok": rep.ok,
                "max_abs": rep.max_abs,
                "mean_abs": rep.mean_abs,
                "n_diff": rep.n_diff,
                "n_total": rep.n_total,
            }
            results["ok"] = results["ok"] and rep.ok

        if args.verbose:
            r = results["functions"][name]
            print(
                f"{name}: ok={r['ok']} max_abs={r.get('max_abs', None)} "
                f"mean_abs={r.get('mean_abs', None)} n_diff={r.get('n_diff', None)}/{r.get('n_total', None)}"
            )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, sort_keys=True)

    return 0 if results["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
