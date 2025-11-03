#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import shlex
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import List


def _ts() -> str:
    # timezone-aware, avoids utcnow() deprecation
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _sanitize_token(x) -> str:
    s = str(x)
    s = s.replace(" ", "")
    s = s.replace(".", "")
    s = s.replace("/", "_")
    return s


def _make_label(template: str, **kw) -> str:
    # Provide a safe formatter: missing keys -> empty string
    class _SafeDict(dict):
        def __missing__(self, key):
            return ""
    # Sanitize common fields for filenames
    for k in ("alpha", "dedup", "hac"):
        if k in kw and kw[k] is not None:
            kw[k] = _sanitize_token(kw[k])
    return template.format_map(_SafeDict(kw))


def main():
    ap = argparse.ArgumentParser(
        description="Grid runner for preselection with pass-through of extra t-stat options."
    )
    # Core axes
    ap.add_argument("--panels", nargs="+", required=True, help="One or more panel ids")
    ap.add_argument("--tags", nargs="+", required=True, help="One or more training tags")
    ap.add_argument("--methods", nargs="+", choices=["sis", "tstat", "lars"], default=["tstat"])

    # Global grids (kebab-case)
    ap.add_argument("--min-features", nargs="+", type=int, default=[30])
    ap.add_argument("--max-features", nargs="+", type=int, default=[80])
    ap.add_argument("--dedup-tau", nargs="+", type=float, default=[0.98])

    # SIS grids
    ap.add_argument("--sis-tau", nargs="+", type=float, default=[0.0])
    ap.add_argument("--sis-topn", nargs="+", type=int, default=[0])

    # t-stat grids
    ap.add_argument("--tstat-alpha", nargs="+", type=float, default=[0.05])
    ap.add_argument("--tstat-topn", nargs="+", type=int, default=[0])
    ap.add_argument("--tstat-ar-lags", nargs="+", type=int, default=[4])
    ap.add_argument("--hac-lags", nargs="+", default=["auto"])

    # Aggregation (fixed values typically)
    ap.add_argument("--agg-rule-map", type=str, default=None)
    ap.add_argument("--agg-rule-default", type=str, choices=["sum3m", "mean3m", "last"], default=None)

    # ADL for X (t-stat)
    ap.add_argument("--x-adl-lags", nargs="+", type=int, default=[0])
    ap.add_argument("--adl-score", nargs="+", choices=["fstat", "max_t"], default=["fstat"])

    # LARS grids
    ap.add_argument("--lars-cv", nargs="+", type=int, default=[10])

    # Execution
    ap.add_argument("--python", type=str, default=sys.executable, help="Python executable to use")
    ap.add_argument("--runner", type=str, default="scripts/preselection/run_preselection.py",
                    help="Path to run_preselection.py")
    ap.add_argument("--manifest", type=str,
                    default=f"data/metadata/variants/preselection_grid_manifest__{_ts()}.csv")
    ap.add_argument("--dry-run", action="store_true", help="Print commands without executing")

    # Labeling
    ap.add_argument(
        "--label-template",
        type=str,
        default="{method}__alpha{alpha}__min{min}__max{max}__dedup{dedup}__ar{ar}__hac{hac}__xadl{xadl}__adl{adl}",
        help="Template for labeling outputs; usable fields: "
             "{method},{panel},{tag},{alpha},{min},{max},{dedup},{ar},{hac},{xadl},{adl}",
    )

    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    _ensure_parent(manifest_path)

    # Write manifest header (add label column)
    if not manifest_path.exists():
        manifest_path.write_text(
            "timestamp,panel,tag,method,label,min_features,max_features,dedup_tau,"
            "sis_tau,sis_topn,tstat_alpha,tstat_topn,tstat_ar_lags,hac_lags,"
            "agg_rule_map,agg_rule_default,x_adl_lags,adl_score,lars_cv,returncode\n",
            encoding="utf-8",
        )

    rows: List[str] = []

    for panel in args.panels:
        for tag in args.tags:
            for method in args.methods:

                if method == "sis":
                    grid = itertools.product(
                        args.min_features, args.max_features, args.dedup_tau,
                        args.sis_tau, args.sis_topn
                    )
                    for minf, maxf, dedup, sis_tau, sis_topn in grid:
                        label = _make_label(
                            args.label_template,
                            method="sis", panel=panel, tag=tag,
                            alpha="", min=minf, max=maxf, dedup=dedup, ar="", hac="",
                            xadl="", adl="",
                        )
                        cmd = [
                            args.python, args.runner,
                            "--panel", panel, "--tag", tag, "--method", "sis",
                            "--min_features", str(minf),
                            "--max_features", str(maxf),
                            "--dedup_tau", str(dedup),
                            "--sis_tau", str(sis_tau),
                            "--sis_topn", str(sis_topn),
                            "--label", label,
                        ]
                        rc = 0
                        if args.dry_run:
                            print("DRY-RUN:", " ".join(shlex.quote(c) for c in cmd))
                        else:
                            proc = subprocess.run(cmd, capture_output=True, text=True)
                            rc = proc.returncode
                            if rc != 0:
                                sys.stderr.write(proc.stdout + proc.stderr)

                        row = f"{_ts()},{panel},{tag},sis,{label},{minf},{maxf},{dedup},{sis_tau},{sis_topn},,,,,,,,{rc}\n"
                        rows.append(row)

                elif method == "tstat":
                    grid = itertools.product(
                        args.min_features, args.max_features, args.dedup_tau,
                        args.tstat_alpha, args.tstat_topn, args.tstat_ar_lags, args.hac_lags,
                        args.x_adl_lags, args.adl_score
                    )
                    for minf, maxf, dedup, alpha, topn, ar, hac, xadl, adlscore in grid:
                        label = _make_label(
                            args.label_template,
                            method="tstat", panel=panel, tag=tag,
                            alpha=alpha, min=minf, max=maxf, dedup=dedup,
                            ar=ar, hac=hac, xadl=xadl, adl=adlscore,
                        )
                        cmd = [
                            args.python, args.runner,
                            "--panel", panel, "--tag", tag, "--method", "tstat",
                            "--min_features", str(minf),
                            "--max_features", str(maxf),
                            "--dedup_tau", str(dedup),
                            "--tstat_alpha", str(alpha),
                            "--tstat_topn", str(topn),
                            "--tstat_ar_lags", str(ar),
                            "--hac_lags", str(hac),
                            "--x_adl_lags", str(xadl),
                            "--adl_score", str(adlscore),
                            "--label", label,
                        ]
                        # Aggregation pass-through (optional)
                        if args.agg_rule_map:
                            cmd += ["--agg_rule_map", args.agg_rule_map]
                        if args.agg_rule_default:
                            cmd += ["--agg_rule_default", args.agg_rule_default]

                        rc = 0
                        if args.dry_run:
                            print("DRY-RUN:", " ".join(shlex.quote(c) for c in cmd))
                        else:
                            proc = subprocess.run(cmd, capture_output=True, text=True)
                            rc = proc.returncode
                            if rc != 0:
                                sys.stderr.write(proc.stdout + proc.stderr)

                        row = (
                            f"{_ts()},{panel},{tag},tstat,{label},{minf},{maxf},{dedup},,,"
                            f"{alpha},{topn},{ar},{hac},{args.agg_rule_map or ''},"
                            f"{args.agg_rule_default or ''},{xadl},{adlscore},,{rc}\n"
                        )
                        rows.append(row)

                elif method == "lars":
                    grid = itertools.product(
                        args.min_features, args.max_features, args.dedup_tau, args.lars_cv
                    )
                    for minf, maxf, dedup, cv in grid:
                        label = _make_label(
                            args.label_template,
                            method="lars", panel=panel, tag=tag,
                            alpha="", min=minf, max=maxf, dedup=dedup, ar="", hac="",
                            xadl="", adl="",  # not applicable
                        )
                        cmd = [
                            args.python, args.runner,
                            "--panel", panel, "--tag", tag, "--method", "lars",
                            "--min_features", str(minf),
                            "--max_features", str(maxf),
                            "--dedup_tau", str(dedup),
                            "--cv", str(cv),
                            "--label", label,
                        ]
                        rc = 0
                        if args.dry_run:
                            print("DRY-RUN:", " ".join(shlex.quote(c) for c in cmd))
                        else:
                            proc = subprocess.run(cmd, capture_output=True, text=True)
                            rc = proc.returncode
                            if rc != 0:
                                sys.stderr.write(proc.stdout + proc.stderr)

                        row = f"{_ts()},{panel},{tag},lars,{label},{minf},{maxf},{dedup},,,,,,,,,{cv},{rc}\n"
                        rows.append(row)

    # Append to manifest
    with Path(args.manifest).open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(r)

    print(f"Wrote manifest with {len(rows)} runs to {args.manifest}")


if __name__ == "__main__":
    main()
