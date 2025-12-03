# scripts/visualization/run_nowcast_diagnostics.py

from __future__ import annotations

import argparse
import os
from typing import List

import matplotlib.pyplot as plt
import pandas as pd

from dfm_pipeline.visualization.nowcast_errors import (
    plot_horizon_error_boxplots,
    plot_calibration_scatter,
    plot_rolling_error,
)
from dfm_pipeline.visualization.nowcast_subperiods import (
    Subperiod,
    plot_subperiod_rmse_bars,
    plot_subperiod_rmse_heatmap,
)
from dfm_pipeline.visualization.nowcast_complexity import (
    plot_complexity_vs_rmse,
)


PLOT_CHOICES = {
    "horizon_box",
    "calibration",
    "subperiod_bars",
    "subperiod_heatmap",
    "rolling",
    "complexity",
}


def _parse_subperiods(raw: str | None) -> List[Subperiod]:
    if not raw:
        return [
            Subperiod("2000-2007", 2000, 2007),
            Subperiod("2008-2012", 2008, 2012),
            Subperiod("2013-2019", 2013, 2019),
        ]

    out: List[Subperiod] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            start_str, end_str = chunk.split("-")
            start_year = int(start_str)
            end_year = int(end_str)
        except ValueError as exc:
            raise ValueError(f"Invalid subperiod specification: {chunk!r}") from exc
        out.append(
            Subperiod(
                label=f"{start_year}-{end_year}",
                start_year=start_year,
                end_year=end_year,
            )
        )
    return out


def _parse_specs(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run nowcast diagnostics plots on a long-format forecasts CSV."
    )

    parser.add_argument(
        "--plot",
        type=str,
        required=True,
        choices=sorted(PLOT_CHOICES),
        help=(
            "Plot type: "
            "horizon_box, calibration, subperiod_bars, "
            "subperiod_heatmap, rolling, complexity."
        ),
    )
    parser.add_argument(
        "--forecasts-long-csv",
        type=str,
        required=True,
        help="Path to the long-format forecasts CSV.",
    )

    # Single-output mode
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output image path (single-plot mode, e.g. 'plot.png').",
    )

    # Batch mode
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=(
            "Base directory for batch mode. "
            "If --all-specs or --all-horizons is set, this is required."
        ),
    )
    parser.add_argument(
        "--all-specs",
        action="store_true",
        help="Use all specs found in the CSV (batch mode).",
    )
    parser.add_argument(
        "--all-horizons",
        action="store_true",
        help="Use horizons 1, 2, 3 (batch mode).",
    )

    # Spec / horizon / subperiod controls
    parser.add_argument(
        "--spec",
        type=str,
        default=None,
        help="Single spec label (e.g. 'q2_r1_p2') for single-plot mode.",
    )
    parser.add_argument(
        "--specs",
        type=str,
        default=None,
        help=(
            "Comma-separated list of specs for subperiod_* plots "
            "(e.g. 'q1_r1_p1,q2_r1_p2'). Ignored if --all-specs is set."
        ),
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=3,
        help="Horizon in quarter (1, 2, 3) for single-plot mode.",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=12,
        help="Rolling window size in quarters (for rolling plots).",
    )
    parser.add_argument(
        "--metric",
        type=str,
        default="rmse",
        choices=["rmse", "mae"],
        help="Error metric for rolling plots.",
    )
    parser.add_argument(
        "--subperiods",
        type=str,
        default=None,
        help=(
            "Subperiods for subperiod_* plots, format: "
            "'2000-2007,2008-2012,2013-2019'. "
            "If omitted, uses default splits."
        ),
    )

    return parser


def _load_all_specs(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path)
    if "spec" not in df.columns:
        raise ValueError("CSV must contain a 'spec' column for --all-specs.")
    specs = sorted(df["spec"].dropna().unique())
    return list(specs)


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.plot not in PLOT_CHOICES:
        raise ValueError(f"Unsupported plot type: {args.plot!r}")

    if not os.path.exists(args.forecasts_long_csv):
        raise FileNotFoundError(args.forecasts_long_csv)

    df_long = pd.read_csv(args.forecasts_long_csv)

    batch_mode = args.all_specs or args.all_horizons

    if batch_mode:
        if args.output_dir is None:
            raise ValueError("--output-dir is required when using --all-specs or --all-horizons.")
        if args.output is not None:
            raise ValueError("Use either --output (single-plot) or --output-dir (batch), not both.")
    else:
        if args.output is None:
            raise ValueError("--output is required in single-plot mode.")

    subperiods = _parse_subperiods(args.subperiods)

    # -------------------
    # Batch mode
    # -------------------
    if batch_mode:
        if args.all_specs:
            spec_list = _load_all_specs(args.forecasts_long_csv)
        else:
            spec_list = _parse_specs(args.specs)
            if not spec_list:
                raise ValueError("--specs must be provided if --all-specs is not set in batch mode.")

        if args.all_horizons:
            horizon_list = [1, 2, 3]
        else:
            horizon_list = [args.horizon]

        base_dir = args.output_dir
        _ensure_dir(base_dir)

        if args.plot == "horizon_box":
            # One plot per spec
            for spec in spec_list:
                out_path = os.path.join(base_dir, f"horizon_box_{spec}.png")
                fig, ax = plt.subplots(figsize=(8.0, 5.0))
                plot_horizon_error_boxplots(df_long, spec=spec, ax=ax)
                fig.tight_layout()
                fig.savefig(out_path, dpi=150)
                plt.close(fig)

        elif args.plot == "calibration":
            # Per (spec, horizon)
            for h in horizon_list:
                out_dir_h = os.path.join(base_dir, f"h{h}")
                _ensure_dir(out_dir_h)
                for spec in spec_list:
                    out_path = os.path.join(out_dir_h, f"calibration_{spec}_h{h}.png")
                    fig, ax = plt.subplots(figsize=(5.0, 5.0))
                    plot_calibration_scatter(df_long, spec=spec, horizon=h, ax=ax)
                    fig.tight_layout()
                    fig.savefig(out_path, dpi=150)
                    plt.close(fig)

        elif args.plot == "rolling":
            # Per (spec, horizon)
            for h in horizon_list:
                out_dir_h = os.path.join(base_dir, f"h{h}")
                _ensure_dir(out_dir_h)
                for spec in spec_list:
                    out_path = os.path.join(
                        out_dir_h,
                        f"rolling_{args.metric}_{spec}_h{h}_w{args.window}.png",
                    )
                    fig, ax = plt.subplots(figsize=(10.0, 4.0))
                    plot_rolling_error(
                        df_long,
                        spec=spec,
                        horizon=h,
                        window=args.window,
                        metric=args.metric,
                        ax=ax,
                    )
                    fig.tight_layout()
                    fig.savefig(out_path, dpi=150)
                    plt.close(fig)

        elif args.plot == "subperiod_bars":
            for h in horizon_list:
                out_path = os.path.join(base_dir, f"subperiod_bars_h{h}.png")
                fig, ax = plt.subplots(figsize=(10.0, 5.0))
                plot_subperiod_rmse_bars(
                    df_long=df_long,
                    horizon=h,
                    subperiods=subperiods,
                    specs=spec_list,
                    ax=ax,
                )
                fig.tight_layout()
                fig.savefig(out_path, dpi=150)
                plt.close(fig)

        elif args.plot == "subperiod_heatmap":
            for h in horizon_list:
                out_path = os.path.join(base_dir, f"subperiod_heatmap_h{h}.png")
                fig, ax = plt.subplots(figsize=(10.0, 6.0))
                plot_subperiod_rmse_heatmap(
                    df_long=df_long,
                    horizon=h,
                    subperiods=subperiods,
                    specs=spec_list,
                    ax=ax,
                )
                fig.tight_layout()
                fig.savefig(out_path, dpi=150)
                plt.close(fig)

        elif args.plot == "complexity":
            for h in horizon_list:
                out_path = os.path.join(base_dir, f"complexity_vs_rmse_h{h}.png")
                fig, ax = plt.subplots(figsize=(7.0, 5.0))
                plot_complexity_vs_rmse(df_long, horizon=h, ax=ax)
                fig.tight_layout()
                fig.savefig(out_path, dpi=150)
                plt.close(fig)

        else:
            raise ValueError(f"Unexpected plot type in batch mode: {args.plot!r}")

        return

    # -------------------
    # Single-plot mode
    # -------------------
    output_path = args.output
    out_dir_single = os.path.dirname(output_path)
    _ensure_dir(out_dir_single)

    fig, ax = plt.subplots(figsize=(8.0, 5.0))

    if args.plot == "horizon_box":
        if args.spec is None:
            raise ValueError("--spec is required for plot=horizon_box in single mode")
        plot_horizon_error_boxplots(df_long, spec=args.spec, ax=ax)

    elif args.plot == "calibration":
        if args.spec is None:
            raise ValueError("--spec is required for plot=calibration in single mode")
        plot_calibration_scatter(df_long, spec=args.spec, horizon=args.horizon, ax=ax)

    elif args.plot == "subperiod_bars":
        spec_list = _parse_specs(args.specs)
        if not spec_list:
            raise ValueError("--specs is required for plot=subperiod_bars in single mode")
        plot_subperiod_rmse_bars(
            df_long=df_long,
            horizon=args.horizon,
            subperiods=subperiods,
            specs=spec_list,
            ax=ax,
        )

    elif args.plot == "subperiod_heatmap":
        spec_list = _parse_specs(args.specs)
        if not spec_list:
            raise ValueError("--specs is required for plot=subperiod_heatmap in single mode")
        plot_subperiod_rmse_heatmap(
            df_long=df_long,
            horizon=args.horizon,
            subperiods=subperiods,
            specs=spec_list,
            ax=ax,
        )

    elif args.plot == "rolling":
        if args.spec is None:
            raise ValueError("--spec is required for plot=rolling in single mode")
        plot_rolling_error(
            df_long,
            spec=args.spec,
            horizon=args.horizon,
            window=args.window,
            metric=args.metric,
            ax=ax,
        )

    elif args.plot == "complexity":
        plot_complexity_vs_rmse(df_long, horizon=args.horizon, ax=ax)

    else:
        raise ValueError(f"Unexpected plot type in single mode: {args.plot!r}")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
