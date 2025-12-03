# scripts/visualization/plot_dfm_nowcasts_four_dots.py

import argparse
import os

import pandas as pd

from dfm_pipeline.visualization.nowcast_quarter_plots import (
    save_four_dots_plot_for_specs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot DFM nowcasts as four dots per quarter "
            "(three monthly nowcasts + actual) for selected specs."
        )
    )
    parser.add_argument(
        "--forecasts-long-csv",
        type=str,
        required=True,
        help=(
            "Path to the long-format forecasts CSV. "
            "Expected to contain columns: Date, spec, year, quarter, y_hat, y_real_Q."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where PNG plots will be stored.",
    )
    parser.add_argument(
        "--specs",
        type=str,
        nargs="*",
        default=None,
        help=(
            "List of specs to plot (e.g. q2_r1_p1 q3_r1_p2). "
            "If omitted, all specs in the file will be used."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.forecasts_long_csv):
        raise FileNotFoundError(args.forecasts_long_csv)

    df_long = pd.read_csv(args.forecasts_long_csv)

    if args.specs is None or len(args.specs) == 0:
        if "spec" not in df_long.columns:
            raise ValueError("The forecasts CSV has no 'spec' column and --specs was not provided.")
        specs = sorted(df_long["spec"].dropna().unique())
    else:
        specs = args.specs

    save_four_dots_plot_for_specs(
        df_long=df_long,
        specs=specs,
        output_dir=args.output_dir,
        base_figsize=(10.0, 5.0),
    )


if __name__ == "__main__":
    main()
