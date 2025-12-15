#!/usr/bin/env python
import argparse
from pathlib import Path

import matplotlib.pyplot as plt

from dfm_pipeline.visualization.plot_month_of_quarter_errors import (
    plot_month_of_quarter_errors,
)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Plot MM quarterly nowcast errors by month-of-quarter "
            "(Month 1 / Month 2 / Month 3) from an mf_dfm_oos_*.csv file."
        )
    )
    parser.add_argument(
        "--oos-csv",
        type=str,
        required=True,
        help="Path to mf_dfm_oos_*.csv produced by run_mf_dfm_cv_oos(_fast).",
    )
    parser.add_argument(
        "--test-start",
        type=str,
        default=None,
        help="Optional start date for plotting (e.g. '2016-01-01').",
    )
    parser.add_argument(
        "--savefig",
        type=str,
        default=None,
        help="Optional path to save the figure (e.g. 'figs/moq_errors.png').",
    )

    args = parser.parse_args()

    ax = plot_month_of_quarter_errors(
        oos_csv_path=Path(args.oos_csv),
        test_start=args.test_start,
        show=False,  # we control show below
        title=None,
    )

    fig = ax.get_figure()
    if args.savefig is not None:
        out_path = Path(args.savefig)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out_path, dpi=150)
        print(f"Saved figure to: {out_path}")

    # Show interactively (if backend allows)
    plt.show()


if __name__ == "__main__":
    main()
