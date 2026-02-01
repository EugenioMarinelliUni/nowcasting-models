from __future__ import annotations

import argparse
from pathlib import Path

from dfm_pipeline.visualization.nowcast_plots import (
    read_predictions_csv,
    plot_nowcasts_moq123_vs_actual,
    plot_nowcast_moq_vs_actual,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot NOW nowcasts vs actual quarterly values.")
    ap.add_argument("--predictions-csv", required=True, help="Path to predictions.csv")
    ap.add_argument("--outdir", required=True, help="Directory to write plots")
    ap.add_argument("--horizon", default="now", help='Which horizon to plot (default: "now")')
    ap.add_argument("--moq", default="all", help='Which moq to plot: "all" or one of 1/2/3 (default: all)')
    ap.add_argument("--title", default=None, help="Optional title prefix")
    args = ap.parse_args()

    pred_path = Path(args.predictions_csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = read_predictions_csv(pred_path)

    prefix = (args.title + " - ") if args.title else ""

    if args.moq == "all":
        out_all = outdir / f"plot_{args.horizon}_nowcasts_moq123_vs_actual.png"
        plot_nowcasts_moq123_vs_actual(
            df, out_all, horizon=args.horizon,
            title=f"{prefix}{args.horizon.upper()} nowcasts vs actual (moq=1/2/3)",
        )
        print(f"[OK] wrote: {out_all}")

        out_m3 = outdir / f"plot_{args.horizon}_nowcast_moq3_vs_actual.png"
        # only write if moq=3 exists
        try:
            plot_nowcast_moq_vs_actual(
                df, out_m3, horizon=args.horizon, moq=3,
                title=f"{prefix}{args.horizon.upper()} nowcast vs actual (moq=3)",
            )
            print(f"[OK] wrote: {out_m3}")
        except ValueError:
            pass
        return 0

    moq = int(args.moq)
    out_one = outdir / f"plot_{args.horizon}_nowcast_moq{moq}_vs_actual.png"
    plot_nowcast_moq_vs_actual(
        df, out_one, horizon=args.horizon, moq=moq,
        title=f"{prefix}{args.horizon.upper()} nowcast vs actual (moq={moq})",
    )
    print(f"[OK] wrote: {out_one}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
