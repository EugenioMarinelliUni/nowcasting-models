from __future__ import annotations

import argparse
from pathlib import Path

from dfm_pipeline.visualization.nowcast_plots import (
    read_predictions_csv,
    plot_nowcasts_moq123_vs_actual,
    plot_nowcast_moq_vs_actual,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot BM pseudo-RT nowcasts vs actual from predictions.csv")
    ap.add_argument("--predictions-csv", required=True, help="Path to predictions.csv")
    ap.add_argument("--outdir", required=True, help="Directory to write plots")
    ap.add_argument("--horizon", default="now", help='Which horizon to plot (default: "now")')
    ap.add_argument("--moq", default="all", help='Which moq: "all" or one of 1/2/3 (default: all)')
    ap.add_argument("--title", default=None, help="Optional title prefix")

    ap.add_argument(
        "--marker-only",
        action="store_true",
        help="Use dots (scatter) instead of connected lines for nowcasts.",
    )
    ap.add_argument(
        "--vlines",
        action="store_true",
        help="Draw thin vertical reference lines at each target_date.",
    )
    ap.add_argument(
        "--dot-size",
        type=int,
        default=18,
        help="Dot size for scatter (default: 18).",
    )

    args = ap.parse_args()

    pred_path = Path(args.predictions_csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = read_predictions_csv(pred_path)

    prefix = (args.title + " - ") if args.title else ""

    # Four distinct colors for the combined moq123 plot:
    colors_moq123 = {
        "moq1": "tab:blue",
        "moq2": "tab:orange",
        "moq3": "tab:green",
        "actual": "tab:red",
    }

    if args.moq == "all":
        out_all = outdir / f"plot_{args.horizon}_nowcasts_moq123_vs_actual.png"
        plot_nowcasts_moq123_vs_actual(
            df,
            out_all,
            horizon=args.horizon,
            title=f"{prefix}{args.horizon.upper()} nowcasts vs actual (moq=1/2/3)",
            marker_only=args.marker_only,
            colors=colors_moq123,
            show_vlines=args.vlines,
            dot_size=args.dot_size,
        )
        print(f"[OK] wrote: {out_all}")

        # Also produce moq=3 plot (commonly the "final" nowcast)
        out_m3 = outdir / f"plot_{args.horizon}_nowcast_moq3_vs_actual.png"
        try:
            plot_nowcast_moq_vs_actual(
                df,
                out_m3,
                horizon=args.horizon,
                moq=3,
                title=f"{prefix}{args.horizon.upper()} nowcast vs actual (moq=3)",
                marker_only=args.marker_only,
                colors={"pred": "tab:green", "actual": "tab:red"},
                show_vlines=args.vlines,
                dot_size=args.dot_size,
            )
            print(f"[OK] wrote: {out_m3}")
        except ValueError:
            pass
        return 0

    moq = int(args.moq)
    out_one = outdir / f"plot_{args.horizon}_nowcast_moq{moq}_vs_actual.png"
    pred_color = {1: "tab:blue", 2: "tab:orange", 3: "tab:green"}[moq]
    plot_nowcast_moq_vs_actual(
        df,
        out_one,
        horizon=args.horizon,
        moq=moq,
        title=f"{prefix}{args.horizon.upper()} nowcast vs actual (moq={moq})",
        marker_only=args.marker_only,
        colors={"pred": pred_color, "actual": "tab:red"},
        show_vlines=args.vlines,
        dot_size=args.dot_size,
    )
    print(f"[OK] wrote: {out_one}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())