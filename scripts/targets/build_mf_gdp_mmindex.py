#!/usr/bin/env python
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def build_mf_target(
    panel_path: Path,
    target_path: Path,
    out_path: Path,
) -> None:
    # 1. panel (monthly X)
    panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)

    # 2. standardized monthly target (z-scored A191R..., % SAAR)
    y = pd.read_csv(target_path, index_col=0, parse_dates=True).iloc[:, 0]

    # 3. align to panel index
    y = y.reindex(panel.index)

    # Ensure datetime index
    idx = pd.DatetimeIndex(y.index)

    # 4. Fill within each quarter, but not across quarters
    #    - original pattern: value only at first month of quarter, NaN in next two
    #    - after groupby().ffill(): same value in all three months of that quarter
    quarters = idx.to_period("Q")
    y_filled = y.groupby(quarters).ffill()

    # 5. quarter-end months for month-start index: Mar, Jun, Sep, Dec
    quarter_end_months = {3, 6, 9, 12}
    mask_qe = idx.month.isin(quarter_end_months)

    # 6. keep value only in quarter-end months, NaN elsewhere
    y_mf = y_filled.where(mask_qe, other=np.nan)

    # 7. save
    out_df = pd.DataFrame({"gdp_q_growth": y_mf})
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path)
    print("Wrote mixed-frequency target to:", out_path)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Build mixed-frequency quarterly GDP target on a monthly index "
            "from a standardized monthly target (A191R..., % SAAR)."
        )
    )
    parser.add_argument(
        "--panel-csv",
        type=str,
        required=True,
        help="Path to standardized monthly panel CSV (rows=time, cols=series).",
    )
    parser.add_argument(
        "--target-csv",
        type=str,
        required=True,
        help=(
            "Path to standardized monthly GDP target CSV "
            "(same monthly index as panel, e.g. A191R...)."
        ),
    )
    parser.add_argument(
        "--out-csv",
        type=str,
        required=True,
        help="Output CSV path for mixed-frequency target.",
    )

    args = parser.parse_args()

    build_mf_target(
        panel_path=Path(args.panel_csv),
        target_path=Path(args.target_csv),
        out_path=Path(args.out_csv),
    )


if __name__ == "__main__":
    main()
