#!/usr/bin/env python3
from pathlib import Path
import argparse, json, os
from datetime import datetime, UTC

from dotenv import load_dotenv  # pip install python-dotenv
load_dotenv()  # loads ./.env into environment

from dfm_pipeline.ingestion.fred_client import get_series, to_named_column

SERIES_ID = "A191RL1Q225SBEA"
OUT_COL   = "gdp_qoq_saar"

def main():
    ap = argparse.ArgumentParser(description="Fetch GDP q/q SAAR (A191RL1Q225SBEA) from FRED/ALFRED.")
    ap.add_argument("--api-key", default=os.getenv("FRED_API_KEY"), help="FRED API key (or set FRED_API_KEY in .env)")
    ap.add_argument("--vintage", help="YYYY-MM-DD (ALFRED real-time date)")
    ap.add_argument("--out-dir", type=Path, default=Path("data/raw_data/targets/gdp"))
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Set FRED_API_KEY in .env/env or pass --api-key.")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Fetch and rename to a friendly column
    df_raw = get_series(SERIES_ID, args.api_key, vintage=args.vintage)
    df = to_named_column(df_raw, SERIES_ID, OUT_COL)

    tag = f"vintage_{args.vintage}" if args.vintage else "latest"
    csv_path  = args.out_dir / f"{SERIES_ID}_{tag}.csv"
    meta_path = args.out_dir / f"{SERIES_ID}_{tag}_meta.json"

    df.to_csv(csv_path, index=False, date_format="%Y-%m-%d", float_format="%.10g")

    meta = {
        "series_id": SERIES_ID,
        "vintage": args.vintage or "latest",
        "downloaded_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "rows": int(df.shape[0]),
        "first_date": df["sasdate"].min().strftime("%Y-%m-%d"),
        "last_date": df["sasdate"].max().strftime("%Y-%m-%d"),
        "column_name": OUT_COL,
        "source": "api.stlouisfed.org/fred/series/observations",
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Wrote:\n  {csv_path}\n  {meta_path}")

if __name__ == "__main__":
    main()

