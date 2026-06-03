#!/usr/bin/env python3
"""Convert daily market trade zips into per-ticker parquet files.

Source layout (one zip per trading day, under --src, in per-month folders):
    <src>/YYYYMM/YYYYMMDD.zip
        00001.csv          # one CSV per ticker, at the zip root
        00002.csv
        ...

Each ticker CSV holds that day's tick-by-tick trades, e.g.:
    ticker,tradeid,date,datetime,price,volume,type,cancelflag

Output layout (the final destination):
    <out>/<yyyy>/<YYYYMMDD>/<ticker>.parquet

i.e. each ticker CSV becomes its own dataframe written as one parquet file, under a
per-day folder. By default <out> is derived from --market/--asset and writes straight
into the market data tree, e.g. /data/equity_data/hkg/md_trade/raw.

Usage:
    python md_trade_zip_to_parquet.py [--market MARKET] [--asset ASSET]
        [--date-range RANGE] [--src DIR] [--out DIR] [--workers N]

--date-range (YYYYMMDD, or YYYYMMDD:YYYYMMDD, with either side open) restricts
processing to those days; otherwise every *.zip in --src is processed.
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

DEFAULT_MARKET = "hkg"
DEFAULT_ASSET = "eq"
DEFAULT_SRC = Path("/home/wangfc/md_trade")

# asset code -> the /data/<dir> root it lives under.
ASSET_DIRS = {"eq": "equity_data", "crypto": "crypto_data"}


def default_out(market: str, asset: str) -> Path:
    """Default output root for a (market, asset) pair."""
    return Path(f"/data/{ASSET_DIRS[asset]}/{market}/md_trade/raw")


def parse_date_range(spec: str) -> tuple[str | None, str | None]:
    """Parse a --date-range spec into inclusive (lo, hi) YYYYMMDD bounds.

    "YYYYMMDD"            -> single day (lo == hi)
    "YYYYMMDD:YYYYMMDD"   -> closed range
    "YYYYMMDD:" / ":YYYYMMDD" -> open-ended on the empty side (None)
    """
    if ":" not in spec:
        return spec, spec
    lo, hi = spec.split(":", 1)
    return (lo or None), (hi or None)


def process_day(zip_path: str, out_base: str) -> dict:
    """Read every ticker CSV in one daily zip and write per-ticker parquet files.

    Returns a small summary dict so the parent can report progress / failures.
    """
    zp = Path(zip_path)
    date8 = zp.stem  # YYYYMMDD
    yyyy = date8[:4]
    out_dir = Path(out_base) / yyyy / date8
    out_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    rows = 0
    errors: list[str] = []
    with zipfile.ZipFile(zp) as z:
        members = [n for n in z.namelist() if n.lower().endswith(".csv")]
        for name in members:
            ticker = Path(name).stem  # e.g. "00001"
            try:
                raw = z.read(name)
                if not raw.strip():
                    continue  # skip empty files
                df = pd.read_csv(io.BytesIO(raw))
                df.to_parquet(out_dir / f"{ticker}.parquet",
                              engine="pyarrow", index=False, compression="snappy")
                written += 1
                rows += len(df)
            except Exception as e:  # noqa: BLE001
                errors.append(f"{name}: {e}")
    return {"date": date8, "members": len(members), "written": written,
            "rows": rows, "errors": errors, "out_dir": str(out_dir)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--market", default=DEFAULT_MARKET, help="market code (default hkg)")
    p.add_argument("--asset", default=DEFAULT_ASSET, choices=sorted(ASSET_DIRS),
                   help="asset class (default eq)")
    p.add_argument("--date-range", dest="date_range", default=None,
                   help="YYYYMMDD or YYYYMMDD:YYYYMMDD; omit = all zips in --src")
    p.add_argument("--src", type=Path, default=DEFAULT_SRC)
    p.add_argument("--out", type=Path, default=None,
                   help="output root; default derived from --market/--asset")
    p.add_argument("--workers", type=int, default=min(20, len(os.sched_getaffinity(0))))
    args = p.parse_args(argv)

    out = args.out or default_out(args.market, args.asset)

    # zips live in per-month folders (<src>/YYYYMM/*.zip); also accept zips
    # placed directly in <src>.
    zips = sorted({*args.src.glob("*.zip"), *args.src.glob("*/*.zip")})
    if args.date_range:
        lo, hi = parse_date_range(args.date_range)
        zips = [z for z in zips
                if (lo is None or z.stem >= lo) and (hi is None or z.stem <= hi)]
    if not zips:
        where = args.src if not args.date_range else f"{args.src} for {args.date_range}"
        print(f"No zips found in {where}", file=sys.stderr)
        return 1

    out.mkdir(parents=True, exist_ok=True)
    print(f"[{args.market}/{args.asset}] Building {len(zips)} day(s) with "
          f"{args.workers} workers -> {out}", flush=True)

    failures = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_day, str(z), str(out)): z for z in zips}
        for fut in as_completed(futs):
            z = futs[fut]
            try:
                r = fut.result()
                nerr = len(r["errors"])
                print(f"[{r['date']}] wrote {r['written']}/{r['members']} parquet, "
                      f"{r['rows']:,} rows{' , ERRORS=' + str(nerr) if nerr else ''}",
                      flush=True)
                if nerr:
                    for e in r["errors"][:5]:
                        print(f"    ! {e}", file=sys.stderr)
                    failures += 1
            except Exception as e:  # noqa: BLE001
                print(f"[{z.name}] FAILED: {e}", file=sys.stderr)
                failures += 1

    print(f"Done. failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
