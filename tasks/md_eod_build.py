#!/usr/bin/env python3
"""Build daily end-of-day (EOD) bars + cross-day features from HKG tick data.

Reads the per-ticker snapshot and trade parquet trees produced by
``md_snapshot_zip_to_parquet`` / ``md_trade_zip_to_parquet`` and, for every
trading day and ticker, distils:

    open, high, low, close, vwap, volume, amount, preclose,
    trading_minutes          # distinct minutes with >=1 orderbook update
    fwd_ret_1d/5d/21d         # close-to-close forward returns (trading-day offsets)
    adv63                     # trailing 63-trading-day mean of daily `amount`

OHLC / volume / amount / preclose are taken from the **official EOD summary**
row of the snapshot stream (the last row with non-zero cumulative volume);
`vwap` is `amount / volume` (the snapshot's own VWAP field is unused — it is
reported as 0). `trading_minutes` counts distinct `HH:MM` buckets across
*all* orderbook updates for the ticker that day (pre-open / closing auction
included).

Source layout (per-ticker parquet, one folder per trading day):
    <snapshot-root>/<yyyy>/<YYYYMMDD>/<ticker>.parquet
    <trade-root>/<yyyy>/<YYYYMMDD>/<ticker>.parquet

Output layout:
    <out>/daily/<yyyy>/<YYYYMMDD>.parquet   # intermediate per-day bar panel (cached)
    <out>/processed/<YYYYMMDD>.csv          # final per-day file, all tickers

By default <out> is /data/<asset>_data/<market>/md_eod.

Two-stage pipeline:
  A. per-day, in parallel, build the daily bar panel (`daily/...parquet`). Cached:
     existing files are reused unless --force.
  B. cross-day features (forward returns, adv63) need neighbouring days, so the
     window [target-63d .. target+21d] of daily panels is loaded and features
     computed per ticker, then each in-range day is written to `processed/...csv`.

To fully populate a target day the runner auto-builds bars for its lookback +
forward window; days near the dataset edges get NaN adv63 / forward returns.

Usage:
    python md_eod_build.py [--market MARKET] [--asset ASSET] [--date-range RANGE]
        [--snapshot-root DIR] [--trade-root DIR] [--out DIR] [--workers N]
        [--force] [--cross-check]
"""
from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

DEFAULT_MARKET = "hkg"
DEFAULT_ASSET = "eq"

# asset code -> the /data/<dir> root it lives under.
ASSET_DIRS = {"eq": "equity_data", "crypto": "crypto_data"}

# Forward-return horizons (in trading days) and the adv lookback window.
FWD_HORIZONS = (1, 5, 21)
ADV_WINDOW = 63

# snapshot columns we actually need (avoid reading all 68).
SNAP_COLS = ["datatime", "openprice", "highprice", "lowprice", "lastprice",
             "volume", "amount", "precloseprice"]

# final CSV column order.
OUT_COLS = ["ticker", "date", "open", "high", "low", "close", "vwap",
            "volume", "amount", "preclose", "trading_minutes",
            *[f"fwd_ret_{h}d" for h in FWD_HORIZONS], "adv63"]


def market_root(market: str, asset: str, sub: str) -> Path:
    """Root of a market-data subtree, e.g. .../hkg/md_snapshot/raw."""
    return Path(f"/data/{ASSET_DIRS[asset]}/{market}/{sub}")


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


def discover_days(snapshot_root: Path) -> list[str]:
    """All YYYYMMDD trading days present under <snapshot-root>/<yyyy>/<YYYYMMDD>/."""
    return sorted({p.name for p in snapshot_root.glob("*/*") if p.is_dir()})


def _ticker_bar(snap_path: Path, trade_dir: Path, cross_check: bool) -> dict | None:
    """Distil one ticker's daily bar from its snapshot parquet.

    Returns a row dict, or None if the file is empty / unreadable.
    """
    try:
        s = pd.read_parquet(snap_path, columns=SNAP_COLS)
    except Exception:  # noqa: BLE001 — unreadable / empty file
        return None
    if s.empty:
        return None

    ticker = int(snap_path.stem)

    # distinct minutes with any orderbook update (HH:MM bucket of "HH:MM:SS.mmm").
    dt = s["datatime"].dropna()
    trading_minutes = int(dt.str.slice(0, 5).nunique()) if len(dt) else 0

    # preclose is constant for the day; grab any non-null value.
    pre = s["precloseprice"].dropna()
    preclose = float(pre.iloc[-1]) if len(pre) else float("nan")

    vol = s["volume"]
    vmax = vol.max()
    if pd.isna(vmax) or vmax <= 0:
        # ticker had orderbook updates but never traded.
        row = dict(ticker=ticker, date=int(snap_path.parent.name),
                   open=float("nan"), high=float("nan"), low=float("nan"),
                   close=float("nan"), vwap=float("nan"), volume=0, amount=0.0,
                   preclose=preclose, trading_minutes=trading_minutes)
        return row

    # official EOD summary = the row carrying the day's max cumulative volume.
    eod = s.loc[vol.idxmax()]
    volume = int(eod["volume"])
    amount = float(eod["amount"])
    row = dict(
        ticker=ticker, date=int(snap_path.parent.name),
        open=float(eod["openprice"]), high=float(eod["highprice"]),
        low=float(eod["lowprice"]), close=float(eod["lastprice"]),
        vwap=(amount / volume if volume else float("nan")),
        volume=volume, amount=amount,
        preclose=preclose, trading_minutes=trading_minutes,
    )

    if cross_check:
        tp = trade_dir / snap_path.name
        if tp.exists():
            try:
                t = pd.read_parquet(tp, columns=["price", "volume"])
                tvol = int(t["volume"].sum())
                row["_xc_vol_diff"] = tvol - volume
            except Exception:  # noqa: BLE001
                pass
    return row


def build_day(date8: str, snapshot_root: str, trade_root: str, out_base: str,
              force: bool, cross_check: bool) -> dict:
    """Stage A worker: write <out>/daily/<yyyy>/<date8>.parquet for one day."""
    yyyy = date8[:4]
    out_path = Path(out_base) / "daily" / yyyy / f"{date8}.parquet"
    snap_dir = Path(snapshot_root) / yyyy / date8
    trade_dir = Path(trade_root) / yyyy / date8

    if out_path.exists() and not force:
        return {"date": date8, "skipped": True, "tickers": 0, "xc_mismatch": 0,
                "out": str(out_path)}

    snaps = sorted(snap_dir.glob("*.parquet"))
    rows: list[dict] = []
    xc_mismatch = 0
    for sp in snaps:
        r = _ticker_bar(sp, trade_dir, cross_check)
        if r is None:
            continue
        if r.pop("_xc_vol_diff", 0):
            xc_mismatch += 1
        rows.append(r)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
    df.to_parquet(out_path, engine="pyarrow", index=False, compression="snappy")
    return {"date": date8, "skipped": False, "tickers": len(df),
            "xc_mismatch": xc_mismatch, "out": str(out_path)}


def add_features(panel: pd.DataFrame) -> pd.DataFrame:
    """Stage B: attach forward returns + adv63 to a multi-day bar panel.

    `panel` holds the daily bars for the full [target-ADV .. target+max_fwd]
    window; features are computed per ticker along the trading-day axis.
    """
    panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = panel.groupby("ticker", sort=False)
    for h in FWD_HORIZONS:
        panel[f"fwd_ret_{h}d"] = g["close"].shift(-h) / panel["close"] - 1.0
    panel["adv63"] = g["amount"].transform(
        lambda s: s.rolling(ADV_WINDOW, min_periods=ADV_WINDOW).mean())
    return panel


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--market", default=DEFAULT_MARKET, help="market code (default hkg)")
    p.add_argument("--asset", default=DEFAULT_ASSET, choices=sorted(ASSET_DIRS),
                   help="asset class (default eq)")
    p.add_argument("--date-range", dest="date_range", default=None,
                   help="YYYYMMDD or YYYYMMDD:YYYYMMDD; omit = all available days")
    p.add_argument("--snapshot-root", type=Path, default=None,
                   help="per-ticker snapshot tree (default .../md_snapshot/raw)")
    p.add_argument("--trade-root", type=Path, default=None,
                   help="per-ticker trade tree (default .../md_trade/raw)")
    p.add_argument("--out", type=Path, default=None,
                   help="output root; default .../md_eod (daily/ + processed/)")
    p.add_argument("--workers", type=int, default=min(20, len(os.sched_getaffinity(0))))
    p.add_argument("--force", action="store_true",
                   help="rebuild daily/ bars even if they already exist")
    p.add_argument("--cross-check", action="store_true",
                   help="also read trade ticks and count vol mismatches (diagnostic; slower)")
    args = p.parse_args(argv)

    snap_root = args.snapshot_root or market_root(args.market, args.asset, "md_snapshot/raw")
    trade_root = args.trade_root or market_root(args.market, args.asset, "md_trade/raw")
    out = args.out or market_root(args.market, args.asset, "md_eod")

    available = discover_days(snap_root)
    if not available:
        print(f"No trading days found under {snap_root}", file=sys.stderr)
        return 1
    idx = {d: i for i, d in enumerate(available)}

    # target days = requested range intersected with what we have source for.
    if args.date_range:
        lo, hi = parse_date_range(args.date_range)
        targets = [d for d in available
                   if (lo is None or d >= lo) and (hi is None or d <= hi)]
    else:
        targets = list(available)
    if not targets:
        print(f"No available days in range {args.date_range}", file=sys.stderr)
        return 1

    # window of source days needed: ADV lookback + forward-return lookahead,
    # measured in trading days (positions in `available`).
    max_fwd = max(FWD_HORIZONS)
    need: set[str] = set()
    for d in targets:
        i = idx[d]
        for j in range(i - (ADV_WINDOW - 1), i + max_fwd + 1):
            if 0 <= j < len(available):
                need.add(available[j])
    build_days = sorted(need)

    print(f"[{args.market}/{args.asset}] targets={len(targets)} "
          f"({targets[0]}..{targets[-1]}), building {len(build_days)} day(s) "
          f"of bars with {args.workers} workers -> {out}", flush=True)

    # ---- Stage A: build (or reuse) per-day daily bars ----
    built = skipped = failures = xc_total = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(build_day, d, str(snap_root), str(trade_root),
                          str(out), args.force, args.cross_check): d
                for d in build_days}
        for fut in as_completed(futs):
            d = futs[fut]
            try:
                r = fut.result()
                if r["skipped"]:
                    skipped += 1
                else:
                    built += 1
                    xc_total += r["xc_mismatch"]
                    xc = f", xc_mismatch={r['xc_mismatch']}" if args.cross_check else ""
                    print(f"[{r['date']}] bars: {r['tickers']} tickers{xc}", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"[{d}] FAILED: {e}", file=sys.stderr)
                failures += 1
    print(f"Stage A done. built={built} reused={skipped} failed={failures}"
          + (f" xc_mismatch_total={xc_total}" if args.cross_check else ""), flush=True)

    # ---- Stage B: load the window, compute features, write processed CSVs ----
    frames = []
    for d in build_days:
        fp = out / "daily" / d[:4] / f"{d}.parquet"
        if fp.exists():
            frames.append(pd.read_parquet(fp))
    if not frames:
        print("No daily bars available to assemble features.", file=sys.stderr)
        return 1
    panel = add_features(pd.concat(frames, ignore_index=True))

    proc_dir = out / "processed"
    proc_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for d in targets:
        day = panel[panel["date"] == int(d)].copy()
        if day.empty:
            print(f"[{d}] no bars — skipped", file=sys.stderr)
            continue
        day["ticker"] = day["ticker"].map(lambda t: f"{int(t):05d}")
        day = day.sort_values("ticker")[OUT_COLS]
        day.to_csv(proc_dir / f"{d}.csv", index=False)
        written += 1
        print(f"[{d}] processed/{d}.csv  ({len(day)} tickers)", flush=True)

    print(f"Done. processed={written} failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
