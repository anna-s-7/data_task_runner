#!/usr/bin/env python3
"""Scrape HK-listed company earnings and partition them by announcement date.

This is the `data_task_runner` port of the four-stage pipeline in `trial/`
(`hk_earnings_scraper.py` -> `hk_announcement_dates.py` -> `hk_merge_announcements.py`
-> `hk_finalize_outputs.py`), folded into one self-contained task so it follows the
same `main(argv) -> int` convention as the other tasks in this repo.

Stages (all run in sequence; each is resumable / idempotent):
  1. financials   : per-company reporting-period financials since --since,
                    from Eastmoney via akshare (resumable via done/failed files).
  2. announcements: publication date/time of each results announcement, from
                    HKEXnews (resumable via done/failed files).
  3. merge        : attach each announcement date to its earnings row using
                    explicit period-end / result-type matching.
  4. finalize     : write a consolidated failures file, then partition the matched
                    rows by announcement date.

Output layout (the final destination):
    <out>/<YYYYMMDD>.csv          one file per announcement date; every enriched
                                  earnings record announced that day.
By default <out> is derived from --market/--asset and writes straight into the
market-data tree, e.g. /data/equity_data/hkg/earnings/scrape.

Intermediate CSVs and resume/progress files live under --work-dir (default
`<out_parent>/scrape_staging`), so re-running skips already-scraped companies.

Usage:
    python hkg_earnings_scrape.py [--market MARKET] [--asset ASSET]
        [--date-range RANGE] [--out DIR] [--work-dir DIR] [--workers N]
        [--skip-scrape]

--date-range (YYYYMMDD, or YYYYMMDD:YYYYMMDD, with either side open) restricts which
announcement-date partition files are (re)written; omit = (re)write every day found.
--skip-scrape reuses existing work-dir CSVs (stages 3-4 only) — handy for re-partitioning
or offline testing without hitting the network.

NOTE: unlike the zip->parquet tasks, the per-company work here is network-bound, so it
fans out with ThreadPoolExecutor (threads), not ProcessPoolExecutor.

Dependencies: pandas, requests, akshare (plus the standard library).
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import glob
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

DEFAULT_MARKET = "hkg"
DEFAULT_ASSET = "eq"

# asset code -> the /data/<dir> root it lives under.
ASSET_DIRS = {"eq": "equity_data", "crypto": "crypto_data"}

# --- scrape windows (fixed, as in the trial pipeline) --------------------
SINCE = "2025-01-01"                 # earnings: keep reporting periods >= this
ANN_FROM_DATE = "20250101"           # announcements: HKEXnews search window start
ANN_TO_DATE = _dt.date.today().strftime("%Y%m%d")   # ... window end = today

MAX_RETRIES = 3

# DATE_TYPE_CODE -> period label, for the earnings rows.
PERIOD_TYPE = {"001": "annual", "002": "interim_H1", "003": "Q1", "004": "Q3"}
# DATE_TYPE_CODE -> matching type used when joining announcements.
TYPE_OF_CODE = {"001": "annual", "002": "interim", "003": "Q1", "004": "Q3"}
MAX_LAG_DAYS = {"annual": 210, "interim": 180, "Q1": 120, "Q3": 120}

# Columns kept from the Eastmoney financial-indicator response.
KEEP = [
    "REPORT_DATE", "DATE_TYPE_CODE", "FISCAL_YEAR", "CURRENCY",
    "BASIC_EPS", "DILUTED_EPS", "EPS_TTM",
    "OPERATE_INCOME", "OPERATE_INCOME_YOY",
    "GROSS_PROFIT", "GROSS_PROFIT_RATIO",
    "HOLDER_PROFIT", "HOLDER_PROFIT_YOY", "NET_PROFIT_RATIO",
    "ROE_AVG", "ROA", "BPS",
    "DEBT_ASSET_RATIO", "CURRENT_RATIO",
    "PER_OI", "PER_NETCASH_OPERATE",
]
EARNINGS_COLS = ["code", "name"] + KEEP + ["period_type"]
ANN_COLS = ["code", "announcement_date", "announcement_time",
            "period_explicit", "rtype", "title"]

# HKEXnews endpoints / headline parsing.
HKEX_BASE = "https://www1.hkexnews.hk/search"
HKEX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=EN",
    "X-Requested-With": "XMLHttpRequest",
}
MONTHS = {m: i for i, m in enumerate(
    ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST",
     "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"], 1)}
ENDED_RE = re.compile(r"ENDED\s+(\d{1,2})\s+([A-Z]+)\s+(\d{4})")
YMD_RE = re.compile(r"^\d{8}\.csv$")

# Headlines that contain "RESULTS" but are NOT an earnings results announcement.
EXCLUDE = ["POLL", "MEETING", "ZOOM", "WEBCAST", "WEBINAR", "CONVENING", "PRESENTATION",
           "BRIEFING", "STRESS TEST", "TENDER", "PROXY", "NOTICE", "CLARIFICATION",
           "SUPPLEMENTAL", "PROFIT WARNING", "PROFIT ALERT", "PROFIT NOTICE",
           "POSITIVE PROFIT", "NEGATIVE PROFIT", "DELAY", "POSTPON",
           # IPO / placing / corporate-action "results" (not earnings)
           "ALLOTMENT", "OFFER PRICE", "OVER-ALLOT", "BALLOT", "SUBSCRIPTION",
           "PLACING", "RIGHTS ISSUE", "VOTING RESULT", "OPEN OFFER", "REPURCHASE"]


def default_out(market: str, asset: str) -> Path:
    """Default output root for a (market, asset) pair."""
    return Path(f"/data/{ASSET_DIRS[asset]}/{market}/earnings/scrape")


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


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _load_set(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip().split("\t")[0] for line in f if line.strip()}


# ========================= stage 1: financials ===========================

def build_universe(universe_csv: Path) -> pd.DataFrame:
    """Fetch + cache the HK universe. Returns a DataFrame with code/name."""
    import akshare as ak

    if universe_csv.exists():
        df = pd.read_csv(universe_csv, dtype={"code": str})
        log(f"universe loaded from cache: {len(df)} companies")
        return df
    log("fetching HK universe from Sina (~2 min)...")
    raw = ak.stock_hk_spot()
    df = raw.rename(columns={"代码": "code", "中文名称": "name", "英文名称": "name_en"})
    df["code"] = df["code"].astype(str).str.zfill(5)
    df.to_csv(universe_csv, index=False, encoding="utf-8-sig")
    log(f"universe saved: {len(df)} companies -> {universe_csv}")
    return df


def fetch_earnings_one(code: str, name: str) -> list[dict]:
    """Fetch reporting-period financials for one code, filtered to >= SINCE."""
    import akshare as ak

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_financial_hk_analysis_indicator_em(symbol=code, indicator="报告期")
            if df is None or df.empty:
                return []
            df = df[df["REPORT_DATE"] >= SINCE]
            if df.empty:
                return []
            rows = []
            for _, r in df.iterrows():
                rec = {"code": code, "name": name}
                for c in KEEP:
                    rec[c] = r.get(c)
                rec["period_type"] = PERIOD_TYPE.get(str(r.get("DATE_TYPE_CODE")), "")
                rows.append(rec)
            return rows
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def scrape_earnings(work: Path, workers: int) -> None:
    """Stage 1: scrape per-company financials into <work>/hk_earnings_2025plus.csv."""
    universe_csv = work / "hk_universe.csv"
    earnings_csv = work / "hk_earnings_2025plus.csv"
    done_file = work / "hk_done.txt"
    failed_file = work / "hk_failed.txt"

    uni = build_universe(universe_csv)
    companies = list(zip(uni["code"].astype(str).str.zfill(5), uni["name"].astype(str)))

    done = _load_set(done_file)
    todo = [(c, n) for c, n in companies if c not in done]
    log(f"financials: total={len(companies)} done={len(done)} todo={len(todo)}")
    if not todo:
        log("financials: nothing to do — all companies scraped.")
        return

    write_header = not earnings_csv.exists()
    fout = open(earnings_csv, "a", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(fout, fieldnames=EARNINGS_COLS)
    if write_header:
        writer.writeheader()
    fdone = open(done_file, "a", encoding="utf-8")
    ffail = open(failed_file, "a", encoding="utf-8")

    n_ok = n_rows = n_fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_earnings_one, c, n): (c, n) for c, n in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            c, n = futs[fut]
            try:
                rows = fut.result()
                for rec in rows:
                    writer.writerow(rec)
                n_rows += len(rows)
                n_ok += 1
                fdone.write(c + "\n")
            except Exception as e:  # noqa: BLE001
                n_fail += 1
                ffail.write(f"{c}\t{repr(e)[:120]}\n")
            if i % 50 == 0 or i == len(todo):
                fout.flush(); fdone.flush(); ffail.flush()
                log(f"financials: {i}/{len(todo)} | ok={n_ok} rows={n_rows} fail={n_fail}")

    fout.close(); fdone.close(); ffail.close()
    log(f"financials DONE. ok={n_ok} rows={n_rows} fail={n_fail} -> {earnings_csv}")


# ====================== stage 2: announcement dates ======================

def is_results(up: str) -> bool:
    """True if a normalized-uppercase headline is an earnings results announcement."""
    return ("RESULT" in up) and not any(k in up for k in EXCLUDE)


def classify(up: str) -> str:
    """Result-type from a normalized-uppercase headline (or '' if undetermined).
    Order matters: 'FINAL/ANNUAL/YEAR ENDED' win over a stray 'INTERIM' that only
    appears in a dividend clause (e.g. HSBC 'FINAL RESULTS ... FOURTH INTERIM DIVIDEND')."""
    if any(k in up for k in ("ANNUAL", "FINAL RESULT", "FINAL RESULTS", "FULL YEAR",
                             "YEAR ENDED", "FOR THE YEAR")):
        return "annual"
    if any(k in up for k in ("THIRD QUARTER", "3RD QUARTER", "THIRD QUARTERLY", "NINE MONTHS")):
        return "Q3"
    if any(k in up for k in ("FIRST QUARTER", "1ST QUARTER", "FIRST QUARTERLY")):
        return "Q1"
    if any(k in up for k in ("INTERIM", "HALF-YEAR", "HALF YEAR", "SIX MONTHS")):
        return "interim"
    if "THREE MONTHS" in up:        # bare "three months ended" w/o quarter word
        return "Q1"
    return ""


_sess = threading.local()


def _session():
    import requests

    if not hasattr(_sess, "s"):
        _sess.s = requests.Session()
        _sess.s.headers.update(HKEX_HEADERS)
    return _sess.s


def get_stock_id(code: str):
    r = _session().get(f"{HKEX_BASE}/prefix.do",
                       params={"callback": "c", "lang": "EN", "type": "A",
                               "name": code, "market": "SEHK"}, timeout=20)
    txt = r.text.strip()
    obj = json.loads(txt[txt.index("(") + 1: txt.rindex(")")])
    for row in obj.get("stockInfo", []):
        if str(row.get("code")).zfill(5) == code:
            return row.get("stockId")
    return None


def period_end_from_title(title: str):
    """Return YYYY-MM-DD of the reporting period end embedded in the headline."""
    ms = ENDED_RE.findall(title.upper())
    if not ms:
        return None
    d, mon, y = ms[-1]                      # last "ENDED <date>" = period end
    if mon not in MONTHS:
        return None
    return f"{int(y):04d}-{MONTHS[mon]:02d}-{int(d):02d}"


def fetch_ann_one(code: str) -> list[dict]:
    """-> list of raw results announcements for one code."""
    err = None
    for attempt in range(MAX_RETRIES):
        try:
            sid = get_stock_id(code)
            if not sid:
                return []
            p = {"sortDir": "0", "sortByOptions": "DateTime", "category": "0",
                 "market": "SEHK", "stockId": str(sid), "documentType": "-1",
                 "fromDate": ANN_FROM_DATE, "toDate": ANN_TO_DATE, "title": "RESULTS",
                 "searchType": "0", "t1code": "-2", "t2Gcode": "-2",
                 "t2code": "-2", "rowRange": "100", "lang": "E"}
            r = _session().get(f"{HKEX_BASE}/titleSearchServlet.do", params=p, timeout=30)
            rows = json.loads(r.json()["result"])
            out = []
            for row in rows:
                title = re.sub(r"\s+", " ", (row.get("TITLE") or "")).strip()
                up = title.upper()
                if any(k in up for k in EXCLUDE):
                    continue
                dt = (row.get("DATE_TIME") or "").strip()      # "dd/mm/yyyy HH:MM"
                md = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2})", dt)
                if not md:
                    continue
                dd, mm, yy, hhmm = md.groups()
                out.append({"code": code, "announcement_date": f"{yy}-{mm}-{dd}",
                            "announcement_time": hhmm,
                            "period_explicit": period_end_from_title(title) or "",
                            "rtype": classify(up), "title": title})
            return out
        except Exception as e:  # noqa: BLE001
            err = e
            time.sleep(1.5 * (attempt + 1))
    raise err


def scrape_announcements(work: Path, workers: int) -> None:
    """Stage 2: scrape HKEXnews results announcements into <work>/hk_announcements_raw.csv."""
    earnings_csv = work / "hk_earnings_2025plus.csv"
    ann_csv = work / "hk_announcements_raw.csv"
    done_file = work / "hk_ann_done.txt"
    failed_file = work / "hk_ann_failed.txt"

    df = pd.read_csv(earnings_csv, dtype={"code": str})
    codes = sorted(df["code"].str.zfill(5).unique())
    done = _load_set(done_file)
    todo = [c for c in codes if c not in done]
    log(f"announcements: distinct codes={len(codes)} done={len(done)} todo={len(todo)}")
    if not todo:
        log("announcements: nothing to do.")
        return

    new = not ann_csv.exists()
    fout = open(ann_csv, "a", newline="", encoding="utf-8-sig")
    w = csv.DictWriter(fout, fieldnames=ANN_COLS)
    if new:
        w.writeheader()
    fdone = open(done_file, "a", encoding="utf-8")
    ffail = open(failed_file, "a", encoding="utf-8")
    lock = threading.Lock()
    n_ok = n_rows = n_fail = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_ann_one, c): c for c in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            c = futs[fut]
            try:
                recs = fut.result()
                with lock:
                    for rec in recs:
                        w.writerow(rec)
                    fdone.write(c + "\n")
                    n_rows += len(recs)
                    n_ok += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    ffail.write(f"{c}\t{repr(e)[:100]}\n")
                    n_fail += 1
            if i % 50 == 0 or i == len(todo):
                with lock:
                    fout.flush(); fdone.flush(); ffail.flush()
                log(f"announcements: {i}/{len(todo)} | ok={n_ok} rows={n_rows} fail={n_fail}")

    fout.close(); fdone.close(); ffail.close()
    log(f"announcements DONE. ok={n_ok} rows={n_rows} fail={n_fail} -> {ann_csv}")


# ========================= stage 3: merge ================================

def merge_announcements(work: Path) -> None:
    """Stage 3: join announcement dates onto earnings rows.

    Output: <work>/hk_earnings_with_announcement.csv (original columns + announcement_date,
    announcement_time, ann_match, ann_title)."""
    earnings_csv = work / "hk_earnings_2025plus.csv"
    raw_csv = work / "hk_announcements_raw.csv"
    out = work / "hk_earnings_with_announcement.csv"

    e = pd.read_csv(earnings_csv, dtype=str)
    e["code"] = e["code"].str.zfill(5)
    e["period_end"] = e["REPORT_DATE"].str[:10]
    e["ptype"] = e["DATE_TYPE_CODE"].map(TYPE_OF_CODE).fillna("")

    raw = pd.read_csv(raw_csv, dtype=str)
    raw["code"] = raw["code"].str.zfill(5)
    # Re-derive type/period from the raw headline so classification can be iterated
    # without re-scraping; also drop IPO/placing "results" via the extended filter.
    up = raw["title"].fillna("").map(lambda t: re.sub(r"\s+", " ", t).upper())
    raw = raw[up.map(is_results)].copy()
    up = up[raw.index]
    raw["rtype"] = up.map(classify)
    raw["period_explicit"] = raw["title"].fillna("").map(
        lambda t: period_end_from_title(re.sub(r"\s+", " ", t)) or "")
    raw["adate"] = pd.to_datetime(raw["announcement_date"], errors="coerce")
    by_code = {c: g for c, g in raw.groupby("code")}

    a_date, a_time, a_match, a_title = [], [], [], []
    for _, r in e.iterrows():
        code, pe, pt = r["code"], r["period_end"], r["ptype"]
        g = by_code.get(code)
        chosen, method = None, ""
        if g is not None and pe:
            pe_ts = pd.Timestamp(pe)
            # --- Tier A: explicit period-end match (earliest publication) ---
            exact = g[g["period_explicit"] == pe].sort_values("adate")
            if not exact.empty:
                chosen, method = exact.iloc[0], "explicit"
            elif pt:
                cap = MAX_LAG_DAYS.get(pt, 180)
                win = g[(g["period_explicit"] == "") & (g["adate"] > pe_ts) &
                        (g["adate"] <= pe_ts + pd.Timedelta(days=cap))]
                # --- Tier B: same detected result-type, published just after period end ---
                cand = win[win["rtype"] == pt]
                if not cand.empty:
                    chosen = cand.sort_values("adate").iloc[0]
                    method = "type+lag"
                elif pt in ("annual", "interim"):
                    # --- Tier C: untyped "Results Announcement" in the period's window ---
                    cand = win[win["rtype"] == ""]
                    if not cand.empty:
                        chosen = cand.sort_values("adate").iloc[0]
                        method = "untyped+lag"
        if chosen is not None:
            a_date.append(chosen["announcement_date"]); a_time.append(chosen["announcement_time"])
            a_match.append(method); a_title.append(chosen["title"])
        else:
            a_date.append(""); a_time.append(""); a_match.append(""); a_title.append("")

    e_out = e.drop(columns=["period_end", "ptype"])
    e_out["announcement_date"] = a_date
    e_out["announcement_time"] = a_time
    e_out["ann_match"] = a_match
    e_out["ann_title"] = a_title
    e_out.to_csv(out, index=False, encoding="utf-8-sig")

    n = len(e_out)
    matched = sum(1 for x in a_date if x)
    log(f"merge: rows={n} with_announcement={matched} ({matched / n * 100:.1f}%) -> {out}")


# ===================== stage 4: finalize / partition =====================

def _load_universe_names(universe_csv: Path) -> dict[str, str]:
    names: dict[str, str] = {}
    if universe_csv.exists():
        with open(universe_csv, encoding="utf-8-sig") as f:
            for u in csv.DictReader(f):
                names[str(u.get("code", "")).zfill(5)] = u.get("name", "")
    return names


def _read_code_failures(path: Path) -> list[tuple[str, str]]:
    """Lines are '<code>\\t<error>' (error optional). Returns [(code, reason)]."""
    out: list[tuple[str, str]] = []
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                code, _, reason = line.partition("\t")
                out.append((code.strip().zfill(5), reason.strip()))
    return out


def finalize_outputs(work: Path, out_dir: Path,
                     lo: str | None, hi: str | None) -> int:
    """Stage 4: write <work>/hk_failures.csv and partition matched rows into
    <out_dir>/<YYYYMMDD>.csv. Only partitions whose date is within [lo, hi] are
    (re)written; if lo/hi are None that side is open. Returns a failure count for
    the process exit code (0 = clean run)."""
    merged = work / "hk_earnings_with_announcement.csv"
    out_fail = work / "hk_failures.csv"
    fin_failed = work / "hk_failed.txt"
    ann_failed = work / "hk_ann_failed.txt"

    with open(merged, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = list(reader)
    log(f"finalize: read {len(rows)} rows from {merged.name}")

    names = _load_universe_names(work / "hk_universe.csv")

    # ---- 1) consolidated failures file -------------------------------------
    matched = [r for r in rows if (r.get("announcement_date") or "").strip()]
    unmatched = [r for r in rows if not (r.get("announcement_date") or "").strip()]

    fail_cols = ["code", "name", "stage", "reason", "REPORT_DATE", "period_type"]
    with open(out_fail, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fail_cols)
        w.writeheader()
        n_fin = n_ann = 0
        for code, reason in _read_code_failures(fin_failed):
            w.writerow({"code": code, "name": names.get(code, ""),
                        "stage": "financial_scrape", "reason": reason or "scrape error",
                        "REPORT_DATE": "", "period_type": ""})
            n_fin += 1
        for code, reason in _read_code_failures(ann_failed):
            w.writerow({"code": code, "name": names.get(code, ""),
                        "stage": "announcement_scrape", "reason": reason or "scrape error",
                        "REPORT_DATE": "", "period_type": ""})
            n_ann += 1
        for r in unmatched:
            w.writerow({"code": str(r.get("code", "")).zfill(5), "name": r.get("name", ""),
                        "stage": "announcement_match",
                        "reason": "no_results_announcement_matched",
                        "REPORT_DATE": r.get("REPORT_DATE", ""),
                        "period_type": r.get("period_type", "")})
    log(f"finalize: wrote {out_fail.name} (financial_scrape={n_fin} "
        f"announcement_scrape={n_ann} announcement_match={len(unmatched)})")

    # ---- 2) partition matched rows by announcement date --------------------
    def in_range(ymd: str) -> bool:
        return (lo is None or ymd >= lo) and (hi is None or ymd <= hi)

    out_dir.mkdir(parents=True, exist_ok=True)
    by_day: dict[str, list[dict]] = {}
    for r in matched:
        ymd = (r.get("announcement_date") or "").replace("-", "")
        if len(ymd) != 8 or not ymd.isdigit() or not in_range(ymd):
            continue
        by_day.setdefault(ymd, []).append(r)

    # clear only our own dated partition files that fall within the selected range,
    # so a scoped --date-range run leaves out-of-range partitions untouched.
    removed = 0
    for p in glob.glob(os.path.join(str(out_dir), "*.csv")):
        base = os.path.basename(p)
        if YMD_RE.match(base) and in_range(base[:8]):
            os.remove(p)
            removed += 1
    if removed:
        log(f"finalize: cleared {removed} stale partition file(s) in {out_dir}")

    for ymd, recs in sorted(by_day.items()):
        path = out_dir / f"{ymd}.csv"
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(recs)
    total = sum(len(v) for v in by_day.values())
    log(f"finalize: wrote {len(by_day)} partition files ({total} records) -> {out_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--market", default=DEFAULT_MARKET, help="market code (default hkg)")
    p.add_argument("--asset", default=DEFAULT_ASSET, choices=sorted(ASSET_DIRS),
                   help="asset class (default eq)")
    p.add_argument("--date-range", dest="date_range", default=None,
                   help="YYYYMMDD or YYYYMMDD:YYYYMMDD; which announcement-date "
                        "partitions to (re)write. omit = all days found")
    p.add_argument("--out", type=Path, default=None,
                   help="output root; default derived from --market/--asset "
                        "(.../earnings/scrape)")
    p.add_argument("--work-dir", dest="work_dir", type=Path, default=None,
                   help="staging dir for intermediate CSVs + resume files; "
                        "default <out_parent>/scrape_staging")
    p.add_argument("--workers", type=int, default=min(8, len(os.sched_getaffinity(0))),
                   help="parallel scrape threads (default min(8, cpus))")
    p.add_argument("--skip-scrape", action="store_true",
                   help="skip network stages 1-2; reuse existing work-dir CSVs "
                        "(re-merge + re-partition only)")
    args = p.parse_args(argv)

    out = args.out or default_out(args.market, args.asset)
    work = args.work_dir or (out.parent / "scrape_staging")
    work.mkdir(parents=True, exist_ok=True)

    lo, hi = (None, None)
    if args.date_range:
        lo, hi = parse_date_range(args.date_range)

    log(f"[{args.market}/{args.asset}] HK earnings scrape | out={out} work={work} "
        f"workers={args.workers} range={lo or '*'}..{hi or '*'} "
        f"skip_scrape={args.skip_scrape}")

    try:
        if args.skip_scrape:
            log("skip-scrape: reusing existing work-dir CSVs (stages 3-4 only)")
        else:
            scrape_earnings(work, args.workers)         # stage 1
            scrape_announcements(work, args.workers)    # stage 2
        merge_announcements(work)                       # stage 3
        rc = finalize_outputs(work, out, lo, hi)        # stage 4
    except FileNotFoundError as e:
        print(f"FAILED: missing input file ({e}). "
              f"Run without --skip-scrape first.", file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"FAILED: {e}", file=sys.stderr)
        return 1

    log("done.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
