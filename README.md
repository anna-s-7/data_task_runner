# data_task_runner

A lightweight framework for running **market-data ETL tasks** from a Jupyter notebook UI.

Each task is a self-contained Python script under `tasks/`; the notebook
(`runner_UI.ipynb`) is a thin config-and-run front end over them. Today there are three
tasks: convert daily HKG market-snapshot zips and daily HKG market-trade zips into
per-ticker parquet files, and scrape HKG company earnings into per-announcement-date
CSV partitions.

Every task declares a **`kind`** in its `TASKS` config section, which selects how the
notebook drives it:

- `zip_days` — discover days by globbing `<src>/*.zip`; pass `--src` + `--date-range`
  (the two `md_*_zip_to_parquet` tasks).
- `scrape` — no local input; pass `--out`/`--work-dir` + `--date-range`
  (`hkg_earnings_scrape`).

> **For future AI coding agents:** the notebook runs this repo's own `tasks/` script via
> this repo's `.venv`. Remaining cleanups are tracked under
> [Notes for future refactoring](#notes-for-future-refactoring).

## Quick start

**Via the notebook (primary UI):**

1. Open `runner_UI.ipynb`.
2. Edit the **CONFIG** cell (task, date range, paths).
3. *Run All*. `DRY_RUN = True` (the default) only lists the days that *would* run —
   set `DRY_RUN = False` to actually process them.

## Repository structure

| Path                  | Purpose                                                                 |
| --------------------- | ----------------------------------------------------------------------- |
| `runner_UI.ipynb`     | Notebook UI: pick a task + date range, then run it (see below).         |
| `tasks/`              | One Python module per data task. Each is a standalone CLI.              |
| `shared/`             | Utilities shared across tasks (currently empty; reserved).             |
| `commands.md`         | Scratch notes only (a data-download command). Not part of code logic.  |
| `.venv/`              | Repo-local Python environment. Managed — do not edit its internals.    |

## How tasks are organized

Every module in `tasks/` is a self-contained ETL task that can be run from the CLI or
invoked by the notebook. New tasks should follow the same convention as the existing one:

- Expose `main(argv: list[str] | None = None) -> int` as the entry point, guarded by
  `if __name__ == "__main__": raise SystemExit(main())`.
- Parse arguments with `argparse`, accepting `--src`, `--out`, and `--workers` where
  applicable, plus optional positional `dates` (`YYYYMMDD`).
- Parallelize per-unit work (e.g. per day) with `ProcessPoolExecutor`.
- Have each worker return a small **summary dict** so the parent can report
  progress/failures; return exit code `0` on full success, `1` if anything failed.
- Put any logic shared by multiple tasks in `shared/` and import it.

### Existing task: `md_snapshot_zip_to_parquet`

`tasks/md_snapshot_zip_to_parquet.py` — converts daily snapshot zips into
per-ticker parquet. Market and asset class are selected via `--market` / `--asset`.

**Input** (one zip per trading day):

```
<src>/YYYYMM/YYYYMMDD.zip
    00001.csv          # one CSV per ticker, at the zip root
    00002.csv
    ...
```

**Output:**

```
<out>/<yyyy>/<YYYYMMDD>/<ticker>.parquet      # pyarrow, snappy, index=False
```

**Arguments:**

| Arg            | Meaning                                                              |
| -------------- | ------------------------------------------------------------------- |
| `--market`     | Market code (default `hkg`). Used in the output path.               |
| `--asset`      | Asset class (default `eq`; `eq`→`equity_data`, `crypto`→`crypto_data`). |
| `--date-range` | `YYYYMMDD` or `YYYYMMDD:YYYYMMDD` (either side open); omit = every `*.zip` in `--src`. |
| `--src`        | Root holding `YYYYMM/YYYYMMDD.zip` files (default `/home/wangfc/tmp_data`). |
| `--out`        | Output root (default derived: `/data/<asset>_data/<market>/md_snapshot/raw`). |
| `--workers`    | Parallel processes (default `min(20, cpus)`).                       |

Key functions: `process_day(zip_path, out_base) -> dict` (per-day worker, returns
`{date, members, written, rows, errors, out_dir}`) and `main(argv)` (CLI + fan-out).

**Dependencies:** `pandas`, `pyarrow` (plus the standard library). There is no
dependency manifest yet — see notes below.

### Existing task: `md_trade_zip_to_parquet`

`tasks/md_trade_zip_to_parquet.py` — converts daily trade zips into per-ticker
parquet. Same shape as `md_snapshot_zip_to_parquet`; the only differences are the
default `--src` and the output subtree (`md_trade` instead of `md_snapshot`).

**Input** (one zip per trading day; each ticker CSV holds that day's tick-by-tick
trades, columns `ticker,tradeid,date,datetime,price,volume,type,cancelflag`):

```
<src>/YYYYMM/YYYYMMDD.zip
    00001.csv          # one CSV per ticker, at the zip root
    00002.csv
    ...
```

**Output:**

```
<out>/<yyyy>/<YYYYMMDD>/<ticker>.parquet      # pyarrow, snappy, index=False
```

**Arguments:** identical to `md_snapshot_zip_to_parquet`, except:

| Arg            | Meaning                                                              |
| -------------- | ------------------------------------------------------------------- |
| `--src`        | Root holding `YYYYMM/YYYYMMDD.zip` files (default `/home/wangfc/md_trade`). |
| `--out`        | Output root (default derived: `/data/<asset>_data/<market>/md_trade/raw`). |

Key functions mirror the snapshot task: `process_day(zip_path, out_base) -> dict`
and `main(argv)`.

**Dependencies:** `pandas`, `pyarrow` (plus the standard library).

### Existing task: `hkg_earnings_scrape`

`tasks/hkg_earnings_scrape.py` — scrapes HK-listed company earnings and partitions them
by **announcement date**. It is a single-file port of the four-stage pipeline in the
sibling `trial/` repo (`hk_earnings_scraper.py` → `hk_announcement_dates.py` →
`hk_merge_announcements.py` → `hk_finalize_outputs.py`), folded into one task. Unlike the
zip tasks it reads **no local input** — it scrapes the network — so its `kind` is
`scrape` and it has no `--src`.

Stages (run in sequence; each is resumable / idempotent):

1. **financials** — per-company reporting-period financials since 2025-01-01, from
   Eastmoney via `akshare` (resumable via `hk_done.txt` / `hk_failed.txt`).
2. **announcements** — publication date/time of each results announcement, from HKEXnews
   (resumable via `hk_ann_done.txt` / `hk_ann_failed.txt`).
3. **merge** — attach each announcement date to its earnings row (explicit period-end /
   result-type matching).
4. **finalize** — write a consolidated `hk_failures.csv`, then partition matched rows by
   announcement date.

**Output:**

```
<out>/<YYYYMMDD>.csv      one file per announcement date; every enriched earnings
                          record whose results announcement was published that day
```

By default `<out>` is `/data/<asset>_data/<market>/earnings/scrape`
(e.g. `/data/equity_data/hkg/earnings/scrape`). Intermediate CSVs and resume files live
under `--work-dir` (default `<out_parent>/scrape_staging`), so re-running skips
already-scraped companies.

**Arguments:**

| Arg            | Meaning                                                              |
| -------------- | ------------------------------------------------------------------- |
| `--market`     | Market code (default `hkg`). Used in the output path.               |
| `--asset`      | Asset class (default `eq`; `eq`→`equity_data`, `crypto`→`crypto_data`). |
| `--date-range` | `YYYYMMDD` or `YYYYMMDD:YYYYMMDD` (either side open); selects which announcement-date partitions to (re)write. omit = all days found. |
| `--out`        | Output root (default derived: `/data/<asset>_data/<market>/earnings/scrape`). |
| `--work-dir`   | Staging dir for intermediate CSVs + resume files (default `<out_parent>/scrape_staging`). |
| `--workers`    | Parallel scrape **threads** (default `min(8, cpus)`; network-bound, so threads not processes). |
| `--skip-scrape`| Skip the network stages 1–2 and reuse existing `--work-dir` CSVs (re-merge + re-partition only). Handy for offline re-runs / testing. |

Key functions: `scrape_earnings`, `scrape_announcements`, `merge_announcements`,
`finalize_outputs`, and `main(argv)` (CLI + stage orchestration).

**Dependencies:** `pandas`, `requests`, `akshare` (plus the standard library).

## How `runner_UI.ipynb` is used

The notebook has four cells:

1. **Intro** (markdown) — lists the available tasks and the safety note.
2. **CONFIG** — the only cell you normally edit. Holds the shared settings (`TASK`,
   `WORKERS`, `DRY_RUN`, repo-derived `VENV_PY`) and a `TASKS` dict with **one config
   section per task**; `TASK` selects which section runs (`CFG = TASKS[TASK]`). Each
   `md_snapshot_zip_to_parquet` section sets:
   - `script` — derived from `REPO` (`Path.cwd()`), i.e. this repo's task script.
   - `market` / `asset` — passed through as `--market` / `--asset` (default `hkg` / `eq`).
   - `start` / `end` — inclusive `YYYYMMDD` date range; `None` = earliest/latest available.
   - `src` / `out` — input zip folder and output root (`out = None` derives it from `market`/`asset`).
3. **Day discovery** — globs `CFG["src"]/*.zip`, extracts `YYYYMMDD` stems, and filters to the
   `start..end` range (inclusive string comparison on `YYYYMMDD`).
4. **Execution** — dispatches on each task's `kind`. For `zip_days` it builds
   `[VENV_PY, CFG["script"], --market, --asset, --date-range lo:hi, --src, --out, --workers]`;
   for `scrape` it builds `[..., --market, --asset, --date-range, --out, --work-dir, --workers]`
   (no `--src`, no zip discovery). Unless `DRY_RUN`, it runs the command via
   `subprocess.Popen`, streaming output live and reporting the exit code.

The work runs in a dedicated venv via the tested script, so it always uses the right
pandas/pyarrow regardless of which kernel the notebook is on. `SCRIPT`/`VENV_PY` resolve
to this repo's `tasks/md_snapshot_zip_to_parquet.py` and its `.venv` via `Path.cwd()`.

## Notes for future refactoring

The repo is mid-refactor. Known gaps to close:

- **Host-specific `--src` default.** `--src` still defaults to the machine-specific
  `/home/wangfc/tmp_data` (zips discovered under `YYYYMM/` subfolders). A cleaner design
  would not bake in a host-specific default path.
- **Per-task execution flow.** The notebook now dispatches on each task's `kind`
  (`zip_days` vs `scrape`) via a `RUNNERS` table, so tasks with different input shapes are
  branched rather than assuming the zip shape. New input shapes add a new `kind` + runner.
- **Dependency manifest.** `requirements.txt` declares `pandas`, `pyarrow`, `requests`,
  `akshare`. Install into the repo venv with `.venv/bin/pip install -r requirements.txt`.
- **`commands.md`** is scratch notes, not logic — safe to ignore or remove.
