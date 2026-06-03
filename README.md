# data_task_runner

A lightweight framework for running **market-data ETL tasks** from a Jupyter notebook UI.

Each task is a self-contained Python script under `tasks/`; the notebook
(`runner_UI.ipynb`) is a thin config-and-run front end over them. Today there are two
tasks: convert daily HKG market-snapshot zips, and daily HKG market-trade zips, into
per-ticker parquet files.

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
4. **Execution** — builds `[VENV_PY, CFG["script"], --market, --asset, --date-range lo:hi, --src,
   --out, --workers]` and, unless `DRY_RUN`, runs it via `subprocess.Popen`, streaming output
   live and reporting the exit code.

The work runs in a dedicated venv via the tested script, so it always uses the right
pandas/pyarrow regardless of which kernel the notebook is on. `SCRIPT`/`VENV_PY` resolve
to this repo's `tasks/md_snapshot_zip_to_parquet.py` and its `.venv` via `Path.cwd()`.

## Notes for future refactoring

The repo is mid-refactor. Known gaps to close:

- **Host-specific `--src` default.** `--src` still defaults to the machine-specific
  `/home/wangfc/tmp_data` (zips discovered under `YYYYMM/` subfolders). A cleaner design
  would not bake in a host-specific default path.
- **Per-task execution flow.** The notebook now keys config off a `TASKS` registry (one
  section per task), but the day-discovery and execution cells still assume the
  `md_snapshot_zip_to_parquet` shape (zip globbing + `--market/--asset/--date-range`). Tasks
  with different inputs will need their flow generalized or branched on `TASK`.
- **No dependency manifest.** Add a `requirements.txt` / `pyproject.toml` declaring
  `pandas` and `pyarrow`.
- **`commands.md`** is scratch notes, not logic — safe to ignore or remove.
