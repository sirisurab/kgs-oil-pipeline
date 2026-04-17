# Pipeline Orchestrator Component Tasks

**Module:** `kgs_pipeline/pipeline.py`
**Test file:** `tests/test_pipeline.py`

## Overview

The pipeline orchestrator ties all four stages (acquire, ingest, transform, features) into
a single end-to-end runnable entry point. It accepts CLI arguments controlling which years
to process, which stages to run, and how many parallel workers to use. It logs structured
per-stage timing information and handles stage-level errors gracefully, reporting which
stage failed without crashing the entire process.

The full pipeline can be invoked as:

```
python -m kgs_pipeline.pipeline --min-year 2024 --workers 5
```

or via the registered console script:

```
kgs-pipeline --min-year 2024 --workers 5
```

## Design Decisions

- Package name: `kgs_pipeline`; orchestrator lives at `kgs_pipeline/pipeline.py`.
- Logging: structured JSON via `kgs_pipeline.logging_utils.get_logger`; log stage name,
  start time, end time, and duration in seconds for every stage.
- Stage selection: the `--stages` argument accepts a space-separated list from
  `["acquire", "ingest", "transform", "features"]`. If omitted, all four stages run.
- Error handling: each stage is wrapped in a `try/except`. On failure, the error is
  logged and `SystemExit(1)` is raised. Downstream stages are not run if an upstream
  stage fails (stages are sequential, not independent).
- CLI entry point registered in `pyproject.toml`:
  `kgs-pipeline = "kgs_pipeline.pipeline:main"`
- All directory paths default to values from `kgs_pipeline.config`.
- `pyproject.toml` build-backend: `"setuptools.build_meta"` — never
  `"setuptools.backends.legacy:build"`.

---

## Task 01: Pipeline orchestrator function

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `run_pipeline(index_path: str, raw_dir: str, interim_dir: str, processed_dir: str, features_dir: str, min_year: int = 2024, workers: int = 5, stages: list[str] | None = None) -> dict`

**Description:**
Implement the top-level pipeline runner that chains all four stages in order. When
`stages` is `None`, all four stages run. When `stages` is provided, only the listed
stages run (in pipeline order, regardless of the order they are listed in the argument).

Stage execution order (when included):
1. `"acquire"` — calls `run_acquire(index_path, raw_dir, min_year, workers)` → list of
   downloaded `Path` objects.
2. `"ingest"` — calls `run_ingest(raw_dir, interim_dir, min_year)` → interim file count
   (integer).
3. `"transform"` — calls `run_transform(interim_dir, processed_dir)` → processed file
   count (integer).
4. `"features"` — calls `run_features(processed_dir, features_dir)` → manifest `Path`.

Return a summary dict with keys populated for the stages that were run:
- `"acquired"`: count of downloaded paths (acquire stage), or `None` if skipped.
- `"interim_files"`: interim file count (ingest stage), or `None` if skipped.
- `"processed_files"`: processed file count (transform stage), or `None` if skipped.
- `"manifest"`: string path to manifest JSON (features stage), or `None` if skipped.

Timing: record `time.perf_counter()` before and after each stage call. Log a JSON
message per stage: `{"stage": "acquire", "status": "completed", "duration_seconds": N}`.

**Error handling:**
- Catch any exception raised by a stage function. Log `{"stage": name, "status": "failed",
  "error": str(e)}`. Re-raise as `RuntimeError` so `main()` can catch it and exit with
  code 1.
- Validate that every element of `stages` is one of the four valid stage names. Raise
  `ValueError` for unrecognised stage names.

**Dependencies:** `time`, `kgs_pipeline.acquire`, `kgs_pipeline.ingest`,
`kgs_pipeline.transform`, `kgs_pipeline.features`, `kgs_pipeline.logging_utils`,
`kgs_pipeline.config`

**Test cases:**

- `@pytest.mark.unit` — Mock all four `run_*` functions. Call `run_pipeline` with
  `stages=None` (all stages). Assert all four mocks were called exactly once and the
  returned dict contains keys `"acquired"`, `"interim_files"`, `"processed_files"`,
  `"manifest"`.
- `@pytest.mark.unit` — Mock all four `run_*` functions. Call `run_pipeline` with
  `stages=["ingest", "transform"]`. Assert only `run_ingest` and `run_transform` mocks
  were called; `run_acquire` and `run_features` mocks were not called.
- `@pytest.mark.unit` — Given `stages=["acquire", "ingest", "transform", "features"]`,
  assert they execute in pipeline order regardless of the list order provided (test by
  checking mock call order using `unittest.mock.call_args_list`).
- `@pytest.mark.unit` — Given `run_acquire` raises `RuntimeError("network error")`,
  assert `run_pipeline` raises `RuntimeError` and `run_ingest` is never called.
- `@pytest.mark.unit` — Given `stages=["unknown_stage"]`, assert `ValueError` is raised
  before any stage mock is called.
- `@pytest.mark.unit` — Assert each stage's duration is logged (check that the logger
  receives a message containing `"duration_seconds"` after each stage completes).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: CLI entry point for full pipeline

**Module:** `kgs_pipeline/pipeline.py`
**Entry point:** `main()` function and `if __name__ == "__main__"` block

**Description:**
Implement `main()` as the CLI entry point for the full pipeline using `argparse`.

Arguments:
- `--index-path`: path to the lease index file (default from `config.LEASE_INDEX_PATH`).
- `--raw-dir`: directory for raw downloaded files (default from `config.RAW_DATA_DIR`).
- `--interim-dir`: directory for interim Parquet output (default from
  `config.INTERIM_DATA_DIR`).
- `--processed-dir`: directory for processed Parquet output (default from
  `config.PROCESSED_DATA_DIR`).
- `--features-dir`: directory for feature Parquet output (default from
  `config.FEATURES_DATA_DIR`).
- `--min-year`: integer, minimum year to include in the pipeline (default `2024`). Also
  accepted as `--start-year` (alias).
- `--end-year`: integer, reserved for future use. If provided, log a warning that
  `--end-year` is not yet implemented and the argument is ignored.
- `--workers`: integer, number of parallel download workers (default `5`).
- `--stages`: zero or more stage names from `["acquire", "ingest", "transform",
  "features"]`. If not provided, all stages run. Example:
  `--stages acquire ingest` runs only the first two stages.

Behaviour:
1. Parse arguments.
2. Log pipeline start: `{"event": "pipeline_start", "min_year": N, "stages": [...], "workers": W}`.
3. Call `run_pipeline(...)` with the parsed arguments.
4. Log pipeline completion: `{"event": "pipeline_complete", "summary": {...}}`.
5. Exit with code `0` on success, `1` on `RuntimeError` or unhandled exception.

Register in `pyproject.toml` under `[project.scripts]`:
`kgs-pipeline = "kgs_pipeline.pipeline:main"`

**Dependencies:** `argparse`, `sys`, `kgs_pipeline.config`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Patch `run_pipeline`. Set `sys.argv` to
  `["pipeline", "--workers", "3", "--min-year", "2024"]`. Assert `run_pipeline` was
  called with `workers=3` and `min_year=2024`.
- `@pytest.mark.unit` — Patch `run_pipeline`. Set `sys.argv` to
  `["pipeline", "--stages", "acquire", "ingest"]`. Assert `run_pipeline` was called with
  `stages=["acquire", "ingest"]`.
- `@pytest.mark.unit` — Patch `run_pipeline` to raise `RuntimeError("stage failed")`.
  Assert `main()` catches it and raises `SystemExit` with code `1`.
- `@pytest.mark.unit` — Set `sys.argv` to include `--end-year 2025`. Assert `main()`
  does not raise and a warning is logged about `--end-year` being ignored.
- `@pytest.mark.unit` — Set `sys.argv` to `["pipeline"]` (no arguments). Assert
  `main()` runs without error and calls `run_pipeline` with all four stages.

**Definition of done:** `main()` implemented, console script registered in
`pyproject.toml`, all tests pass, `ruff` and `mypy` report no errors.
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: README and requirements.txt

**Files:** `README.md`, `requirements.txt`

**Description:**
Write the project `README.md` and ensure `requirements.txt` is complete.

`README.md` must include the following sections:

1. **Project Overview** — one-paragraph description of the KGS oil & gas production data
   pipeline, its purpose (ingest 2024-2025 KGS Kansas lease production data for ML and
   analytics workflows), and the data source (KGS public data portal).

2. **Directory Structure** — a tree diagram showing:
   ```
   kgs_pipeline/       <- pipeline package
   tests/              <- pytest test suite
   data/
     external/         <- lease index
     raw/              <- downloaded lease files
     interim/          <- ingested Parquet
     processed/        <- cleaned Parquet
     features/         <- ML-ready Parquet + manifest.json
   references/         <- data dictionaries
   logs/               <- pipeline log files
   ```

3. **Setup** — step-by-step instructions:
   - Requires Python 3.11+
   - `make env` to create the virtual environment
   - `make install` to install all dependencies
   - Place the lease index file at `data/external/oil_leases_2020_present.txt`

4. **Usage** — command examples:
   - Run the full pipeline:
     `kgs-pipeline --min-year 2024 --workers 5`
   - Run individual stages:
     `kgs-acquire --min-year 2024 --workers 5`
     `kgs-ingest --min-year 2024`
     `kgs-transform`
     `kgs-features`
   - Run tests: `make test`
   - Run linter: `make lint`
   - Run type checker: `make typecheck`

5. **Pipeline Stages** — a table with one row per stage describing its input, output,
   and key operations:
   | Stage | Input | Output | Key operations |
   |-------|-------|--------|----------------|
   | Acquire | `data/external/oil_leases_2020_present.txt` | `data/raw/*.txt` | HTTP two-step download, Dask parallel |
   | Ingest | `data/raw/*.txt` | `data/interim/*.parquet` | CSV parse, year filter, Parquet write |
   | Transform | `data/interim/*.parquet` | `data/processed/*.parquet` | Pivot, type cast, deduplicate, outlier flag |
   | Features | `data/processed/*.parquet` | `data/features/*.parquet` + `manifest.json` | GOR, water cut, cumulative, rolling, lag |

6. **Data Dictionary** — a note directing the reader to `references/kgs_monthly_data_dictionary.csv`
   for column descriptions of the raw KGS lease files.

7. **Configuration** — description of all tuneable parameters in `kgs_pipeline/config.py`,
   including environment variable overrides.

`requirements.txt` must list every third-party package used across all pipeline modules
with pinned or minimum-version constraints. It must include at minimum:
`requests`, `beautifulsoup4`, `lxml`, `dask[complete]`, `pandas`, `pyarrow`,
`scikit-learn`, `numpy`, `urllib3`, `pytest`, `pytest-mock`, `ruff`, `mypy`,
`pandas-stubs`, `types-requests`.

**Test cases:** None for this task (documentation only).

**Definition of done:** `README.md` written with all six sections above. `requirements.txt`
present and complete. `ruff` reports no errors on the pipeline package.

---

## Task 04: End-to-end integration test

**Module:** `tests/test_pipeline.py`

**Description:**
Implement integration and orchestration tests for the pipeline module in
`tests/test_pipeline.py`. These complement the unit tests in Tasks 01 and 02.

**Test cases:**

- `@pytest.mark.unit` — Call `run_pipeline` with all four stage mocks returning valid
  values (acquire returns `[Path("data/raw/lp1.txt")]`, ingest returns `2`, transform
  returns `3`, features returns `Path("data/features/manifest.json")`). Assert the
  returned summary dict is:
  `{"acquired": 1, "interim_files": 2, "processed_files": 3, "manifest": "data/features/manifest.json"}`.
- `@pytest.mark.unit` — Assert that when `stages=["transform", "features"]` only, the
  returned dict has `"acquired": None` and `"interim_files": None`.
- `@pytest.mark.unit` — Assert that calling `run_pipeline` with `workers=10` passes
  `max_workers=10` (or `workers=10`) through to `run_acquire`.
- `@pytest.mark.integration` — Given a fully populated pipeline run (all stages have
  previously executed successfully and `data/features/` contains Parquet files and a
  `manifest.json`), call `run_pipeline` with `stages=["features"]` only and assert the
  manifest `Path` returned exists on disk and is valid JSON.

**Definition of done:** All test cases implemented in `tests/test_pipeline.py`, all unit
tests pass without network access, `ruff` and `mypy` report no errors.
`requirements.txt` updated with all third-party packages imported in this task.
