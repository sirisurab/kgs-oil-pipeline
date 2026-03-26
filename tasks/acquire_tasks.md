# Acquire Component — Task Specifications

## Overview

The acquire component is responsible for:
1. Providing shared configuration constants (`kgs_pipeline/config.py`) and utility helpers (`kgs_pipeline/utils.py`) used by all pipeline stages.
2. Reading the KGS lease index file (`data/external/oil_leases_2020_present.txt`) to extract all unique lease URLs for leases active between 2020 and 2025.
3. Using Playwright (async API) with a rate-limiting `asyncio.Semaphore(5)` to scrape each lease's monthly data `.txt` file from the KGS web portal.
4. Saving each downloaded raw file to `data/raw/`.
5. Providing a synchronous orchestrator entry-point (`run_acquire_pipeline()`) that drives the async scraping workflow via Dask delayed tasks for parallel scheduling, with a single `.compute()` call at the end.

**Source modules:** `kgs_pipeline/config.py`, `kgs_pipeline/utils.py`, `kgs_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

---

## Design Decisions & Constraints

- Python 3.11+. All functions must carry full type hints. All modules must have module-level docstrings.
- Playwright async API (`playwright.async_api`). `asyncio.Semaphore(5)` enforces a maximum of 5 concurrent page contexts at any time to avoid overloading the KGS server.
- A lease URL follows the pattern `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`.
- The "Save Monthly Data to File" button navigates to `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<LEASE_KID>`.
- The download link on the MonthSave page is a relative `.txt` filename (e.g. `lp564.txt`). The resolved download URL is `https://chasm.kgs.ku.edu/ords/<filename>`.
- All downloaded raw files land in `data/raw/`. The directory must be created if it does not exist.
- Acquire is **idempotent**: if a file already exists in `data/raw/` and its size is greater than 0 bytes, it must be skipped without re-downloading and without raising an error.
- A failed download for one lease must be logged and skipped; it must not abort the entire pipeline.
- Dask delayed tasks wrap the per-lease scraping coroutines so the full set of leases is submitted as a graph; a single `.compute()` call in `run_acquire_pipeline()` executes them all.
- Use structured Python `logging` (not print). All log records must include timestamps.
- Use `pydantic` (v2) `BaseSettings` / `BaseModel` for configuration validation in `config.py`.
- The data dictionary for `oil_leases_2020_present.txt` is at `references/kgs_archives_data_dictionary.csv`. Columns of interest: `LEASE KID`, `URL`.
- The data dictionary for per-lease monthly `.txt` files is at `references/kgs_monthly_data_dictionary.csv`. Key columns: `LEASE KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`.

---

## Task 01: Implement pipeline configuration module

**Module:** `kgs_pipeline/config.py`
**Class:** `PipelineConfig` (Pydantic `BaseSettings`)

**Description:**
Create a Pydantic v2 `BaseSettings` subclass `PipelineConfig` that holds all configurable pipeline constants. All field values must have defaults so the class can be instantiated without any environment variables. The module must also export a module-level singleton `CONFIG = PipelineConfig()` for use by other modules.

Fields to include:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `raw_dir` | `Path` | `Path("data/raw")` | Directory for raw downloaded files |
| `interim_dir` | `Path` | `Path("data/interim")` | Directory for interim Parquet files |
| `processed_dir` | `Path` | `Path("data/processed")` | Directory for cleaned processed files |
| `features_dir` | `Path` | `Path("data/processed/features")` | Directory for ML feature Parquet files |
| `external_dir` | `Path` | `Path("data/external")` | Directory for external source files |
| `logs_dir` | `Path` | `Path("logs")` | Directory for log files |
| `lease_index_file` | `Path` | `Path("data/external/oil_leases_2020_present.txt")` | Lease index CSV/txt file |
| `year_start` | `int` | `2020` | First year of data to process |
| `year_end` | `int` | `2025` | Last year of data to process (inclusive) |
| `kgs_base_url` | `str` | `"https://chasm.kgs.ku.edu/ords"` | Base URL for KGS ORDS portal |
| `kgs_main_lease_path` | `str` | `"oil.ogl5.MainLease"` | Path segment for main lease pages |
| `kgs_month_save_path` | `str` | `"oil.ogl5.MonthSave"` | Path segment for monthly data save pages |
| `max_concurrent_requests` | `int` | `5` | Max concurrent Playwright page contexts |
| `scrape_timeout_ms` | `int` | `30000` | Playwright navigation timeout in milliseconds |
| `http_retry_attempts` | `int` | `3` | Number of retry attempts for failed downloads |
| `http_retry_backoff_s` | `float` | `2.0` | Base backoff in seconds between retries |
| `oil_unit` | `str` | `"BBL"` | Unit for oil production volumes |
| `gas_unit` | `str` | `"MCF"` | Unit for gas production volumes |
| `water_unit` | `str` | `"BBL"` | Unit for water production volumes |
| `oil_max_bbl_per_month` | `float` | `50000.0` | Upper bound for single-well monthly oil production (outlier threshold) |
| `rolling_windows` | `list[int]` | `[3, 6, 12]` | Rolling window sizes in months for feature engineering |
| `decline_rate_clip_min` | `float` | `-1.0` | Lower clip bound for computed decline rate |
| `decline_rate_clip_max` | `float` | `10.0` | Upper clip bound for computed decline rate |
| `dask_n_workers` | `int` | `4` | Number of Dask workers for local cluster |
| `parquet_engine` | `str` | `"pyarrow"` | Parquet read/write engine |
| `log_level` | `str` | `"INFO"` | Python logging level string |

**Validation:**
- `year_start` must be >= 2000 and <= `year_end`.
- `year_end` must be <= 2030.
- `max_concurrent_requests` must be between 1 and 20.
- `oil_max_bbl_per_month` must be > 0.
- Use Pydantic `@field_validator` decorators for these constraints.

**Error handling:** `ValidationError` is raised automatically by Pydantic if constraints are violated. No additional error handling required.

**Dependencies:** pydantic, pathlib

**Test cases (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Instantiate `PipelineConfig()` with no arguments and assert all fields have their documented defaults (spot-check at least 8 fields).
- `@pytest.mark.unit` — Assert `CONFIG` (module-level singleton) is an instance of `PipelineConfig`.
- `@pytest.mark.unit` — Construct `PipelineConfig(year_start=1990)` and assert a `ValidationError` is raised (year below 2000).
- `@pytest.mark.unit` — Construct `PipelineConfig(year_start=2025, year_end=2020)` and assert a `ValidationError` is raised (start > end).
- `@pytest.mark.unit` — Construct `PipelineConfig(max_concurrent_requests=0)` and assert a `ValidationError` is raised.
- `@pytest.mark.unit` — Assert that `CONFIG.raw_dir`, `CONFIG.interim_dir`, `CONFIG.processed_dir`, `CONFIG.features_dir` are all `Path` instances.

**Definition of done:** Module is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement shared utility helpers

**Module:** `kgs_pipeline/utils.py`

**Description:**
Implement a set of shared utility functions and decorators used across all pipeline components.

**Functions and signatures to implement:**

### `setup_logging(name: str, log_dir: Path, level: str = "INFO") -> logging.Logger`
Configure and return a named logger that writes structured log records (format: `%(asctime)s | %(name)s | %(levelname)s | %(message)s`) to both a rotating file handler (`logs/<name>.log`, max 10 MB, 3 backups) and a `StreamHandler`. Create `log_dir` if it does not exist. Return the configured logger.

### `retry(max_attempts: int = 3, backoff_s: float = 2.0, exceptions: tuple[type[Exception], ...] = (Exception,))`
A function decorator factory. When the decorated function raises any exception in `exceptions`, it is retried up to `max_attempts` times with exponential backoff: `backoff_s * (2 ** attempt)` seconds between attempts. After exhausting all retries, re-raise the last exception. Log each retry attempt at WARNING level using the module logger.

### `timer(logger: logging.Logger | None = None)`
A decorator factory. Wraps a function to record its wall-clock execution time. If `logger` is provided, log a message at DEBUG level: `"<function_name> completed in <elapsed:.3f>s"`. Always return the wrapped function's return value unchanged.

### `compute_file_hash(path: Path, algorithm: str = "sha256") -> str`
Compute and return the hex digest of the file at `path` using `hashlib`. Read the file in 64 KB chunks. Raise `FileNotFoundError` if the file does not exist.

### `ensure_dir(path: Path) -> Path`
Create `path` (and all parents) if it does not exist. Return `path`. Must be idempotent (no error if already exists).

### `is_valid_raw_file(path: Path) -> bool`
Return `True` if and only if all three conditions hold: (a) `path.exists()` and `path.stat().st_size > 0`; (b) the file is decodable as UTF-8 text without errors; (c) the file contains at least two lines (header + one data row). Return `False` otherwise. Do not raise exceptions.

**Error handling:**
- `compute_file_hash` raises `FileNotFoundError` for missing paths.
- `retry` logs each failure before sleeping; re-raises after last attempt.
- All other functions must not raise except on genuinely unrecoverable OS errors.

**Dependencies:** logging, logging.handlers, pathlib, hashlib, time, functools

**Test cases (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Call `ensure_dir` with a path inside `tmp_path` (pytest fixture); assert the directory is created and calling again does not raise.
- `@pytest.mark.unit` — Call `compute_file_hash` on a known small text file (written with `tmp_path`) and assert the returned string is a 64-character hex string.
- `@pytest.mark.unit` — Call `compute_file_hash` on a non-existent path and assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Decorate a function with `@retry(max_attempts=3, backoff_s=0.01, exceptions=(ValueError,))` that raises `ValueError` on every call. Assert it raises `ValueError` after exactly 3 attempts (use a call counter with `nonlocal`).
- `@pytest.mark.unit` — Decorate a function with `@retry(max_attempts=3, backoff_s=0.01)` that succeeds on the second attempt. Assert it returns the correct result and was called exactly twice.
- `@pytest.mark.unit` — Call `is_valid_raw_file` on a file with 0 bytes; assert it returns `False`.
- `@pytest.mark.unit` — Call `is_valid_raw_file` on a file containing only a header line (no data row); assert it returns `False`.
- `@pytest.mark.unit` — Call `is_valid_raw_file` on a valid two-line UTF-8 text file; assert it returns `True`.
- `@pytest.mark.unit` — Call `is_valid_raw_file` on a file containing invalid UTF-8 bytes; assert it returns `False` and does not raise.
- `@pytest.mark.unit` — Decorate a function with `@timer()` and verify it returns the correct value; verify it does not raise.

**Definition of done:** Module is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement lease URL loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_urls(lease_index_path: Path) -> list[str]`

**Description:**
Read the lease index file at `lease_index_path` (a pipe-delimited or comma-delimited `.txt` file in the format described by `references/kgs_archives_data_dictionary.csv`). Extract the `URL` column values, drop nulls and duplicates, and return a deduplicated list of lease URL strings. The function must be robust to both comma and pipe delimiters — attempt comma first, fall back to pipe. Each URL in the returned list must be a non-empty string.

**Error handling:**
- Raise `FileNotFoundError` if `lease_index_path` does not exist.
- Raise `KeyError` with a descriptive message if the `URL` column is absent from the file.
- Log the number of unique lease URLs loaded at INFO level.

**Dependencies:** pandas, pathlib, logging

**Test cases (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Write a minimal CSV fixture with a `URL` column to `tmp_path`; call `load_lease_urls` and assert the returned list has the correct length and all entries are non-empty strings.
- `@pytest.mark.unit` — Write a fixture with duplicate URL values; assert the returned list contains no duplicates.
- `@pytest.mark.unit` — Write a fixture with some null/empty URL values; assert nulls are not present in the result.
- `@pytest.mark.unit` — Pass a non-existent path; assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Write a fixture with no `URL` column; assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement single-lease Playwright scraper

**Module:** `kgs_pipeline/acquire.py`
**Function:** `scrape_lease_page(lease_url: str, output_dir: Path, semaphore: asyncio.Semaphore, playwright_browser) -> Path | None`

**Description:**
An async function that, given a lease URL, navigates to the lease page, finds the "Save Monthly Data to File" button, clicks it to navigate to the MonthSave page, finds the `.txt` download link, and downloads the file content to `output_dir/<filename>.txt`. Returns the `Path` to the saved file on success, or `None` on failure (after logging the error).

The semaphore must be acquired before opening a new page context and released after the page is closed, regardless of success or failure — use an `async with semaphore:` block.

**Idempotency:** Before scraping, check if the output file already exists and `is_valid_raw_file()` returns `True` for it. If so, log at DEBUG level that the file is being skipped and return the existing path immediately without opening a browser page.

**Timeout:** Use `CONFIG.scrape_timeout_ms` for all Playwright navigation calls.

**Error handling:**
- Wrap the entire scrape in a try/except block. On any exception (`PlaywrightError`, `TimeoutError`, or any other), log the error at ERROR level (including `lease_url`) and return `None`.
- Do not re-raise exceptions — a single failed lease must not stop the overall pipeline.
- Define a custom exception class `ScrapingError(Exception)` in `acquire.py`. Raise it internally (then catch) when the "Save Monthly Data to File" button is not found on the page, and when the download link is not found on the MonthSave page.

**Dependencies:** playwright (async_api), asyncio, pathlib, logging

**Test cases (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Using `unittest.mock.AsyncMock`, mock the Playwright browser and page objects so that the button is found and a fake `.txt` URL is returned. Assert the function returns a `Path` pointing to a `.txt` file inside `tmp_path`.
- `@pytest.mark.unit` — Mock the page so that the "Save Monthly Data to File" button is **not** found. Assert the function returns `None` and does not raise.
- `@pytest.mark.unit` — Mock the page so that the MonthSave page has no `.txt` download link. Assert the function returns `None` and does not raise.
- `@pytest.mark.unit` — Create a pre-existing valid file at the expected output path; assert the function returns the existing path without calling any Playwright methods (verify via mock call count).
- `@pytest.mark.unit` — Mock the navigation to raise a `TimeoutError`. Assert the function returns `None` and does not raise.

**Definition of done:** Function and `ScrapingError` class are implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement parallel acquire orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire_pipeline(lease_index_path: Path | None = None, output_dir: Path | None = None, max_workers: int | None = None) -> list[Path]`

**Description:**
The synchronous orchestrator that drives the full acquisition workflow:

1. Resolve `lease_index_path` (default: `CONFIG.lease_index_file`), `output_dir` (default: `CONFIG.raw_dir`), and `max_workers` (default: `CONFIG.dask_n_workers`). Call `ensure_dir(output_dir)`.
2. Call `load_lease_urls(lease_index_path)` to obtain the list of lease URLs.
3. Wrap each per-lease async scrape call inside a `dask.delayed` task. Each delayed task must call an inner sync helper `_run_single_lease(lease_url, output_dir)` that uses `asyncio.run()` to execute the async `scrape_lease_page` function within a fresh Playwright context (`async_playwright()`).
4. Collect all delayed tasks into a list and call `dask.compute(*tasks, scheduler="threads", num_workers=max_workers)` — this is the **only** `.compute()` call in the acquire component.
5. Filter the compute results to exclude `None` values (failed leases) and return the list of successfully written `Path` objects.
6. Log a summary at INFO level: total leases attempted, succeeded, and failed.

**Error handling:**
- Individual lease failures produce `None` in results (handled inside `scrape_lease_page`). The orchestrator must count and log failures but must not re-raise them.
- If `load_lease_urls` raises, the orchestrator must propagate the exception (do not catch it here).

**Dependencies:** dask, asyncio, playwright, pathlib, logging

**Test cases (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Patch `load_lease_urls` to return 3 fake URLs and patch `_run_single_lease` to return a fake `Path` for each. Assert `run_acquire_pipeline` returns a list of 3 `Path` objects.
- `@pytest.mark.unit` — Patch `_run_single_lease` to return `None` for all inputs. Assert the return value is an empty list and no exception is raised.
- `@pytest.mark.unit` — Patch `_run_single_lease` to return a `Path` for 2 out of 3 URLs (the third returns `None`). Assert the returned list has length 2.
- `@pytest.mark.unit` — Patch `load_lease_urls` to raise `FileNotFoundError`. Assert `run_acquire_pipeline` propagates `FileNotFoundError`.
- `@pytest.mark.integration` — (Skipped by default unless `--integration` flag is passed to pytest.) Call `run_acquire_pipeline` against 2 real lease URLs. Assert that the files land in `data/raw/`, each has size > 0, and `is_valid_raw_file` returns `True` for each.

**Acquire idempotency test (`tests/test_acquire.py`):**
- `@pytest.mark.unit` — Call `run_acquire_pipeline` (with patched `_run_single_lease` that writes real small fixture files to `tmp_path`) twice on the same `output_dir`. Assert the file count is the same after both runs (not doubled). Assert no exception is raised on the second run. Assert the file contents are unchanged after the second run.

**Acquired file integrity test (`tests/test_acquire.py`):**
- `@pytest.mark.integration` — After a real or fixture-driven acquire run, iterate all files in `data/raw/`. Assert: (a) every file has `stat().st_size > 0`; (b) every file is decodable as UTF-8; (c) every file has at least 2 lines.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
