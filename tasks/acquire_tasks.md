# Acquire Component — Task Specifications

## Overview
The acquire component is responsible for:
1. Reading the lease index file (`kgs/data/external/oil_leases_2020_present.txt`) to extract all unique lease URLs.
2. Using Playwright (async API) with a rate-limiting `asyncio.Semaphore(5)` to scrape each lease's monthly data `.txt` file from the KGS web portal.
3. Saving each downloaded file to `kgs/data/raw/`.
4. Providing a synchronous orchestrator entry-point that drives the async scraping workflow via Dask for parallel task scheduling.

**Source module:** `kgs_pipeline/acquire.py`  
**Test file:** `tests/test_acquire.py`

---

## Design Decisions & Constraints
- Python 3.11+, Playwright async API (`playwright.async_api`), `asyncio.Semaphore(5)` for concurrency control.
- Maximum 5 concurrent browser page contexts at any time to avoid overloading the KGS server.
- All downloaded raw files land in `kgs/data/raw/` — the directory must be created if it does not exist.
- A lease URL follows the pattern `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`.
- The "Save Monthly Data to File" button leads to `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<LEASE_KID>`.
- On the MonthSave page a link labelled like `lp<N>.txt` is the download target; clicking it triggers a file download.
- If a lease page has no "Save Monthly Data to File" button, raise a custom `ScrapingError`.
- If the MonthSave page has no download link, log a warning and return `None` for that lease (do not crash the run).
- Files are named exactly as the server provides them (e.g. `lp564.txt`). If a file with that name already exists in `kgs/data/raw/`, skip re-downloading it (idempotent re-runs).
- Use a custom `ScrapingError(Exception)` class defined in `acquire.py`.
- All configuration values (raw data directory, concurrency limit, base URL) must be read from `kgs_pipeline/config.py` — **never hardcoded** in `acquire.py`.
- Dask is used to build a delayed task graph over lease chunks, with `.compute()` called exactly **once** in the orchestrator.

---

## Task 01: Create project scaffolding and `.gitignore`

**Module:** `kgs/.gitignore`  
**Function:** N/A — file creation task

**Description:**  
Create a `.gitignore` file at the project root (`kgs/`) to prevent sensitive, generated or large data files from being committed to version control.

The `.gitignore` must exclude:
- `.env` and all `*.env` files
- `__pycache__/` directories and `*.pyc` files
- `data/raw/` — raw downloaded data files
- `data/interim/` — intermediate transformed data
- `data/processed/` — final processed Parquet files
- `data/external/` — third-party source files
- Any files that could contain API keys or credentials (`*.key`, `*.secret`, `secrets.yaml`, `secrets.json`)
- Playwright browser binaries cache: `.playwright/` and `playwright/.local-browsers/`
- Dask worker scratch directories: `dask-worker-space/`
- Pytest cache: `.pytest_cache/`
- MyPy cache: `.mypy_cache/`
- Ruff cache: `.ruff_cache/`
- IDE folders: `.vscode/`, `.idea/`
- OS artefacts: `.DS_Store`, `Thumbs.db`
- Jupyter checkpoint folders: `.ipynb_checkpoints/`

**Definition of done:** `.gitignore` file exists at `kgs/.gitignore` with all entries listed above. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement `config.py` — central configuration

**Module:** `kgs_pipeline/config.py`  
**Function:** Module-level constants (no callable functions required)

**Description:**  
Create `kgs_pipeline/config.py` as the single source of truth for all path and runtime configuration used across the pipeline. Define the following constants:

- `PROJECT_ROOT: Path` — absolute path to the `kgs/` project root, resolved relative to this file's location.
- `RAW_DATA_DIR: Path` — `PROJECT_ROOT / "data" / "raw"`
- `INTERIM_DATA_DIR: Path` — `PROJECT_ROOT / "data" / "interim"`
- `PROCESSED_DATA_DIR: Path` — `PROJECT_ROOT / "data" / "processed"`
- `EXTERNAL_DATA_DIR: Path` — `PROJECT_ROOT / "data" / "external"`
- `LEASE_INDEX_FILE: Path` — `EXTERNAL_DATA_DIR / "oil_leases_2020_present.txt"`
- `KGS_BASE_URL: str` — `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="`
- `KGS_MONTH_SAVE_URL: str` — `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc="`
- `SCRAPE_CONCURRENCY: int` — `5`
- `SCRAPE_TIMEOUT_MS: int` — `30000` (30 seconds, used as Playwright navigation timeout)
- `OIL_UNIT: str` — `"BBL"`
- `GAS_UNIT: str` — `"MCF"`
- `WATER_UNIT: str` — `"BBL"`
- `MAX_REALISTIC_OIL_BBL_PER_MONTH: float` — `50000.0`

All `Path` constants must call `.resolve()` to produce absolute paths. No secrets or credentials may appear in this file.

**Definition of done:** `config.py` exists, all constants are defined with correct types, all downstream modules import from `config.py` rather than hardcoding paths, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement `load_lease_urls()`

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `load_lease_urls(lease_index_path: Path) -> list[dict]`

**Description:**  
Read the lease index file (`oil_leases_2020_present.txt`) and return a deduplicated list of dicts, one per unique lease, each containing:
- `lease_kid: str` — the unique lease identifier (from column `LEASE_KID`)
- `url: str` — the lease main page URL (from column `URL`)

Steps:
1. Read the file using `pandas.read_csv()` (the file is comma-separated with quoted fields).
2. Select and deduplicate on `LEASE_KID` — only the first occurrence per `LEASE_KID` is needed since the URL is the same for all rows of the same lease.
3. Drop rows where `URL` is null or empty.
4. Return the resulting list of dicts with keys `lease_kid` and `url`.

**Error handling:**
- If `lease_index_path` does not exist, raise `FileNotFoundError` with a descriptive message.
- If the file is missing the `LEASE_KID` or `URL` columns, raise `KeyError` with a descriptive message.

**Test cases:**
- `@pytest.mark.unit` — Given a small in-memory CSV string with 3 rows for 2 unique `LEASE_KID` values, assert the returned list has length 2 (deduplication works).
- `@pytest.mark.unit` — Given a CSV where one row has a null `URL`, assert that row is excluded from the result.
- `@pytest.mark.unit` — Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a CSV missing the `URL` column, assert `KeyError` is raised.
- `@pytest.mark.integration` — Given the real `kgs/data/external/oil_leases_2020_present.txt`, assert the result is a non-empty list and every dict has non-null `lease_kid` and `url` keys.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement `scrape_lease_page()`

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `async def scrape_lease_page(lease_kid: str, url: str, output_dir: Path, semaphore: asyncio.Semaphore, browser: playwright.async_api.Browser) -> Path | None`

**Description:**  
Async function that, for one lease, navigates to its KGS main lease page, clicks "Save Monthly Data to File", lands on the MonthSave page, finds the `.txt` download link, downloads the file, and saves it to `output_dir`.

Detailed steps:
1. Acquire `semaphore` before opening a new browser page (`async with semaphore`).
2. Open a new browser page (`browser.new_page()`).
3. Navigate to `url` using `page.goto(url, timeout=SCRAPE_TIMEOUT_MS)`.
4. Locate the "Save Monthly Data to File" button/link on the page. If not found, raise `ScrapingError(f"No 'Save Monthly Data to File' button found for lease {lease_kid}")`.
5. Click the element; this navigates to the MonthSave page.
6. On the MonthSave page, locate an anchor (`<a>`) element whose `href` ends with `.txt`. If not found, log a `WARNING` via the standard `logging` module and return `None`.
7. Use Playwright's `page.expect_download()` context manager to trigger the file download by clicking the link.
8. Save the downloaded file to `output_dir / <filename>` using `download.save_as(...)`.
9. Close the page (`page.close()`) in a `finally` block to ensure cleanup even on errors.
10. If a file with the resolved output path already exists on disk, skip all navigation steps and return the existing path immediately (idempotent re-run support).
11. Return the `Path` to the saved file on success, or `None` if the download link was missing.

**Error handling:**
- Wrap `page.goto()` in a try/except for `playwright.async_api.TimeoutError`; on timeout log an error and return `None`.
- Wrap the entire function body (outside of the semaphore acquire) in a broad `except Exception` that logs the exception and returns `None`, so one failed lease never crashes the batch run.

**Test cases:**
- `@pytest.mark.unit` — Mock a Playwright `Browser` and `Page`; given a page where "Save Monthly Data to File" is present and the `.txt` link exists, assert the function returns a `Path` ending in `.txt`.
- `@pytest.mark.unit` — Mock a page with no "Save Monthly Data to File" element; assert `ScrapingError` is raised (before the broad except, so it propagates within the unit mock scope — test that the error is raised by the inner logic before the outer handler catches it, i.e. mock the outer except away for this test).
- `@pytest.mark.unit` — Mock a page where the MonthSave page has no `.txt` link; assert the function returns `None` and a WARNING is logged.
- `@pytest.mark.unit` — Given an `output_dir` that already contains a file with the expected name, assert the function returns the existing path without calling `page.goto()`.
- `@pytest.mark.integration` — Given the real lease URL for lease `1001135839`, assert the function downloads a file to a temporary directory and returns a valid `Path` to a `.txt` file.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement `run_acquire_pipeline()`

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `def run_acquire_pipeline(lease_index_path: Path = LEASE_INDEX_FILE, output_dir: Path = RAW_DATA_DIR) -> list[Path]`

**Description:**  
Synchronous orchestrator that drives the full async scraping workflow using Dask for distributed task scheduling.

Steps:
1. Call `load_lease_urls(lease_index_path)` to get the list of lease dicts.
2. Create `output_dir` if it does not exist (`output_dir.mkdir(parents=True, exist_ok=True)`).
3. Use `dask.delayed` to wrap each individual lease's scraping work. For each `lease_dict` in the list, create a `dask.delayed` call to a thin wrapper function `_scrape_one_lease(lease_kid, url, output_dir)` that internally runs the async `scrape_lease_page()` via `asyncio.run()` with its own `async with async_playwright()` browser context and a shared `asyncio.Semaphore(SCRAPE_CONCURRENCY)`.
4. Collect all delayed objects into a list and call `.compute()` exactly once using `dask.compute(*delayed_tasks)`.
5. Filter out `None` results from the computed list.
6. Log a summary: total leases attempted, successful downloads, skipped (already existed), failed (None).
7. Return the list of successfully downloaded `Path` objects.

**Note on Semaphore and Playwright scope:** Because `dask.delayed` workers may run in separate threads or processes, each `_scrape_one_lease` call must own its own `async with async_playwright() as pw` context and browser instance. The `asyncio.Semaphore` is therefore per-worker, not shared globally across workers. The concurrency limit of 5 is enforced within a single async batch; for Dask thread-based scheduling, set `scheduler="synchronous"` or `scheduler="threads"` with `num_workers=SCRAPE_CONCURRENCY` to limit total concurrent workers.

**Error handling:**
- If `load_lease_urls` raises, let the exception propagate (do not swallow it at orchestrator level).
- Each individual `_scrape_one_lease` must never raise — all exceptions are caught and logged internally, returning `None`.

**Test cases:**
- `@pytest.mark.unit` — Mock `load_lease_urls` to return 3 lease dicts and mock `_scrape_one_lease` to return a dummy `Path`; assert `run_acquire_pipeline()` returns a list of 3 `Path` objects and `.compute()` is called exactly once.
- `@pytest.mark.unit` — Mock `_scrape_one_lease` to return `None` for all leases; assert the returned list is empty (all `None` filtered).
- `@pytest.mark.unit` — Assert that the return type is `list` and each element is an instance of `Path`.
- `@pytest.mark.integration` — Given the real lease index and a temporary output directory, run the pipeline for a subset of 2 leases (mock the rest) and assert `.txt` files are written to the output directory.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.
