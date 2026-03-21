# Acquire Component — Task Specifications
**Module:** `kgs_pipeline/acquire.py`  
**Test file:** `tests/test_acquire.py`  
**Output directory:** `kgs/data/raw/`

---

## Design Decisions & Constraints

- All web scraping uses **Playwright's async API** (`playwright.async_api`).
- Concurrency is capped at **5 simultaneous lease requests** via `asyncio.Semaphore(5)`.
- Dask is used to fan out and orchestrate scrape tasks across the full URL list; the actual HTTP work is async/playwright inside each Dask task.
- Raw downloaded `.txt` files are written to `kgs/data/raw/` with the filename derived from the download link label on the KGS MonthSave page (e.g. `lp564.txt`). If a filename cannot be determined, fall back to `lease_{lease_id}.txt`.
- A lease that has already been downloaded (file exists in `kgs/data/raw/`) is skipped — **idempotent download**.
- All errors during scraping are caught, logged with the lease URL, and do not abort the full run. Failed leases are collected and returned as a summary list.
- No credentials or API keys are needed; all URLs are public.
- A `ScrapeError` custom exception class is defined in this module and raised for unrecoverable per-lease failures.
- The archives file path and raw output directory are read from `kgs_pipeline/config.py`.

---

## Task 01: Create project scaffolding and `.gitignore`

**Module:** `kgs/.gitignore`, `kgs_pipeline/__init__.py`, `kgs_pipeline/config.py`  
**Function:** N/A (configuration and project setup)

**Description:**  
Create the foundational project files required before any pipeline code can run.

1. Create `kgs/.gitignore` with the following exclusions:
   - `.env` and any `*.env` files
   - `__pycache__/` and `*.pyc`
   - `data/raw/`
   - `data/interim/`
   - `data/processed/`
   - `data/external/`
   - Any files matching `*credentials*`, `*api_key*`, `*secret*`
   - Playwright browser cache: `.playwright/` and `playwright-browsers/`
   - Dask worker scratch space: `dask-worker-space/`
   - pytest cache: `.pytest_cache/`
   - `.mypy_cache/`
   - `.ruff_cache/`
   - `*.parquet` at the root level (not under data/)

2. Create `kgs_pipeline/__init__.py` as an empty file (makes the directory a Python package).

3. Create `kgs_pipeline/config.py` with the following module-level constants:
   - `PROJECT_ROOT: Path` — resolved absolute path to the `kgs/` directory, derived from this file's location using `pathlib`.
   - `RAW_DATA_DIR: Path = PROJECT_ROOT / "data" / "raw"` — directory for downloaded raw lease files.
   - `INTERIM_DATA_DIR: Path = PROJECT_ROOT / "data" / "interim"` — directory for intermediate Parquet data.
   - `PROCESSED_DATA_DIR: Path = PROJECT_ROOT / "data" / "processed"` — directory for final processed Parquet data.
   - `FEATURES_DATA_DIR: Path = PROJECT_ROOT / "data" / "processed" / "features"` — directory for feature Parquet data.
   - `EXTERNAL_DATA_DIR: Path = PROJECT_ROOT / "data" / "external"` — directory for source data.
   - `REFERENCES_DIR: Path = PROJECT_ROOT / "references"` — directory for data dictionaries.
   - `OIL_LEASES_FILE: Path = EXTERNAL_DATA_DIR / "oil_leases_2020_present.txt"` — path to the archives file.
   - `MAX_CONCURRENT_SCRAPES: int = 5` — semaphore limit for async playwright.
   - `RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)` — ensure output directories exist on import (also call `.mkdir` for INTERIM, PROCESSED, FEATURES directories).

**Error handling:** `config.py` must not raise errors on import. All `mkdir` calls use `exist_ok=True`.

**Dependencies:** `pathlib` (stdlib)

**Test cases:**
- `@pytest.mark.unit` — Assert that `PROJECT_ROOT` resolves to an existing directory on the filesystem.
- `@pytest.mark.unit` — Assert that all `Path` constants defined in `config.py` are instances of `pathlib.Path`.
- `@pytest.mark.unit` — Assert that importing `config` twice does not raise any exception (idempotent import).

**Definition of done:** Files are created, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement lease URL extractor

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `extract_lease_urls(leases_file: Path) -> pd.DataFrame`

**Description:**  
Read the archives file (`oil_leases_2020_present.txt`) and return a deduplicated DataFrame of unique lease identifiers and their URLs.

- Read the file using `pandas.read_csv` with `dtype=str` to prevent any numeric coercion of lease IDs.
- The file is comma-separated with quoted fields; use appropriate `quotechar` and `sep` parameters.
- Select only the columns `LEASE_KID` and `URL`.
- Drop rows where `URL` is null or empty.
- Deduplicate on `LEASE_KID` (keep first occurrence).
- Return a `pd.DataFrame` with columns `["lease_kid", "url"]` (lowercased column names).
- Log the total number of unique leases found.

**Error handling:**
- If the leases file does not exist, raise `FileNotFoundError` with a descriptive message including the file path.
- If the `URL` column is missing from the file, raise `KeyError` with a descriptive message.

**Dependencies:** `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small in-memory CSV string (3 rows, 2 unique lease IDs, 1 with a null URL), assert the returned DataFrame has exactly 2 rows and columns `["lease_kid", "url"]`.
- `@pytest.mark.unit` — Given a CSV with duplicate `LEASE_KID` values, assert deduplication produces one row per unique lease.
- `@pytest.mark.unit` — Given a file path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a CSV missing the `URL` column, assert `KeyError` is raised.
- `@pytest.mark.integration` — Given the real `OIL_LEASES_FILE`, assert the returned DataFrame has more than 0 rows and that all values in the `url` column start with `"https://"`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement single-lease async page scraper

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `scrape_lease_page(lease_url: str, output_dir: Path, semaphore: asyncio.Semaphore, playwright_instance) -> Path | None`

**Description:**  
Asynchronously scrape one KGS lease page and download the monthly data file for that lease. This function is the core unit of async work; it must be called from an async context.

Step-by-step behaviour:
1. Acquire the `semaphore` before opening a browser page (enforces max 5 concurrent requests).
2. Using the provided `playwright_instance`, open a new browser page and navigate to `lease_url` (e.g. `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839`).
3. On the lease page, find and click the button or link whose visible text contains `"Save Monthly Data to File"`. Wait for navigation to complete after the click.
4. On the resulting MonthSave page (e.g. `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=1001135839`), find the download link for the data file. The link text will resemble `"lp564.txt"` — a short alphanumeric filename ending in `.txt`. Extract the `href` attribute of this link.
5. Construct the full download URL if the `href` is relative (prepend the KGS base URL `https://chasm.kgs.ku.edu`).
6. Download the file content using an HTTP GET request (using `playwright`'s `page.goto` or `APIRequestContext`) and write the raw bytes to `output_dir / filename`.
7. Return the `Path` to the written file.
8. Release the semaphore on exit (use `async with semaphore`).

- Extract the `lease_kid` from the URL query parameter `f_lc` for use in fallback filenames and logging.
- If the output file already exists in `output_dir`, skip downloading and return the existing `Path` immediately (idempotency).
- Log each step (navigating, clicking, downloading) at DEBUG level; log success at INFO level with lease ID and filename.

**Error handling:**
- If the `"Save Monthly Data to File"` button/link is not found on the page, raise `ScrapeError` with the lease URL.
- If the download link (`.txt` file link) is not found on the MonthSave page, log a WARNING and return `None` (do not raise — some leases may have no downloadable data file).
- If a network or playwright error occurs, catch it, log it as ERROR with the lease URL, and return `None`.

**Custom exception:**  
Define `class ScrapeError(Exception): pass` at module level in `acquire.py`.

**Dependencies:** `playwright.async_api`, `asyncio`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Using `unittest.mock` / `AsyncMock`, mock a playwright page that successfully navigates, finds the button, and returns a download link. Assert the function returns a `Path` ending in `.txt`.
- `@pytest.mark.unit` — Mock a playwright page where the `"Save Monthly Data to File"` button is absent. Assert `ScrapeError` is raised.
- `@pytest.mark.unit` — Mock a playwright page where the MonthSave page has no `.txt` download link. Assert the function returns `None` and logs a warning.
- `@pytest.mark.unit` — Mock a playwright page where the output file already exists in `output_dir`. Assert the function returns the existing `Path` without calling `page.goto` a second time (download is skipped).
- `@pytest.mark.integration` — Given the real URL for lease `1001135839`, assert the function downloads a `.txt` file to a temporary directory and returns a valid `Path`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement parallel scrape orchestrator

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `run_scrape_pipeline(leases_file: Path, output_dir: Path) -> dict`

**Description:**  
Orchestrate the full parallel scraping run for all leases in the archives file. This is the top-level entry point for the acquire component.

- Call `extract_lease_urls(leases_file)` to get the full list of `(lease_kid, url)` pairs.
- Use **Dask** to partition the URL list into chunks. Submit each chunk as a Dask delayed task that internally runs an `asyncio` event loop (`asyncio.run(...)`) for the async playwright scraping of all URLs in that chunk.
- Within each Dask task (chunk), use `asyncio.gather` to run `scrape_lease_page` for all URLs in the chunk concurrently, sharing a single `asyncio.Semaphore(MAX_CONCURRENT_SCRAPES)` and a single `async_playwright` browser instance across the chunk.
- Use `dask.compute` to execute all chunk tasks in parallel (use the `threads` or `processes` scheduler as appropriate — **threads** is preferred since the work is I/O-bound playwright async).
- Collect results: a list of successfully downloaded file `Path` objects and a list of failed lease URLs (those that returned `None` or raised).
- Return a summary `dict` with keys:
  - `"downloaded"`: `list[Path]` — paths of successfully downloaded files.
  - `"failed"`: `list[str]` — lease URLs that failed.
  - `"skipped"`: `list[Path]` — files that already existed and were skipped.
  - `"total_leases"`: `int` — total leases attempted.
- Log a final summary at INFO level.

**Error handling:**
- Any unhandled exception from a Dask chunk task is caught, logged, and does not abort the remaining chunks.
- If `leases_file` does not exist, propagate `FileNotFoundError` immediately (do not start scraping).

**Dependencies:** `dask`, `dask.delayed`, `asyncio`, `playwright.async_api`, `logging`, `pathlib`

**Test cases:**
- `@pytest.mark.unit` — Mock `extract_lease_urls` to return a 4-row DataFrame and mock `scrape_lease_page` to always return a `Path`. Assert `run_scrape_pipeline` returns a dict with `"downloaded"` having 4 items and `"failed"` being empty.
- `@pytest.mark.unit` — Mock `scrape_lease_page` to return `None` for 2 of 4 leases. Assert the returned dict has `"failed"` with 2 items.
- `@pytest.mark.unit` — Assert that the return type of each Dask delayed task (before `.compute()`) is a `dask.delayed.Delayed` object, confirming lazy evaluation.
- `@pytest.mark.integration` — Given a small real subset of 3 lease URLs (hardcoded), assert the function downloads `.txt` files to a temp directory and the returned `"downloaded"` list has length 3.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
