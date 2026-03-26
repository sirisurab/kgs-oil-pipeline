# Acquire Component — Task Specifications

## Overview

The acquire component is responsible for downloading raw KGS oil and gas production data files
for the years 2020–2025 from the KGS public data portal. It uses Playwright (async API) with an
`asyncio.Semaphore(5)` rate-limiter to scrape per-lease monthly data files from the KGS web
portal, driven by a lease index file. All downloads land in `data/raw/`. A synchronous
orchestrator entry-point drives the full async scraping workflow.

**Source module:** `kgs_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`
**Config:** `kgs_pipeline/config.py`

---

## Design Decisions & Constraints

- Python 3.11+, Playwright async API (`playwright.async_api`), `asyncio.Semaphore(5)` for
  concurrency control (maximum 5 concurrent browser page contexts at any time).
- The lease index file lives at `data/external/oil_leases_2020_present.txt`.
- A lease URL follows the pattern:
  `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`
- The "Save Monthly Data to File" button leads to:
  `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<LEASE_KID>`
- The data file link on the MonthSave page follows the pattern `lp<digits>.txt`.
- Downloaded files are saved as `data/raw/lp<LEASE_KID>.txt`.
- The function is idempotent: if the output file already exists, skip the download and log
  an INFO message.
- A custom `ScrapingError` exception is raised for unrecoverable scraping failures.
- Retry logic: up to 3 attempts per lease with exponential back-off (1 s, 2 s, 4 s).
- Logging via Python's standard `logging` module at DEBUG/INFO/WARNING/ERROR levels.
- All raw file column definitions come from `references/kgs_monthly_data_dictionary.csv`.
- `data/raw/` directory is created if it does not exist.
- The orchestrator collects all failures and writes a `data/raw/failed_leases.csv` report
  after all scraping is complete.

---

## Task 01: Define configuration and custom exceptions

**Module:** `kgs_pipeline/config.py` and `kgs_pipeline/acquire.py`
**Symbols:** `RAW_DIR`, `INTERIM_DIR`, `PROCESSED_DIR`, `EXTERNAL_DIR`, `LEASE_INDEX_FILE`,
             `MAX_CONCURRENT_REQUESTS`, `RETRY_ATTEMPTS`, `RETRY_BACKOFF_BASE`,
             `ScrapingError`

**Description:**
- In `kgs_pipeline/config.py`, define all path constants as `pathlib.Path` objects rooted at
  the project root (one level above `kgs_pipeline/`).
  - `RAW_DIR = Path(...) / "data" / "raw"`
  - `INTERIM_DIR = Path(...) / "data" / "interim"`
  - `PROCESSED_DIR = Path(...) / "data" / "processed"`
  - `EXTERNAL_DIR = Path(...) / "data" / "external"`
  - `FEATURES_DIR = PROCESSED_DIR / "features"`
  - `LEASE_INDEX_FILE = EXTERNAL_DIR / "oil_leases_2020_present.txt"`
- Also define scraping control constants:
  - `MAX_CONCURRENT_REQUESTS: int = 5`
  - `RETRY_ATTEMPTS: int = 3`
  - `RETRY_BACKOFF_BASE: float = 1.0`
- In `kgs_pipeline/acquire.py`, define `ScrapingError(Exception)` with a docstring explaining
  it is raised when a lease page cannot be scraped after all retries.
- All directories must be created via `mkdir(parents=True, exist_ok=True)` at import time of
  `config.py`.

**Error handling:** None — config and exception class definitions have no runtime logic.

**Dependencies:** `pathlib`, standard library only for this task.

**Test cases:**
- `@pytest.mark.unit` — Assert `RAW_DIR`, `INTERIM_DIR`, `PROCESSED_DIR`, `EXTERNAL_DIR`, and
  `FEATURES_DIR` are all `pathlib.Path` instances.
- `@pytest.mark.unit` — Assert `MAX_CONCURRENT_REQUESTS == 5`.
- `@pytest.mark.unit` — Assert `ScrapingError` is a subclass of `Exception`.
- `@pytest.mark.unit` — Assert all configured directories are created on disk when `config.py`
  is imported (use `tmp_path` and monkeypatch to redirect paths so tests don't write to the
  real filesystem).

**Definition of done:** All symbols defined, all unit tests pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement lease index loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_urls(index_file: Path) -> list[str]`

**Description:**
- Read the lease index file at `index_file` using `pandas.read_csv()`. The file uses the
  column schema from `references/kgs_archives_data_dictionary.csv`.
- Extract the `URL` column, drop nulls and duplicates.
- Return a plain Python `list[str]` of unique non-empty URL strings.
- Log the count of URLs loaded at INFO level.

**Error handling:**
- If `index_file` does not exist, raise `FileNotFoundError` with a descriptive message.
- If the `URL` column is absent from the file, raise `KeyError` with a descriptive message.

**Dependencies:** `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a valid CSV with 5 rows and a `URL` column, assert the function
  returns a list of 5 strings.
- `@pytest.mark.unit` — Given a CSV with duplicate URLs, assert the returned list contains
  only unique URLs.
- `@pytest.mark.unit` — Given a CSV with some null values in `URL`, assert those rows are
  excluded from the result.
- `@pytest.mark.unit` — Given a file path that does not exist, assert `FileNotFoundError` is
  raised.
- `@pytest.mark.unit` — Given a CSV without a `URL` column, assert `KeyError` is raised.
- `@pytest.mark.integration` — Given the real `LEASE_INDEX_FILE`, assert the returned list is
  non-empty and all elements start with `"https://"`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement per-lease page scraper

**Module:** `kgs_pipeline/acquire.py`
**Function:**
`scrape_lease_page(lease_url: str, output_dir: Path, semaphore: asyncio.Semaphore, browser: playwright.async_api.Browser) -> Path | None`

**Description:**
- This is an `async` function.
- Accepts the lease URL, the output directory, a shared `asyncio.Semaphore`, and a shared
  Playwright `Browser` instance.
- Acquires the semaphore before opening a new browser page context; releases it after the
  page context is closed.
- Step 1 — Navigate to `lease_url`. Confirm the page loaded by checking the title or a known
  element. If not found, raise `ScrapingError`.
- Step 2 — Find and click the "Save Monthly Data to File" button. Wait for navigation to the
  MonthSave URL. If the button is not found, raise `ScrapingError`.
- Step 3 — On the MonthSave page, find an anchor element whose `href` matches the pattern
  `lp\d+\.txt`. Extract the full download URL.
- Step 4 — Download the file to `output_dir / <filename>` using Playwright's download
  mechanism or a direct HTTP GET via `httpx`.
- Step 5 — If the output file already exists on disk (idempotency check), skip steps 1–4
  and return the existing `Path` immediately, logging an INFO message.
- Return the `Path` of the downloaded file on success, or `None` if the file link is not
  found (log a WARNING in this case).
- Implements exponential back-off retry: on `ScrapingError` or network exception, wait
  `RETRY_BACKOFF_BASE * (2 ** attempt)` seconds and retry up to `RETRY_ATTEMPTS` times.
  After all retries are exhausted, log an ERROR and return `None`.

**Error handling:**
- `ScrapingError` on missing button or missing page elements.
- Network/timeout exceptions caught and retried.
- All exceptions after final retry are caught, logged at ERROR level, and `None` is returned
  (the orchestrator collects failures separately).

**Dependencies:** `playwright.async_api`, `asyncio`, `httpx`, `pathlib`, `logging`, `re`

**Test cases:**
- `@pytest.mark.unit` — Mock the Playwright browser and page objects; given a page that has
  a "Save Monthly Data to File" button and a valid `lp*.txt` link, assert the function returns
  a `Path` ending in `.txt`.
- `@pytest.mark.unit` — Mock the page so the "Save Monthly Data to File" button is absent;
  assert `ScrapingError` is raised on the first attempt (and ultimately `None` returned after
  retries).
- `@pytest.mark.unit` — Mock the page so the `lp*.txt` link is absent; assert the function
  returns `None` and logs a WARNING.
- `@pytest.mark.unit` — Given an output file that already exists on disk (use `tmp_path`),
  assert the function returns the existing `Path` without opening the browser.
- `@pytest.mark.unit` — Verify that the semaphore is released even when a `ScrapingError` is
  raised (inspect semaphore count before and after).
- `@pytest.mark.integration` — Given a real lease URL for lease `1001135839`, assert a `.txt`
  file is downloaded to a temp directory and is non-empty.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement async scraping orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_scraping_async(lease_urls: list[str], output_dir: Path) -> list[Path]`

**Description:**
- This is an `async` function that launches and manages a single Playwright browser instance
  for the lifetime of the scraping run.
- Creates one shared `asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)`.
- Creates one `asyncio.Task` per lease URL by calling `scrape_lease_page(...)` for each URL.
- Gathers all tasks with `asyncio.gather(*tasks, return_exceptions=True)`.
- Collects successful `Path` results and failed URLs (those returning `None` or an exception).
- Writes a failure report to `output_dir / "failed_leases.csv"` with columns
  `[url, reason]` if any failures occurred.
- Returns the list of successfully downloaded `Path` objects.
- Logs a summary at INFO level: total attempted, total succeeded, total failed.

**Error handling:**
- Individual task failures are collected without aborting the run.
- Browser launch failure raises immediately and is not caught (caller handles).

**Dependencies:** `playwright.async_api`, `asyncio`, `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a list of 3 mock lease URLs where all scraping succeeds, assert
  the returned list has 3 `Path` objects.
- `@pytest.mark.unit` — Given a list of 3 mock lease URLs where 1 scraping returns `None`,
  assert the returned list has 2 `Path` objects and `failed_leases.csv` is written with 1 row.
- `@pytest.mark.unit` — Assert that no more than `MAX_CONCURRENT_REQUESTS` browser page
  contexts are open simultaneously (inspect semaphore value during execution with a mock).
- `@pytest.mark.unit` — Assert the return type is `list` and all elements are `pathlib.Path`
  instances.
- `@pytest.mark.integration` — Given 2 real lease URLs, assert that 2 `.txt` files are
  downloaded to a temp directory and both are non-empty.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement synchronous pipeline entry-point

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire_pipeline(index_file: Path | None = None, output_dir: Path | None = None) -> list[Path]`

**Description:**
- Synchronous entry-point that wires together `load_lease_urls()` and `run_scraping_async()`.
- Resolves default values from `config.py` if `index_file` or `output_dir` are `None`.
- Ensures `output_dir` exists before starting.
- Calls `asyncio.run(run_scraping_async(...))` to execute the async scraping workflow.
- Returns the list of `Path` objects returned by `run_scraping_async`.
- Logs start and completion times at INFO level (total wall-clock seconds elapsed).

**Error handling:**
- Re-raises `FileNotFoundError` from `load_lease_urls()` with an additional context message.
- Any unhandled exception from `run_scraping_async` is logged at CRITICAL level before
  re-raising.

**Dependencies:** `asyncio`, `pathlib`, `logging`, `kgs_pipeline.config`, `kgs_pipeline.acquire`

**Test cases:**
- `@pytest.mark.unit` — Patch `load_lease_urls` to return a list of 2 URLs and patch
  `run_scraping_async` to return 2 `Path` objects; assert `run_acquire_pipeline` returns those
  2 paths.
- `@pytest.mark.unit` — Patch `load_lease_urls` to raise `FileNotFoundError`; assert that
  `run_acquire_pipeline` re-raises `FileNotFoundError`.
- `@pytest.mark.unit` — Assert the function accepts `None` for both arguments and resolves
  them from config defaults without raising.
- `@pytest.mark.integration` — Run `run_acquire_pipeline()` end-to-end with the real lease
  index; assert at least one `.txt` file appears in `data/raw/` after the call.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
