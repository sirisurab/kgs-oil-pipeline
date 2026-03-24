# Acquire Component — Task Specifications

## Overview

The acquire component is responsible for:
1. Reading the lease index file (`data/external/oil_leases_2020_present.txt`) to extract all unique lease URLs.
2. Using Playwright (async API) with a rate-limiting `asyncio.Semaphore(5)` to scrape each lease's monthly data `.txt` file from the KGS web portal.
3. Saving each downloaded `.txt` file to `data/raw/`.
4. Providing a synchronous orchestrator entry-point (`run_acquire_pipeline()`) that drives the async scraping workflow, using Dask delayed for parallel task scheduling.

**Source module:** `kgs_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

---

## Design Decisions & Constraints

- Python 3.11+. Playwright async API (`playwright.async_api`). `asyncio.Semaphore(5)` enforces a maximum of 5 concurrent browser page contexts at any time to avoid overloading the KGS server.
- All downloaded raw files land in `data/raw/`. The directory must be created if it does not exist.
- A lease URL follows the pattern `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`.
- The "Save Monthly Data to File" button on the lease page navigates to `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<LEASE_KID>`.
- On the MonthSave page, a link labelled like `lp<N>.txt` is the download target. Clicking it triggers a browser file download.
- If a lease page has no "Save Monthly Data to File" button, raise `ScrapingError`.
- If the MonthSave page has no `.txt` download link, log a warning and return `None` for that lease (do not abort the run).
- Downloaded files are named exactly as the server provides (e.g. `lp564.txt`). If a file with that name already exists in `data/raw/`, skip re-downloading (idempotent re-runs).
- A custom exception class `ScrapingError(Exception)` must be defined in `acquire.py`.
- All configuration values (raw data directory, concurrency limit, base URLs, lease index path) must be read from `kgs_pipeline/config.py` — never hardcoded in `acquire.py`.
- Dask delayed is used to build a task graph over lease URL batches. `.compute()` is called exactly once in `run_acquire_pipeline()`.
- All logging must use the Python `logging` module. No `print()` statements.

---

## Task 01: Define configuration constants

**Module:** `kgs_pipeline/config.py`
**Function:** N/A — module-level constants

**Description:**
Create `kgs_pipeline/config.py` as the single source of configuration for the entire pipeline. The module must define the following constants used by the acquire component (and later extended by other components):

- `RAW_DATA_DIR` — `Path` pointing to `data/raw/`
- `INTERIM_DATA_DIR` — `Path` pointing to `data/interim/`
- `PROCESSED_DATA_DIR` — `Path` pointing to `data/processed/`
- `FEATURES_DATA_DIR` — `Path` pointing to `data/processed/features/`
- `EXTERNAL_DATA_DIR` — `Path` pointing to `data/external/`
- `LEASE_INDEX_FILE` — `Path` pointing to `references/oil_leases_2020_present.txt`
- `KGS_BASE_URL` — base URL string `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease"`
- `KGS_MONTH_SAVE_URL` — base URL string `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"`
- `MAX_CONCURRENT_REQUESTS` — integer `5`
- `DASK_N_WORKERS` — integer controlling the number of Dask workers (default `4`)
- `LOG_LEVEL` — string `"INFO"`

All `Path` constants must be constructed relative to the project root, resolved using `Path(__file__).parent.parent` so they work regardless of the working directory from which the pipeline is invoked.

**Error handling:** None — this is a pure constants module.

**Dependencies:** `pathlib.Path`

**Test cases:**

- `@pytest.mark.unit` — Assert that `RAW_DATA_DIR`, `INTERIM_DATA_DIR`, `PROCESSED_DATA_DIR`, `FEATURES_DATA_DIR`, and `EXTERNAL_DATA_DIR` are all instances of `pathlib.Path`.
- `@pytest.mark.unit` — Assert that `MAX_CONCURRENT_REQUESTS` equals `5`.
- `@pytest.mark.unit` — Assert that `KGS_BASE_URL` starts with `"https://"`.
- `@pytest.mark.unit` — Assert that `LEASE_INDEX_FILE` ends with `".txt"`.

**Definition of done:** Module is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Define custom exception class

**Module:** `kgs_pipeline/acquire.py`
**Class:** `ScrapingError(Exception)`

**Description:**
Define a custom exception class `ScrapingError` that inherits from `Exception`. This class is raised when the Playwright scraper encounters a fatal, unrecoverable condition for a given lease page (e.g., the "Save Monthly Data to File" button is absent from the page). It must accept an optional `lease_id: str` keyword argument and include it in the string representation of the exception to aid debugging.

The class must be importable from `kgs_pipeline.acquire` and re-exported via `kgs_pipeline/__init__.py` for convenience.

**Error handling:** N/A — this is the error class itself.

**Dependencies:** None beyond the Python standard library.

**Test cases:**

- `@pytest.mark.unit` — Instantiate `ScrapingError("message")` and assert `str(error)` contains `"message"`.
- `@pytest.mark.unit` — Instantiate `ScrapingError("message", lease_id="1001135839")` and assert `str(error)` contains `"1001135839"`.
- `@pytest.mark.unit` — Assert `isinstance(ScrapingError("x"), Exception)` is `True`.
- `@pytest.mark.unit` — Assert that `raise ScrapingError("x")` can be caught by an `except Exception` block.

**Definition of done:** Class is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement lease index reader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_urls(index_file: Path) -> list[str]`

**Description:**
Read the lease index file at `index_file` (tab- or comma-separated `.txt` file) into a pandas DataFrame and return a deduplicated list of all non-null URL strings found in the `URL` column. The file is the KGS lease archive file (`oil_leases_2020_present.txt`) whose schema is defined in `references/kgs_archives_data_dictionary.csv`. The `URL` column contains URLs of the form `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`.

Processing steps:
1. Read the file using `pandas.read_csv()`. Try comma as delimiter first; fall back to tab delimiter if the resulting DataFrame has only one column.
2. Strip whitespace from all column names.
3. Drop rows where `URL` is null or empty string.
4. Deduplicate on `URL`.
5. Return the `URL` column as a plain Python `list[str]`.

Log the total count of URLs loaded at `INFO` level.

**Error handling:**
- If the file does not exist, raise `FileNotFoundError` with a message including the path.
- If the `URL` column is absent after reading, raise `KeyError` with a descriptive message.

**Dependencies:** `pandas`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a small in-memory CSV string (written to a `tmp_path` fixture file) with a `URL` column containing 3 rows (1 duplicate, 1 null), assert the function returns a list of exactly 2 unique URL strings.
- `@pytest.mark.unit` — Given a file missing the `URL` column, assert `KeyError` is raised.
- `@pytest.mark.unit` — Given a non-existent path, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Assert the return type is `list` and all elements are `str`.
- `@pytest.mark.integration` — Call `load_lease_urls(config.LEASE_INDEX_FILE)` against the real file in `references/` and assert the returned list is non-empty and all items start with `"https://"`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement per-lease Playwright scraper

**Module:** `kgs_pipeline/acquire.py`
**Function:** `scrape_lease_page(lease_url: str, output_dir: Path, semaphore: asyncio.Semaphore, browser: playwright.async_api.Browser) -> Path | None`

**Description:**
An `async` function that scrapes a single KGS lease page to download the monthly production `.txt` file. The function must follow this exact sequence:

1. Acquire `semaphore` before opening a new browser page context, to enforce the maximum-5-concurrent-requests constraint.
2. Navigate to `lease_url` (e.g. `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839`).
3. Find the element with text "Save Monthly Data to File" on the page. If absent, release the semaphore and raise `ScrapingError` with the `lease_id` extracted from the URL's `f_lc` query parameter.
4. Set up a Playwright download listener (using `page.expect_download()`) before clicking the button, to capture the file download event reliably.
5. Click the "Save Monthly Data to File" button/link, which navigates to the MonthSave page.
6. On the MonthSave page, locate the `<a>` link whose `href` ends with `.txt` (e.g. `lp564.txt`).
7. If no such link exists, log a `WARNING` with the lease URL and return `None`.
8. Derive the filename from the link's `href` attribute (basename only, e.g. `lp564.txt`).
9. If `output_dir / filename` already exists, log a `DEBUG` message ("skipping — already downloaded") and return the existing path without re-downloading.
10. Click the download link, await the download event, and save the file to `output_dir / filename` using Playwright's `download.save_as()`.
11. Log an `INFO` message with the saved file path.
12. Release the semaphore and close the page context.
13. Return the `Path` to the saved file.

The function must always release the semaphore (use `async with semaphore`).

**Error handling:**
- Any `playwright.async_api.Error` (navigation timeout, page crash) must be caught, logged at `ERROR` level with the lease URL, and re-raised as `ScrapingError`.
- Semaphore must be released even if an exception occurs (guaranteed by `async with`).

**Dependencies:** `playwright.async_api`, `asyncio`, `pathlib.Path`, `logging`, `urllib.parse`

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.AsyncMock` to mock the Playwright `Browser` and `Page` objects, assert that when the page contains a "Save Monthly Data to File" element and a `.txt` link, the function returns a `Path` ending with `.txt`.
- `@pytest.mark.unit` — Mock a page where the "Save Monthly Data to File" element is absent; assert `ScrapingError` is raised.
- `@pytest.mark.unit` — Mock a MonthSave page where no `.txt` link exists; assert the function returns `None` and logs a `WARNING`.
- `@pytest.mark.unit` — Mock a page where the target file already exists in `output_dir` (use `tmp_path`); assert the function returns the existing path without calling `download.save_as()`.
- `@pytest.mark.unit` — Mock a `playwright.async_api.Error` during navigation; assert `ScrapingError` is raised and the semaphore is released.
- `@pytest.mark.integration` — Against the real KGS website, scrape lease `1001135839` and assert a `.txt` file is created in a temporary directory with a non-zero file size.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement the acquire pipeline orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire_pipeline() -> list[Path]`

**Description:**
A synchronous entry-point function that orchestrates the full acquisition workflow end-to-end. It must:

1. Read configuration values from `kgs_pipeline/config.py`: `LEASE_INDEX_FILE`, `RAW_DATA_DIR`, `MAX_CONCURRENT_REQUESTS`, `DASK_N_WORKERS`.
2. Call `load_lease_urls()` to obtain the full list of lease URLs.
3. Ensure `RAW_DATA_DIR` exists (create it with `mkdir(parents=True, exist_ok=True)`).
4. Partition the list of URLs into chunks of size `MAX_CONCURRENT_REQUESTS * 4` (to give Dask meaningful work units).
5. For each chunk, create a `dask.delayed` task that wraps an `async` helper (`_scrape_chunk`) which:
   - Launches a single Playwright `async_with_playwright` session and a single `Chromium` browser instance per chunk.
   - Creates one `asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)` per chunk.
   - Calls `asyncio.gather(*[scrape_lease_page(...) for url in chunk])` to process all URLs in the chunk concurrently under the semaphore.
   - Returns the list of `Path | None` results.
6. Compute all Dask delayed tasks with `dask.compute(*delayed_tasks)`, using a `dask.distributed.LocalCluster` with `DASK_N_WORKERS` workers if available, otherwise falling back to the synchronous scheduler.
7. Flatten the nested results, filter out `None` values, and return the flat list of downloaded `Path` objects.
8. Log a summary at `INFO` level: total URLs attempted, total files downloaded, total skipped.

**Error handling:**
- Individual `ScrapingError` exceptions from `scrape_lease_page()` must be caught inside `_scrape_chunk`, logged at `ERROR` level (with the URL), and counted. They must not abort the entire pipeline run.
- If `load_lease_urls()` raises, let the exception propagate — the pipeline cannot run without the URL list.

**Dependencies:** `dask`, `asyncio`, `playwright.async_api`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Patch `load_lease_urls` to return a list of 3 URLs and patch `scrape_lease_page` to return a dummy `Path`. Assert `run_acquire_pipeline()` returns a list of 3 `Path` objects.
- `@pytest.mark.unit` — Patch `scrape_lease_page` to raise `ScrapingError` for one URL and return a valid `Path` for the others. Assert the function returns the valid paths without raising and that the error is logged.
- `@pytest.mark.unit` — Patch `scrape_lease_page` to return `None` for all URLs. Assert the function returns an empty list.
- `@pytest.mark.unit` — Assert that the return type of `run_acquire_pipeline()` (with all internals mocked) is `list`.
- `@pytest.mark.integration` — Run `run_acquire_pipeline()` against the real KGS site (or a small subset of leases). Assert that `data/raw/` contains at least one `.txt` file after the run.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
