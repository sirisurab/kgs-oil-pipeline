# Acquire Component Tasks

**Module:** `kgs_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

## Overview

The acquire component downloads KGS oil production data files for all leases active from
2024 onward. It reads the lease index file (`data/external/oil_leases_2020_present.txt`),
filters to leases with MONTH-YEAR >= 1-2024, deduplicates by URL, then for each unique
lease URL performs a two-step HTTP download: first fetching the MonthSave page to extract
the file download link, then downloading the actual data file and saving it under
`data/raw/`. All downloads are executed in parallel using Dask with a maximum of 5
concurrent workers and a 0.5-second delay per worker to avoid overloading the KGS server.

## Design Decisions

- Package name: `kgs_pipeline`; all modules live under `kgs_pipeline/`.
- HTTP client: `requests` only — no browser automation (no Playwright, Selenium, etc.).
- HTML parsing: `BeautifulSoup` from the `beautifulsoup4` package.
- Parallelism: Dask delayed with a `synchronous` or `threads` scheduler capped at 5
  workers — do NOT use a `ProcessPoolExecutor` for I/O-bound network work.
- Already-downloaded files are skipped (idempotent) — a file is considered present if
  it exists on disk with size > 0 bytes.
- Logging: structured Python `logging` with JSON-formatted entries; log each lease URL,
  outcome (skipped / downloaded / error), and file size in bytes.
- Configuration: `kgs_pipeline/config.py` must define all tuneable constants (base URLs,
  raw data dir, max workers, sleep seconds, lease index path).
- `pyproject.toml` build-backend must be `setuptools.build_meta`. The `Makefile` must
  include a `make env` target that creates `.venv` using `python3 -m venv .venv` and an
  `install` target that bootstraps `pip`, `setuptools`, and `wheel` before running
  `pip install -e ".[dev]"`.
- `pyproject.toml` dev dependencies must include `pandas-stubs` and `types-requests`.

---

## Task 01: Project scaffold and configuration

**Module:** `kgs_pipeline/config.py`, `pyproject.toml`, `Makefile`, `.gitignore`

**Description:**
Create the full project scaffold so that subsequent tasks have a stable foundation.

1. Create `kgs_pipeline/__init__.py` (empty, sets package version string).
2. Create `kgs_pipeline/config.py` containing a `Config` dataclass (or module-level
   constants) with the following fields — all values must be overridable at runtime via
   environment variables or CLI:
   - `LEASE_INDEX_PATH`: default `data/external/oil_leases_2020_present.txt`
   - `RAW_DATA_DIR`: default `data/raw`
   - `INTERIM_DATA_DIR`: default `data/interim`
   - `PROCESSED_DATA_DIR`: default `data/processed`
   - `FEATURES_DATA_DIR`: default `data/features`
   - `MAX_WORKERS`: default `5` (integer)
   - `SLEEP_SECONDS`: default `0.5` (float)
   - `MIN_YEAR`: default `2024` (integer)
   - `MONTH_SAVE_URL_TEMPLATE`: `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"`
   - `MAIN_LEASE_URL_PREFIX`: `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="`
3. Create `pyproject.toml` with:
   - `build-backend = "setuptools.build_meta"` — never `"setuptools.backends.legacy:build"`.
   - `[project]` section: name = `kgs-pipeline`, python requires `>=3.11`.
   - `[project.optional-dependencies]` `dev` group containing: `pytest`, `pytest-mock`,
     `ruff`, `mypy`, `pandas-stubs`, `types-requests`, `dask[complete]`, `requests`,
     `beautifulsoup4`, `pyarrow`, `lxml`.
   - `[tool.setuptools.packages.find]` pointing to the project root so `kgs_pipeline`
     is discovered.
4. Create `Makefile` with targets:
   - `env`: `python3 -m venv .venv`
   - `install`: activates `.venv`, runs `pip install --upgrade pip setuptools wheel`,
     then `pip install -e ".[dev]"`.
   - `test`: `pytest tests/ -v`
   - `lint`: `ruff check kgs_pipeline/ tests/`
   - `typecheck`: `mypy kgs_pipeline/`
5. Create `.gitignore` containing at minimum: `.venv/`, `__pycache__/`, `*.pyc`,
   `data/`, `large_tool_results/`, `conversation_history/`, `.DS_Store`, `.pytest_cache/`,
   `*.egg-info/`.
6. Create `tests/__init__.py` (empty).
7. Create directory stubs: `data/raw/.gitkeep`, `data/interim/.gitkeep`,
   `data/processed/.gitkeep`, `data/features/.gitkeep`, `data/external/.gitkeep`.

**Dependencies:** setuptools, pip

**Test cases:** None for this task (scaffold only).

**Definition of done:** All files listed above exist, `pip install -e ".[dev]"` completes
without error in a fresh `.venv`, `ruff` and `mypy` report no errors on the empty
`kgs_pipeline/` package. `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 02: Structured JSON logging utility

**Module:** `kgs_pipeline/logging_utils.py`
**Function:** `get_logger(name: str) -> logging.Logger`

**Description:**
Implement a reusable logging factory used by every pipeline module. The logger must
emit structured JSON-formatted log entries to both stdout and a rotating file handler
at `logs/pipeline.log` (create `logs/` directory if absent). Each log record must
include at minimum the fields: `timestamp` (ISO-8601), `level`, `logger`, `message`,
and any extra keyword arguments passed via the `extra=` parameter.

- Use a custom `logging.Formatter` subclass named `JsonFormatter` that overrides
  `format()` to return a JSON-serialised string.
- `get_logger(name)` must be idempotent — calling it twice with the same name must not
  add duplicate handlers.
- Log level defaults to `INFO`; can be overridden via the `LOG_LEVEL` environment
  variable.

**Dependencies:** `logging`, `json`, `os`, `datetime` (all stdlib)

**Test cases:**

- `@pytest.mark.unit` — Given a call to `get_logger("test")`, assert the returned object
  is a `logging.Logger` instance and has at least one handler.
- `@pytest.mark.unit` — Given two calls to `get_logger("test")`, assert the logger has
  the same number of handlers as after the first call (no duplicates).
- `@pytest.mark.unit` — Given a logger obtained from `get_logger("test")`, call
  `logger.info("hello", extra={"lease_id": "123"})` and assert the captured log output
  is valid JSON containing keys `timestamp`, `level`, `message`, and `lease_id`.

**Definition of done:** `JsonFormatter` and `get_logger` implemented, all unit tests pass,
`ruff` and `mypy` report no errors. `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 03: Lease index loader and filter

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str, min_year: int = 2024) -> pd.DataFrame`

**Description:**
Load the lease index file (`data/external/oil_leases_2020_present.txt`) into a pandas
DataFrame and apply filtering and deduplication so that the returned DataFrame contains
exactly one row per unique lease URL active from `min_year` onward.

Steps:
1. Read the file using `pd.read_csv()` with tab or comma separation (auto-detect by
   trying both; the file is a `.txt` that may be pipe-, comma-, or tab-delimited).
   Handle `UnicodeDecodeError` by retrying with `encoding="latin-1"` if `utf-8` fails.
2. Strip whitespace from all column names.
3. Filter rows where the year component of `MONTH-YEAR` >= `min_year`. The `MONTH-YEAR`
   column has format `"M-YYYY"` (e.g. `"1-2024"`). Extract the year by splitting on `"-"`
   and taking the **last** element. Drop rows where the extracted year is not numeric
   (e.g. `"-1-1965"`, `"0-1966"` produce `"1965"` correctly but entries like `"abc"`
   must be silently dropped).
4. Deduplicate by `URL` — keep first occurrence.
5. Drop rows where `URL` is null or empty.
6. Return the filtered, deduplicated DataFrame with at least columns: `LEASE KID`, `URL`,
   `OPERATOR`, `COUNTY`.

**Error handling:**
- Raise `FileNotFoundError` with a descriptive message if `index_path` does not exist.
- Log the count of rows before and after filtering and deduplication.

**Dependencies:** `pandas`, `kgs_pipeline.logging_utils`, `kgs_pipeline.config`

**Test cases:**

- `@pytest.mark.unit` — Given a synthetic in-memory CSV string with 5 rows spanning
  years 2023 and 2024 (3 rows year=2024, 2 rows year=2023), assert the function returns
  a DataFrame with exactly 3 rows.
- `@pytest.mark.unit` — Given a synthetic CSV with duplicate URL values for two 2024
  rows, assert the result contains exactly 1 row for that URL (deduplication).
- `@pytest.mark.unit` — Given a synthetic CSV where one MONTH-YEAR entry is
  `"-1-1965"`, assert the function does not raise an exception and the row is excluded.
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError`
  is raised.
- `@pytest.mark.integration` — Given `data/external/oil_leases_2020_present.txt` on
  disk, assert the function returns a non-empty DataFrame and all URLs in the result
  contain the string `"kgs.ku.edu"` or `"chasm.kgs.ku.edu"`.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report no
errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Lease ID extractor

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:**
Extract the lease ID (the value of the `f_lc` query parameter) from a KGS lease URL.
Example: given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`,
return `"1001135839"`. Use `urllib.parse.urlparse` and `urllib.parse.parse_qs` to parse
the URL. Raise `ValueError` if the `f_lc` parameter is absent or empty.

**Dependencies:** `urllib.parse` (stdlib)

**Test cases:**

- `@pytest.mark.unit` — Given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`,
  assert return value is `"1001135839"`.
- `@pytest.mark.unit` — Given a URL with no `f_lc` parameter, assert `ValueError` is raised.
- `@pytest.mark.unit` — Given a URL with `f_lc=` (empty value), assert `ValueError` is raised.
- `@pytest.mark.unit` — Given a URL with multiple query params including `f_lc=abc123`,
  assert return value is `"abc123"`.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: MonthSave page fetcher and download-link parser

**Module:** `kgs_pipeline/acquire.py`
**Functions:**
- `fetch_month_save_page(lease_id: str, session: requests.Session, max_retries: int = 3, backoff_base: float = 1.0) -> str`
- `parse_download_link(html: str, base_url: str = "https://chasm.kgs.ku.edu") -> str`

**Description:**

`fetch_month_save_page`:
- Constructs the MonthSave URL from config: `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"`.
- Performs an HTTP GET using the provided `requests.Session` with a 30-second timeout.
- Retries up to `max_retries` times on `requests.exceptions.RequestException` using
  exponential backoff: sleep `backoff_base * (2 ** attempt)` seconds between retries.
- Returns the raw HTML response text on success.
- Raises `RuntimeError` after all retries are exhausted.

`parse_download_link`:
- Parses the HTML string using `BeautifulSoup` with the `"lxml"` parser.
- Finds the first `<a>` tag whose `href` attribute contains `"anon_blobber.download"`.
- If the href is a relative URL, prepend `base_url` to form the absolute URL.
- Returns the absolute download URL string.
- Raises `ValueError` if no matching anchor tag is found.

**Dependencies:** `requests`, `beautifulsoup4`, `lxml`, `time`, `kgs_pipeline.config`,
`kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.patch` on `requests.Session.get`, given a
  mock response returning a valid HTML page containing an `<a href=".../anon_blobber.download?p_file_name=lp564.txt">`,
  assert `fetch_month_save_page` returns the HTML string and the session GET is called
  exactly once.
- `@pytest.mark.unit` — Given a mock that raises `requests.exceptions.ConnectionError`
  on every attempt, assert `fetch_month_save_page` raises `RuntimeError` after
  `max_retries` attempts and that the GET method was called exactly `max_retries` times.
- `@pytest.mark.unit` — Given an HTML string with a relative href
  `"/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"`, assert
  `parse_download_link` returns the fully-qualified URL starting with `"https://chasm.kgs.ku.edu"`.
- `@pytest.mark.unit` — Given an HTML string with no `anon_blobber.download` link,
  assert `parse_download_link` raises `ValueError`.
- `@pytest.mark.unit` — Given an HTML string with an absolute href already containing
  the full URL, assert `parse_download_link` returns it unchanged.

**Definition of done:** Both functions implemented, all tests pass, `ruff` and `mypy`
report no errors. `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 06: Data file downloader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_file(download_url: str, output_dir: str, session: requests.Session, sleep_seconds: float = 0.5) -> Path | None`

**Description:**
Download a single KGS lease data file and save it to `output_dir`.

Steps:
1. Extract the filename from the `p_file_name` query parameter of `download_url`
   (e.g. `lp564.txt` from `...?p_file_name=lp564.txt`). Fall back to the last path
   segment of the URL if the parameter is absent.
2. Construct `output_path = Path(output_dir) / filename`.
3. If `output_path` exists and has size > 0 bytes, log a "skipped" message and return
   `output_path` immediately (idempotent behaviour).
4. Sleep `sleep_seconds` seconds to rate-limit the server.
5. Perform an HTTP GET request with a 60-second timeout.
6. Raise `RuntimeError` on HTTP status codes >= 400.
7. Write the response content (bytes) to `output_path`.
8. If the written file has size 0 bytes, delete it and return `None` (log a warning).
9. Return `output_path` on success.

**Error handling:**
- Catch `requests.exceptions.RequestException` and log a warning; return `None`
  (do not raise — a single failed download must not abort the entire pipeline run).

**Dependencies:** `requests`, `pathlib`, `time`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.patch` on `requests.Session.get`, given a
  mock response with `content = b"col1,col2\nval1,val2\n"` and `status_code = 200`,
  assert the function returns a `Path` object pointing to a file inside `output_dir`
  with the correct filename.
- `@pytest.mark.unit` — Given that `output_path` already exists on disk with size > 0,
  assert `session.get` is never called and the existing path is returned (idempotent).
- `@pytest.mark.unit` — Given a mock response with `content = b""` (empty body),
  assert the function returns `None` and no file remains in `output_dir`.
- `@pytest.mark.unit` — Given a mock that raises `requests.exceptions.Timeout`,
  assert the function returns `None` and does not raise an exception.
- `@pytest.mark.unit` — Given a mock response with `status_code = 404`, assert
  `RuntimeError` is raised.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Parallel download orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire(index_path: str, output_dir: str, min_year: int = 2024, max_workers: int = 5) -> list[Path]`

**Description:**
Orchestrate the full acquisition workflow using Dask delayed tasks.

Steps:
1. Call `load_lease_index(index_path, min_year)` to get the filtered lease DataFrame.
2. Create a `requests.Session` with a `Retry` adapter (3 retries, backoff factor 0.5,
   retry on status codes 500, 502, 503, 504).
3. For each row in the filtered DataFrame:
   a. Call `extract_lease_id(row["URL"])` to get the lease ID.
   b. Wrap the two-step download logic (`fetch_month_save_page` → `parse_download_link`
      → `download_file`) inside a `dask.delayed` function named `_download_one_lease`.
4. Build a list of `dask.delayed` objects and compute them using
   `dask.compute(*delayed_list, scheduler="threads", num_workers=max_workers)`.
5. Collect and return the list of successfully downloaded `Path` objects (filter out
   `None` values from the results).
6. Log a final summary: total leases attempted, downloaded, skipped, and failed.

**Error handling:**
- Individual lease failures must be caught inside `_download_one_lease` and return
  `None` — they must not propagate and abort the entire run.

**Dependencies:** `dask`, `requests`, `urllib3`, `kgs_pipeline.logging_utils`,
`kgs_pipeline.config`

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.patch` to mock `_download_one_lease` to
  return a fixed `Path`, given a lease index DataFrame with 3 rows, assert
  `run_acquire` returns a list of 3 `Path` objects.
- `@pytest.mark.unit` — Using mocks, given one lease whose `_download_one_lease`
  returns `None` (simulated failure), assert that `run_acquire` returns a list of
  length 2 (not 3) and does not raise an exception.
- `@pytest.mark.integration` — Given the real `data/external/oil_leases_2020_present.txt`
  on disk and real network access, assert `run_acquire` completes without exception and
  writes at least one `.txt` file to `data/raw/`. Mark with `@pytest.mark.integration`.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Acquire idempotency and file-integrity validation (TR-20, TR-21)

**Module:** `tests/test_acquire.py`

**Description:**
Implement the test cases mandated by test requirements TR-20 (acquire idempotency) and
TR-21 (acquired file integrity) in `tests/test_acquire.py`. These tests exercise the
full acquire output rather than individual functions.

**Test cases:**

- `@pytest.mark.unit` TR-20a — Using `unittest.mock` to simulate two successive calls to
  `run_acquire` that both write the same set of mocked files to a temporary directory,
  assert the file count after the second call equals the file count after the first call
  (no file duplication).
- `@pytest.mark.unit` TR-20b — Given a pre-existing file written to the temp dir,
  assert `download_file` returns the existing path without calling `session.get`, and
  the file content is byte-for-byte identical before and after the call.
- `@pytest.mark.unit` TR-20c — Given a pre-existing file, assert `download_file` does
  not raise any exception.
- `@pytest.mark.integration` TR-21a — Given files in `data/raw/` written by a prior
  acquire run, assert every file has `os.path.getsize(file) > 0`.
- `@pytest.mark.integration` TR-21b — Given files in `data/raw/`, assert every file
  can be opened and read as UTF-8 text without a `UnicodeDecodeError` (fall back to
  `latin-1` if needed, but do not silently swallow encoding errors — log and assert).
- `@pytest.mark.integration` TR-21c — Given files in `data/raw/`, assert every file
  contains at least 2 lines (header + at least one data row).

**Definition of done:** All test cases implemented in `tests/test_acquire.py`, all unit
tests pass without network access, `ruff` and `mypy` report no errors. `requirements.txt`
updated with all third-party packages imported in this task.

---

## Task 09: CLI entry point for acquire stage

**Module:** `kgs_pipeline/acquire.py`
**Entry point:** `if __name__ == "__main__"` block and `kgs_pipeline/cli.py`

**Description:**
Add a CLI entry point so the acquire stage can be run as:

```
python -m kgs_pipeline.acquire --output-dir data/raw --min-year 2024 --workers 5
```

Use `argparse` to define arguments:
- `--index-path`: path to lease index file (default from `config.LEASE_INDEX_PATH`)
- `--output-dir`: where to write raw files (default from `config.RAW_DATA_DIR`)
- `--min-year`: integer, default 2024
- `--workers`: integer, default 5

The entry point calls `run_acquire(...)` with the provided arguments and exits with
code 0 on success, 1 on unhandled error.

Also register this as a console script entry point in `pyproject.toml`:
`kgs-acquire = "kgs_pipeline.acquire:main"` (implement a `main()` function that wraps
the `argparse` block).

**Dependencies:** `argparse` (stdlib)

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.patch` on `run_acquire`, call `main()`
  with `sys.argv` set to `["acquire", "--min-year", "2024", "--workers", "3"]` and
  assert `run_acquire` was called with `min_year=2024` and `max_workers=3`.
- `@pytest.mark.unit` — Given `run_acquire` raises an unhandled exception, assert
  `main()` catches it and raises `SystemExit` with code 1.

**Definition of done:** `main()` implemented, CLI arguments parsed correctly, all tests
pass, `ruff` and `mypy` report no errors, console script entry point registered in
`pyproject.toml`. `requirements.txt` updated with all third-party packages imported in
this task.
