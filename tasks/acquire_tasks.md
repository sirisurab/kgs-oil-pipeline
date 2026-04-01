# Acquire Component — Task Specifications

## Overview

The acquire component downloads KGS (Kansas Geological Survey) monthly lease-level oil and gas
production data files for the 2024–present period. It reads a lease index file
(`data/external/oil_leases_2024_present.txt`) that was pre-filtered from the full
`oil_leases_2020_present.txt` archive, extracts unique lease download URLs, and for each lease
fetches the per-lease production data file from the KGS CHASM server. Downloads are executed in
parallel using Dask (max 5 concurrent workers) with a 0.5 s per-worker sleep to avoid
overloading the KGS server. All raw files are saved to `data/raw/`. The stage is idempotent:
files already present on disk are skipped on re-runs.

**Module:** `kgs_pipeline/acquire.py`
**Entry-point CLI:** `python -m kgs_pipeline.acquire` (or invoked by `pipeline.py`)
**Test file:** `tests/test_acquire.py`

---

## Design decisions and constraints

- Use `requests` for all HTTP operations. Do NOT use Playwright, Selenium, or any browser automation.
- Use `BeautifulSoup` (html.parser) for parsing MonthSave HTML responses.
- Use Dask `delayed` + `dask.compute` (scheduler="threads") with `num_workers=5` for parallelism.
- Add `time.sleep(0.5)` inside each download worker function to rate-limit server requests.
- A retry decorator with exponential backoff (max 3 retries, base delay 1 s, factor 2) must wrap
  each HTTP GET call. The decorator is defined in `kgs_pipeline/utils.py` and imported here.
- The lease index file read at acquire time is `data/external/oil_leases_2024_present.txt` (the
  filtered version, not the raw 2020-present file). The Makefile `acquire` target must reference
  this filename.
- Filter the index to rows where the year component of MONTH-YEAR >= 2024, then deduplicate by URL.
  MONTH-YEAR format is "M-YYYY"; extract year by splitting on "-" and taking the last element.
  Drop rows where the year component is not numeric before filtering.
- Raw output files are saved to `data/raw/` using the filename extracted from the `p_file_name`
  query parameter of the download URL (e.g., `lp564.txt`).
- If a file already exists in `data/raw/` (idempotency), skip the download and return the
  existing path without raising an error or re-downloading.
- `pyproject.toml` build-backend must be `"setuptools.build_meta"`.
- `Makefile` must include a `make env` target using `python3 -m venv .venv` and an `install`
  target that bootstraps pip/setuptools/wheel before `pip install -e ".[dev]"`.
- `pyproject.toml` dev dependencies must include `pandas-stubs` and `types-requests`.
- `data/` must be present in `.gitignore`.
- `requirements.txt` must be updated with every third-party package imported in this module.

---

## Task 01: Implement lease index loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str) -> pd.DataFrame`

**Description:**
Read the lease index text file (pipe/comma-delimited CSV with quoted fields) from
`data/external/oil_leases_2024_present.txt` into a Pandas DataFrame. Apply the following
transformations before returning:
1. Parse the MONTH-YEAR column (format "M-YYYY"). Extract the year component by splitting on
   "-" and taking the **last** element (index -1).
2. Drop rows where the extracted year component is not numeric (e.g., "-1-1965", "0-1966").
3. Cast the year component to int.
4. Retain only rows where year >= 2024.
5. Deduplicate the DataFrame by the URL column, keeping the first occurrence.
6. Return the deduplicated DataFrame with only the URL column (other columns may be retained
   but URL must be present).

The function must accept an arbitrary file path so tests can point it at fixture files.

**Inputs:** `index_path` — absolute or relative path to the lease index CSV/text file.

**Outputs:** `pd.DataFrame` with deduplicated rows filtered to year >= 2024.

**Edge cases:**
- File does not exist → raise `FileNotFoundError` with a descriptive message.
- File is empty → raise `ValueError("Lease index file is empty")`.
- No rows survive the year >= 2024 filter → raise `ValueError("No leases found for year >= 2024")`.
- Rows where MONTH-YEAR year component is not a digit string after split must be silently dropped
  before the int cast (do not raise on individual bad rows).

**Test cases:**
- `@pytest.mark.unit` — Given a minimal CSV fixture with 3 rows: two with MONTH-YEAR "1-2024"
  and one with "6-2023", assert the returned DataFrame has exactly 2 rows (the 2023 row is
  excluded).
- `@pytest.mark.unit` — Given a CSV fixture with duplicate URLs in the 2024 rows, assert the
  returned DataFrame has no duplicate URL values.
- `@pytest.mark.unit` — Given a CSV fixture containing a row with MONTH-YEAR "-1-1965" (invalid
  year component), assert the row is silently dropped and no exception is raised.
- `@pytest.mark.unit` — Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a CSV where all rows are pre-2024, assert `ValueError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement lease ID extractor

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:**
Parse the lease URL and return the lease ID string. The URL format is:
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_ID>`
Extract the value of the `f_lc` query parameter. If the parameter is absent or the URL is
malformed, raise `ValueError` with a message that includes the offending URL.

**Inputs:** `url` — a URL string from the lease index URL column.

**Outputs:** `str` — the lease ID (e.g., `"1001135839"`).

**Edge cases:**
- URL with no query string → raise `ValueError`.
- URL where `f_lc` parameter is present but empty → raise `ValueError`.
- URL with extra query parameters alongside `f_lc` → correctly return only the `f_lc` value.

**Test cases:**
- `@pytest.mark.unit` — Given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`,
  assert the function returns `"1001135839"`.
- `@pytest.mark.unit` — Given a URL with no `f_lc` parameter, assert `ValueError` is raised.
- `@pytest.mark.unit` — Given a URL with an empty `f_lc` value, assert `ValueError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement MonthSave page scraper

**Module:** `kgs_pipeline/acquire.py`
**Function:** `scrape_download_url(lease_id: str, session: requests.Session) -> str`

**Description:**
Construct the MonthSave URL for the given lease ID:
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<lease_id>`

Make an HTTP GET request to that URL using the provided `session`. Parse the HTML response
with BeautifulSoup (html.parser). Find the first anchor tag whose `href` attribute contains
the string `"anon_blobber.download"`. Return the full resolved download URL (the href value
as-is — it is already an absolute URL). If no such anchor tag exists, raise `ScrapingError`
(a custom exception defined in this module).

The HTTP GET call must be wrapped with the retry decorator from `kgs_pipeline/utils.py`.

**Custom exception:** Define `class ScrapingError(Exception): pass` at module level.

**Inputs:**
- `lease_id: str` — the lease identifier string.
- `session: requests.Session` — a shared requests Session for connection pooling.

**Outputs:** `str` — the absolute download URL containing `anon_blobber.download`.

**Edge cases:**
- MonthSave page returns HTTP 4xx/5xx → the retry decorator raises after max retries; let it
  propagate.
- MonthSave page has no `anon_blobber.download` link → raise `ScrapingError`.
- MonthSave page HTML is malformed → BeautifulSoup degrades gracefully; if no link found, raise
  `ScrapingError`.

**Test cases:**
- `@pytest.mark.unit` — Mock `session.get` to return a response with HTML containing one anchor
  with `href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"`;
  assert the function returns that exact URL.
- `@pytest.mark.unit` — Mock `session.get` to return HTML with no `anon_blobber.download` link;
  assert `ScrapingError` is raised.
- `@pytest.mark.unit` — Mock `session.get` to return an empty HTML body; assert `ScrapingError`
  is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement file downloader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_file(download_url: str, output_dir: str, session: requests.Session) -> Path`

**Description:**
1. Extract the filename from the `p_file_name` query parameter of the download URL
   (e.g., `"lp564.txt"` from `"…?p_file_name=lp564.txt"`). If the parameter is missing, raise
   `ValueError`.
2. Construct the output path: `Path(output_dir) / filename`.
3. **Idempotency check:** if the file already exists at that path, return the path immediately
   without making any HTTP request.
4. Make an HTTP GET request to the download URL using `session`. The retry decorator from
   `kgs_pipeline/utils.py` must wrap this call.
5. If the response body is empty (zero bytes), raise `ValueError(f"Downloaded file is empty: {filename}")`.
6. Write the response content (bytes) to the output path.
7. Sleep for 0.5 seconds (`time.sleep(0.5)`) after a successful download.
8. Return the `Path` to the saved file.

**Inputs:**
- `download_url: str` — the full download URL with `p_file_name` query parameter.
- `output_dir: str` — directory where raw files are saved (typically `data/raw/`).
- `session: requests.Session` — shared requests Session.

**Outputs:** `pathlib.Path` pointing to the saved (or pre-existing) raw data file.

**Edge cases:**
- File already exists → return immediately (no download, no sleep, no error). `[TR-20]`
- Empty response body → raise `ValueError`. `[TR-21]`
- `p_file_name` query parameter missing → raise `ValueError`.
- `output_dir` does not exist → create it with `Path(output_dir).mkdir(parents=True, exist_ok=True)`.

**Test cases:**
- `@pytest.mark.unit` — Mock `session.get` to return 200 with non-empty byte content;
  assert the function returns a `Path` ending in the expected filename and the file exists
  on disk (use `tmp_path` pytest fixture for `output_dir`).
- `@pytest.mark.unit` — Call the function a second time for a file that already exists;
  assert `session.get` is NOT called (mock call count == 0) and the same path is returned. `[TR-20]`
- `@pytest.mark.unit` — Mock `session.get` to return 200 with empty byte content `b""`;
  assert `ValueError` is raised. `[TR-21]`
- `@pytest.mark.unit` — Given a download URL with no `p_file_name` parameter, assert
  `ValueError` is raised.
- `@pytest.mark.unit` — Given a non-existent `output_dir`, assert the directory is created
  and the file is written successfully.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement single-lease download worker

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_lease(url: str, output_dir: str, session: requests.Session) -> Path | None`

**Description:**
Orchestrate the full download workflow for a single lease URL:
1. Call `extract_lease_id(url)` to get the lease ID.
2. Call `scrape_download_url(lease_id, session)` to get the data file download URL.
3. Call `download_file(download_url, output_dir, session)` to fetch and save the file.
4. Return the resulting `Path`.

If any step raises an exception (`ScrapingError`, `ValueError`, or a `requests` exception),
log a WARNING that includes the URL and the exception message, and return `None`. Do not
propagate exceptions — the parallel runner must continue with remaining leases even if some fail.

**Inputs:**
- `url: str` — the lease main-page URL from the index.
- `output_dir: str` — target directory for raw files.
- `session: requests.Session` — shared requests Session.

**Outputs:** `pathlib.Path` on success, `None` on failure.

**Test cases:**
- `@pytest.mark.unit` — Mock `scrape_download_url` and `download_file` to return successfully;
  assert the function returns a `Path`.
- `@pytest.mark.unit` — Mock `scrape_download_url` to raise `ScrapingError`; assert the function
  returns `None` (does not raise) and a WARNING is logged.
- `@pytest.mark.unit` — Mock `extract_lease_id` to raise `ValueError`; assert `None` is returned
  and WARNING is logged.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement parallel acquire orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire(index_path: str, output_dir: str, max_workers: int = 5) -> list[Path]`

**Description:**
This is the main entry point for the acquire stage. It:
1. Calls `load_lease_index(index_path)` to obtain the deduplicated list of lease URLs for
   year >= 2024.
2. Opens a `requests.Session` (one shared session for all workers).
3. Builds a list of `dask.delayed` tasks — one per URL — each calling `download_lease(url,
   output_dir, session)`.
4. Executes all delayed tasks using `dask.compute(*tasks, scheduler="threads",
   num_workers=max_workers)`.
5. Collects results, filters out `None` values, and returns the list of successful `Path` objects.
6. Logs a summary: total URLs attempted, number of successes, number of failures.

The function must not call `.compute()` on any Dask DataFrame — it uses `dask.delayed` for
task parallelism only, not the Dask DataFrame API.

**Inputs:**
- `index_path: str` — path to the lease index file.
- `output_dir: str` — directory for raw downloaded files.
- `max_workers: int` — Dask thread concurrency limit (default 5).

**Outputs:** `list[pathlib.Path]` — paths to all successfully downloaded (or pre-existing) files.

**Edge cases:**
- All lease downloads fail → return empty list (do not raise).
- `index_path` does not exist → propagate `FileNotFoundError` from `load_lease_index`.
- `output_dir` does not exist → created by `download_file` internally.

**Test cases:**
- `@pytest.mark.unit` — Mock `load_lease_index` to return a DataFrame with 3 URLs; mock
  `download_lease` to return a `Path` for each; assert the returned list has length 3.
- `@pytest.mark.unit` — Mock `download_lease` to return `None` for all URLs; assert the
  returned list is empty.
- `@pytest.mark.unit` — Mock `download_lease` to return `Path` for 2 out of 3 URLs (one
  returns `None`); assert the returned list has length 2.
- `@pytest.mark.unit` — Assert that `run_acquire` respects the `max_workers` parameter by
  verifying it is passed to `dask.compute` (inspect the call via mock).
- `@pytest.mark.integration` — (Requires network) Run `run_acquire` against a real index
  fixture containing 1–2 known 2024 lease URLs; assert that at least one `.txt` file is
  created in `data/raw/` with size > 0 bytes. `[TR-20, TR-21]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement build environment and project scaffolding

**Module:** `pyproject.toml`, `Makefile`, `.gitignore`, `requirements.txt`

**Description:**
Create or verify the following project-level artefacts so the pipeline installs and runs
cleanly on a fresh machine:

**`pyproject.toml`:**
- `build-backend` must be `"setuptools.build_meta"` (NOT `"setuptools.backends.legacy:build"`).
- `[project]` section: name, version, `requires-python = ">=3.11"`.
- Runtime dependencies: `pandas`, `dask[dataframe]`, `requests`, `beautifulsoup4`, `pyarrow`,
  `pyyaml`, `numpy`.
- `[project.optional-dependencies]` dev section: `pytest`, `pytest-mock`, `ruff`, `mypy`,
  `pandas-stubs`, `types-requests`.

**`Makefile`:**
- `env` target: `python3 -m venv .venv`
- `install` target: activates venv, runs `pip install --upgrade pip setuptools wheel`, then
  `pip install -e ".[dev]"`. Must NOT use `pip install -r requirements.txt` as the sole step.
- `acquire` target: runs `python -m kgs_pipeline.acquire` with the path
  `data/external/oil_leases_2024_present.txt`. Must NOT reference `oil_leases_2020_present.txt`.
  Must NOT reference playwright.
- `test` target: `pytest tests/ -v`
- `lint` target: `ruff check kgs_pipeline/ tests/`
- `typecheck` target: `mypy kgs_pipeline/`

**`.gitignore`:**
- Must contain `data/` to prevent raw and processed data files from being committed.
- Also include `.venv/`, `__pycache__/`, `*.pyc`, `*.egg-info/`, `.pytest_cache/`, `.ruff_cache/`.

**`requirements.txt`:**
- Must list all third-party runtime packages with pinned or minimum versions.

**Test cases:**
- `@pytest.mark.unit` — Assert that `pyproject.toml` contains `"setuptools.build_meta"` as
  the build-backend (read the file and check the string).
- `@pytest.mark.unit` — Assert that `Makefile` does NOT contain the string `"playwright"`.
- `@pytest.mark.unit` — Assert that `Makefile` references `"oil_leases_2024_present.txt"` (not
  the 2020 version) in the acquire target.
- `@pytest.mark.unit` — Assert that `.gitignore` contains the line `data/`.

**Definition of done:** All project files created/updated, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in this task.
