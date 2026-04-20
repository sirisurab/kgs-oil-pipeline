# Acquire Stage — Task Specifications

**Module:** `kgs_pipeline/acquire.py`
**Stage manifest:** `agent_docs/stage-manifest-acquire.md`
**ADRs in force:** ADR-001, ADR-005, ADR-006, ADR-007, ADR-008

---

## Task ACQ-01: Implement lease index loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str) -> pandas.DataFrame`

**Description:**
Load the lease index file at `data/external/oil_leases_2020_present.txt` into a
pandas DataFrame. The file is a quoted CSV (`.txt` extension, comma-separated).
After loading, apply the following filters before returning:

1. Parse the year component from the `MONTH-YEAR` column: split on `"-"`, take the
   last element. Discard rows where the year element is not a valid integer (e.g.
   `"-1-1965"`, `"0-1966"` — rows where split produces a non-numeric last token).
2. Retain only rows where the parsed year integer is >= 2024.
3. Deduplicate on the `URL` column — the index has one row per month per lease; only
   one URL per lease is needed to drive the download workflow.
4. Return the deduplicated DataFrame with the `URL` column intact.

**Error handling:**
- If `index_path` does not exist, raise `FileNotFoundError` with a descriptive message.
- If the `URL` or `MONTH-YEAR` column is absent from the file, raise `ValueError`.

**Dependencies:** pandas

**Test cases (unit):**
- Given a synthetic lease index with rows spanning 2020–2025 and some rows with
  `MONTH-YEAR` values of `"-1-1965"` and `"0-1966"`, assert that only rows with
  year >= 2024 are returned and rows with non-numeric year tokens are excluded.
- Given a synthetic lease index where multiple rows share the same URL, assert that
  the returned DataFrame has exactly one row per unique URL (deduplication).
- Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- Given a file missing the `URL` column, assert `ValueError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-02: Implement lease ID extractor

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:**
Extract the lease ID from a KGS MainLease URL. The URL format is:
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<lease_id>`

Parse the `f_lc` query parameter value and return it as a string.

**Error handling:**
- If the URL does not contain the `f_lc` query parameter, raise `ValueError` with a
  message that includes the URL.

**Dependencies:** urllib.parse (stdlib)

**Test cases (unit):**
- Given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`, assert
  the function returns `"1001135839"`.
- Given a URL without the `f_lc` parameter, assert `ValueError` is raised.
- Given a URL with additional query parameters alongside `f_lc`, assert the correct
  lease ID is returned.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-03: Implement MonthSave page parser

**Module:** `kgs_pipeline/acquire.py`
**Function:** `parse_download_link(html: str, base_url: str) -> str`

**Description:**
Given the raw HTML content of a KGS MonthSave page and the base URL of the KGS
host, locate the download anchor tag whose `href` contains `"anon_blobber.download"`
and return the full download URL as a string.

The MonthSave URL pattern is:
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<lease_id>`

The download link anchor pattern is:
`<a href="...qualified.anon_blobber.download?p_file_name=lp<N>.txt">...`

Use BeautifulSoup to parse the HTML. Return the first matching `href`. If the
`href` is a relative path, construct the absolute URL using `base_url`.

**Error handling:**
- If no anchor matching `"anon_blobber.download"` is found in the HTML, raise
  `ValueError` with a message identifying the lease.

**Dependencies:** beautifulsoup4, lxml (or html.parser)

**Test cases (unit):**
- Given a synthetic HTML string containing one anchor with an `anon_blobber.download`
  href, assert the function returns the correct full URL.
- Given a synthetic HTML string with no matching anchor, assert `ValueError` is
  raised.
- Given a relative href in the anchor, assert the function resolves it to an absolute
  URL using `base_url`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-04: Implement single-lease downloader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_lease(lease_url: str, output_dir: Path, session: requests.Session) -> Path | None`

**Description:**
Execute the two-step KGS download workflow for a single lease:

1. Derive the lease ID from `lease_url` using `extract_lease_id`.
2. Construct the MonthSave URL:
   `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<lease_id>`
3. Make an HTTP GET request to the MonthSave URL using `session`.
4. Parse the HTML response using `parse_download_link` to obtain the data file
   download URL.
5. Extract the filename from the `p_file_name` query parameter of the download URL
   (e.g. `lp564.txt`).
6. Check whether `output_dir / filename` already exists on disk. If it does, skip
   the download and return the existing path (idempotency — see TR-20).
7. Make a second HTTP GET request to the data file download URL using `session`.
8. Validate the response: status 200, non-empty content, decodable as UTF-8
   (see TR-21). If any check fails, log a warning and return `None` — do not write
   a partial or empty file.
9. Write the response content to `output_dir / filename`.
10. Sleep for 0.5 seconds after each download to avoid overloading the KGS server.
11. Return the `Path` to the saved file.

**Error handling:**
- HTTP errors (non-200 status on either request) must be caught, logged as warnings,
  and cause the function to return `None`.
- Network exceptions (`requests.exceptions.RequestException`) must be caught, logged,
  and cause the function to return `None`.
- A failed download must never produce a file on disk (shared state hazard H2 from
  stage-manifest-acquire.md).

**Dependencies:** requests, pathlib

**Test cases (unit — all mocked, no network access):**
- Given mocked responses for both the MonthSave page and the data download, assert
  the function writes a file to `output_dir` and returns the correct `Path`.
- Given a file that already exists in `output_dir`, assert the function returns the
  existing path without making any HTTP requests (idempotency — TR-20).
- Given a mocked MonthSave response that contains no download link, assert the
  function logs a warning and returns `None` without writing any file.
- Given a mocked data download response with status 500, assert the function returns
  `None` and no file is written to disk.
- Given a mocked data download response with empty content (0 bytes), assert the
  function returns `None` and no file is written to disk (TR-21).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-05: Implement retry decorator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `with_retry(max_attempts: int, backoff_base: float)`

**Description:**
Implement a decorator factory `with_retry` that wraps a callable with retry logic
using exponential backoff. The decorator must:

- Retry the wrapped function up to `max_attempts` times on any exception.
- Wait `backoff_base * (2 ** attempt)` seconds between attempts (exponential
  backoff), starting from attempt 0.
- Log each retry attempt at WARNING level, including the attempt number and the
  exception message.
- Re-raise the final exception if all attempts are exhausted.

**Dependencies:** time (stdlib), logging (stdlib)

**Test cases (unit):**
- Given a function that raises on the first call and succeeds on the second, assert
  the decorator returns the success result after one retry.
- Given a function that always raises, assert the decorator re-raises after
  `max_attempts` and that the function was called exactly `max_attempts` times.
- Given `max_attempts=1`, assert no retry occurs and the exception is re-raised
  immediately.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-06: Implement parallel acquire orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire(config: dict) -> list[Path]`

**Description:**
Orchestrate parallel download of all lease production files identified in the lease
index. The function must:

1. Load the lease index from the path specified in `config["acquire"]["lease_index"]`
   using `load_lease_index`.
2. Extract the list of unique lease URLs from the filtered, deduplicated index.
3. Resolve the output directory from `config["acquire"]["raw_dir"]`; create it if
   absent.
4. Initialize a `requests.Session` (shared across all workers for connection reuse).
5. Use Dask threaded scheduler (I/O-bound workload — see ADR-001 and
   stage-manifest-acquire.md H1) with a maximum of 5 concurrent workers (as specified
   in task-writer-kgs.md) to execute `download_lease` in parallel across all lease
   URLs.
6. Each worker call must be wrapped with the `with_retry` decorator (ACQ-05) with
   settings from `config["acquire"]`.
7. Collect and return the list of `Path` objects for all successfully downloaded
   files (excluding `None` results from failed downloads).
8. Log a summary at INFO level: total leases attempted, succeeded, and failed.

**Error handling:**
- If the lease index file is missing, log the error and re-raise.
- Individual download failures must not abort the batch — failed leases are logged
  and skipped.

**Dependencies:** dask, requests, pathlib, logging

**Test cases (unit — all mocked):**
- Given a synthetic config and a mocked `load_lease_index` returning 3 URLs, and
  mocked `download_lease` returning a valid `Path` for each, assert `run_acquire`
  returns a list of 3 paths.
- Given mocked `download_lease` returning `None` for 1 of 3 URLs, assert the
  returned list has 2 paths and a WARNING is logged for the failed lease.
- Assert that the Dask computation uses the threaded scheduler, not the distributed
  scheduler.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-07: Implement acquired file integrity validator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `validate_raw_files(raw_dir: Path) -> list[str]`

**Description:**
Scan all files in `raw_dir` and return a list of error messages for any file that
fails integrity checks. A file passes integrity if:

1. Its size is greater than 0 bytes.
2. Its content is decodable as UTF-8 without error.
3. It contains at least one data row beyond the header line (i.e., at least 2
   non-empty lines).

Files that pass all checks produce no entry in the returned list. Files that fail
produce one descriptive error string per failure mode.

**Error handling:**
- If `raw_dir` does not exist, raise `FileNotFoundError`.
- If a file cannot be opened due to OS-level errors, include the filename and error
  in the returned error list rather than raising.

**Dependencies:** pathlib (stdlib)

**Test cases (unit — TR-21):**
- Given a temp directory containing one valid file (non-empty, UTF-8, 2+ lines),
  assert the function returns an empty list.
- Given a temp directory containing a zero-byte file, assert the function returns
  one error entry mentioning that file.
- Given a file containing only a header line (1 line, no data rows), assert the
  function returns one error entry.
- Given a file with non-UTF-8 binary content, assert the function returns one error
  entry mentioning the file.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ACQ-08: Wire acquire stage entry point

**Module:** `kgs_pipeline/acquire.py`
**Function:** `acquire(config: dict) -> None`

**Description:**
Top-level entry point for the acquire stage, called by the pipeline orchestrator
(build-env-manifest.md). The function must:

1. Call `run_acquire(config)` to download all lease files.
2. Call `validate_raw_files` on the output directory.
3. If any integrity errors are returned, log each error at WARNING level.
4. Log a stage-complete summary at INFO level.

This function must not initialize a Dask distributed cluster — acquire is I/O-bound
and uses the Dask threaded scheduler internally (ADR-001).

**Dependencies:** logging (stdlib)

**Test cases (unit — all mocked):**
- Given mocked `run_acquire` that returns 5 paths and mocked `validate_raw_files`
  that returns an empty list, assert `acquire` logs a stage-complete summary.
- Given mocked `validate_raw_files` returning 2 error messages, assert both are
  logged at WARNING level.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.
