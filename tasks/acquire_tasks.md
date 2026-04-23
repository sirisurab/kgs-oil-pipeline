# Acquire Stage Tasks

**Module:** `kgs_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

Read `stage-manifest-acquire.md`, `ADRs.md` (ADR-001, ADR-007, ADR-008), and
`build-env-manifest.md` before implementing any task in this file.

---

## Task A-01: Load and filter the lease index

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str | Path) -> pd.DataFrame`

**Description:** Read the lease index file at `index_path`
(`data/external/oil_leases_2020_present.txt`) into a pandas DataFrame. Filter rows
to those where the year component of `MONTH-YEAR` is >= 2024. The `MONTH-YEAR`
column has the format `"M-YYYY"` — extract the year by splitting on `"-"` and
taking the last element; discard rows where that element is not numeric. After
filtering, deduplicate by the `URL` column so that each unique lease URL appears
exactly once. Return the deduplicated DataFrame with the full set of columns from
the source file.

**Error handling:**
- If `index_path` does not exist, raise `FileNotFoundError` with a descriptive message.
- If the `URL` column is absent from the file, raise `ValueError`.
- If the `MONTH-YEAR` column is absent from the file, raise `ValueError`.

**Dependencies:** pandas, pathlib

**Test cases (unit):**
- Given a synthetic lease index with rows spanning 2022–2025, assert the returned
  DataFrame contains only rows where the parsed year >= 2024.
- Given rows with malformed `MONTH-YEAR` values such as `"-1-1965"` and `"0-1966"`,
  assert those rows are dropped and no exception is raised.
- Given a lease index where the same URL appears in multiple rows (one per month),
  assert the returned DataFrame contains that URL exactly once.
- Given a file path that does not exist, assert `FileNotFoundError` is raised.
- Given a file missing the `URL` column, assert `ValueError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-02: Extract lease ID from URL

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:** Extract the lease ID from a KGS lease URL. The lease ID is the
value of the `f_lc` query parameter. For example, given the URL
`"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`, return
`"1001135839"`.

**Error handling:**
- If the `f_lc` parameter is absent from the URL, raise `ValueError` with a
  descriptive message including the URL.

**Dependencies:** urllib.parse

**Test cases (unit):**
- Given the example URL above, assert the return value is `"1001135839"`.
- Given a URL with no `f_lc` parameter, assert `ValueError` is raised.
- Given a URL with multiple query parameters where `f_lc` is not first, assert the
  correct value is extracted.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-03: Fetch the MonthSave download link

**Module:** `kgs_pipeline/acquire.py`
**Function:** `fetch_download_link(lease_id: str, session: requests.Session) -> str`

**Description:** Make an HTTP GET request to the MonthSave page for the given
`lease_id` at `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}`.
Parse the HTML response using BeautifulSoup to find the anchor tag whose `href`
contains `"anon_blobber.download"`. Return the full URL of that anchor tag as a
string.

**Error handling:**
- If the HTTP request fails (non-2xx status), raise `requests.HTTPError`.
- If no anchor tag matching the pattern is found in the page, raise `ValueError`
  with a message that includes the `lease_id`.

**Dependencies:** requests, beautifulsoup4

**Test cases (unit — all HTTP calls mocked):**
- Given a mocked HTML response containing one matching anchor tag, assert the
  returned URL string equals the expected download URL.
- Given a mocked HTML response with no matching anchor tag, assert `ValueError`
  is raised.
- Given a mocked HTTP response with status 404, assert `requests.HTTPError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-04: Download a single lease data file

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_lease_file(download_url: str, output_dir: str | Path, session: requests.Session) -> Path | None`

**Description:** Make an HTTP GET request to `download_url`. Extract the target
filename from the `p_file_name` query parameter of `download_url`. If a file with
that name already exists in `output_dir` and is non-empty, skip the download and
return the existing `Path` — this is the caching behaviour described in
`stage-manifest-acquire.md` H2. Otherwise, write the response content to
`output_dir / filename`. Return the `Path` of the written file. If the response
body is empty (zero bytes), do not write a file; log a warning and return `None`.
Sleep for 0.5 seconds after each download attempt to respect the rate-limit
requirement in `task-writer-kgs.md`.

**Error handling:**
- If `output_dir` does not exist, create it before writing.
- If the HTTP request raises a connection or timeout error, log a warning and
  return `None` — do not propagate the exception.
- If the `p_file_name` parameter is absent from `download_url`, raise `ValueError`.

**Dependencies:** requests, pathlib, time, logging

**Test cases (unit — all HTTP calls mocked):**
- Given a mocked download response with valid content and a filename derived from
  `p_file_name`, assert the file is written to `output_dir` and the returned `Path`
  points to it.
- Given a pre-existing non-empty file at the expected path, assert no HTTP request
  is made and the existing `Path` is returned (idempotency — TR-20).
- Given a mocked response with a zero-byte body, assert no file is written, a
  warning is logged, and `None` is returned (H2 from `stage-manifest-acquire.md`).
- Given a mocked connection error on the download request, assert a warning is
  logged and `None` is returned.
- Given a `download_url` with no `p_file_name` parameter, assert `ValueError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-05: Orchestrate parallel acquire with Dask

**Module:** `kgs_pipeline/acquire.py`
**Function:** `acquire(config: dict) -> list[Path]`

**Description:** Orchestrate the full acquire workflow for all lease URLs derived
from the filtered lease index. Load the lease index using `load_lease_index`,
extract all lease URLs, and for each URL: extract the lease ID, fetch the download
link, then download the file. Execute the per-URL download workflow in parallel
using the Dask threaded scheduler (I/O-bound — ADR-001). Limit concurrency to a
maximum of 5 workers as specified in `task-writer-kgs.md`. Return a list of `Path`
objects for all successfully downloaded files (i.e. non-`None` results).

All path and URL settings must be read from `config` — no hardcoded paths or URLs.
The relevant config keys are defined in `build-env-manifest.md` under the `acquire`
section.

**Error handling:**
- Per-lease errors must not abort the run; log each failure and continue.
- If the lease index file is missing, raise `FileNotFoundError` and abort.

**Dependencies:** dask, requests, logging, pathlib

**Test cases (unit — all HTTP calls mocked):**
- Given a config pointing to a synthetic lease index with 3 URLs and mocked HTTP
  responses, assert that `acquire` returns a list of 3 `Path` objects.
- Given one URL that produces a download failure, assert the returned list contains
  2 paths (the successful ones) and a warning is logged for the failed one.
- Given a config where the lease index file does not exist, assert `FileNotFoundError`
  is raised.

**Test cases (TR-20 — idempotency, unit):**
- Run `acquire` twice against the same `output_dir`; assert the file count does not
  increase on the second run.
- Assert the content of a pre-existing file is unchanged after the second run.
- Assert no exception is raised when target files already exist.

**Test cases (TR-21 — file integrity, unit with mocked acquire):**
- Assert every file in `output_dir` after a mocked acquire run has size > 0 bytes.
- Assert every file is readable as UTF-8 text without `UnicodeDecodeError`.
- Assert every file contains at least one data row beyond the header line.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
