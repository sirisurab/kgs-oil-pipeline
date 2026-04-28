# Acquire Stage Tasks

**Module:** `kgs_pipeline/acquire.py`  
**Test file:** `tests/test_acquire.py`  
**Governing docs:** stage-manifest-acquire.md, ADR-001, ADR-006, ADR-007, build-env-manifest.md

---

## Task A-01: Load and filter the lease index

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `load_lease_index(config: dict) -> pd.DataFrame`

**Description:**  
Read the lease index file at the path given by `config["acquire"]["lease_index_path"]`
into a pandas DataFrame. Filter rows to those whose MONTH-YEAR column has a year
component >= the target year given by `config["acquire"]["target_year"]`. The MONTH-YEAR
format is "M-YYYY"; extract the year by splitting on "-" and taking the last element. Drop
rows where the year component is not numeric before applying the year filter. After
filtering, deduplicate by the URL column, keeping the first occurrence of each unique URL.
Return the deduplicated DataFrame.

**Input:** `config` dict whose structure is defined in build-env-manifest.md.

**Output:** `pd.DataFrame` with one row per unique lease URL satisfying the year filter.
The URL column is present and non-null for every row. All other columns from the source
file are retained unchanged.

**Constraints:**
- Scheduler choice: see ADR-001 (acquire is I/O-bound; threaded scheduler governs).
- Non-numeric year rows must be dropped before the year comparison — do not attempt
  integer conversion on un-validated strings.
- Deduplication must key on the URL column specifically — a bare `drop_duplicates()` with
  no subset must not be used.
- No retry logic, no caching, no URL discovery or index scraping.

**Test cases:**
- Given a DataFrame with MONTH-YEAR values ["1-2024", "6-2025", "3-2023"], assert only
  the 2024 and 2025 rows survive the filter.
- Given rows with non-numeric MONTH-YEAR values such as "-1-1965" and "0-1966", assert
  those rows are dropped before year comparison.
- Given two rows with the same URL but different MONTH-YEAR values, assert only one row
  is returned after deduplication.
- Assert the return type is `pd.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-02: Extract lease ID from URL

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `extract_lease_id(url: str) -> str`

**Description:**  
Parse the `f_lc` query parameter from the lease URL and return it as a string. For
example, `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839` returns
`"1001135839"`. The function must handle the exact URL format produced by the KGS lease
index.

**Input:** `url: str` — a lease URL from the URL column of the lease index.  
**Output:** `str` — the lease ID value of the `f_lc` query parameter.

**Constraints:**
- No retry logic, no caching, no URL discovery.
- If the `f_lc` parameter is absent from the URL, raise `ValueError` with a message
  identifying the URL.

**Test cases:**
- Given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`, assert
  the return value is `"1001135839"`.
- Given a URL without an `f_lc` parameter, assert `ValueError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-03: Resolve download URL from MonthSave page

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `resolve_download_url(lease_id: str, config: dict) -> str | None`

**Description:**  
Construct the MonthSave page URL using the template
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}`. Issue an HTTP GET
request to that URL. Parse the HTML response with BeautifulSoup to find the first anchor
tag whose `href` attribute contains `"anon_blobber.download"`. Return the full href value
as a string. If no such anchor is found, log a warning and return `None`.

**Input:** `lease_id: str`, `config: dict`.  
**Output:** `str` — the download URL, or `None` if no download link is found.

**Constraints:**
- HTTP library: see build-env-manifest.md (standard request library; browser automation
  is prohibited).
- BeautifulSoup is used here and only here — do not use it in any other stage.
- The MonthSave URL template is fixed as stated; no discovery, index scraping, or URL
  construction beyond this template.
- No retry logic, no caching.
- Logging: see ADR-006.

**Test cases:**
- Given a mocked HTTP response containing an anchor with
  `href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"`,
  assert the function returns that URL string.
- Given a mocked HTTP response containing no anchor whose href contains
  `"anon_blobber.download"`, assert the function returns `None` and logs a warning.
- Assert the constructed request URL contains the correct `lease_id`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-04: Download a single lease file

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `download_lease_file(download_url: str, output_dir: str) -> Path | None`

**Description:**  
Extract the `p_file_name` query parameter from `download_url` to determine the output
filename. If a file with that name already exists in `output_dir` and has size greater
than 0 bytes, skip the download and return the existing `Path` (idempotency — see TR-20).
Otherwise, issue an HTTP GET request to `download_url`, write the response content to
`output_dir/{p_file_name}`, and return the resulting `Path`. If the download produces an
empty response (0 bytes), delete any partial file and return `None`. Include a 0.5-second
sleep after each download attempt regardless of success or skip, as required by
task-writer-kgs.md (rate-limiting).

**Input:** `download_url: str`, `output_dir: str`.  
**Output:** `Path` to the written file, or `None` on empty-response failure.

**Constraints:**
- Idempotency: a pre-existing non-empty file must not be overwritten or cause an error
  (TR-20).
- Sleep duration: 0.5 seconds per worker call — defined in task-writer-kgs.md.
- No retry logic, no caching.
- HTTP library: see build-env-manifest.md.

**Test cases (TR-20, TR-21):**
- Given a mocked download response with content `b"LEASE_KID,LEASE\n1,TestLease\n"`,
  assert the function writes a file at `output_dir/lp564.txt` and returns its `Path`.
- Given a pre-existing non-empty file at the target path, assert the function returns the
  existing `Path` without issuing a network request and without raising any exception.
- Given a mocked download response with 0-byte content, assert the function returns `None`
  and no file remains at the target path.
- Assert that the 0.5-second sleep occurs after each download attempt (mock `time.sleep`
  and verify the call).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-05: Run parallel acquisition with Dask threaded scheduler

**Module:** `kgs_pipeline/acquire.py`  
**Function:** `acquire(config: dict) -> list[Path | None]`

**Description:**  
Orchestrate the full acquire workflow. Call `load_lease_index` to obtain the filtered,
deduplicated URL list. For each URL, build a `dask.delayed` call that: (1) calls
`extract_lease_id`, (2) calls `resolve_download_url`, and (3) if a download URL is
resolved, calls `download_lease_file`. Compute all delayed tasks using the Dask threaded
scheduler with `num_workers` set to the value at `config["acquire"]["max_workers"]` (which
must not exceed 5 — see task-writer-kgs.md). Return the list of results (each a `Path`
or `None`).

**Input:** `config: dict`.  
**Output:** `list[Path | None]` — one entry per unique lease URL processed.

**Constraints:**
- Scheduler: Dask threaded scheduler — see ADR-001 (acquire is I/O-bound).
- Max concurrent workers: value from config, not to exceed 5 (task-writer-kgs.md).
- The distributed scheduler must NOT be initialized in this stage (see build-env-manifest.md).
- No retry logic, no caching, no URL discovery beyond the workflow defined in
  task-writer-kgs.md.
- Logging: see ADR-006.

**Test cases (TR-20, TR-21):**
- Given a mocked lease index with 3 URLs and mocked HTTP responses returning valid content,
  assert the function returns a list of 3 `Path` objects.
- Given a mocked lease index with 2 URLs where one returns an empty HTTP response, assert
  the result list contains one `Path` and one `None`.
- Assert that `dask.compute` is called with the threaded scheduler (not distributed).
- Assert that running `acquire` on a directory where all target files already exist returns
  the same file count as the first run without issuing any network requests (TR-20).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A-06: Acquire stage file integrity verification (tests only)

**Test file:** `tests/test_acquire.py`

**Description:**  
Write test cases verifying the integrity of files produced by the acquire stage, per
TR-21. These tests exercise the output directory after a real or mocked acquire run.

**Test cases (TR-21):**
- Assert every file in the raw output directory after an acquire run has size > 0 bytes.
- Assert every file in the raw output directory is readable as UTF-8 text without raising
  `UnicodeDecodeError`.
- Assert every file in the raw output directory contains at least one data row beyond the
  header line (i.e., line count > 1).

**Definition of done:** Test cases are implemented, they pass using a mocked or real
acquire fixture, ruff and mypy report no errors.
