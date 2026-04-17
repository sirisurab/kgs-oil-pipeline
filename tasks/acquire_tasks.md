# Acquire Stage — Task Specifications

## Context and Design Decisions

The acquire stage downloads raw KGS lease production files from the KGS public portal
and saves them to `data/raw/`. It is I/O-bound and must use Dask's threaded scheduler
(not the distributed scheduler) for parallel downloads. Browser automation tools are
prohibited; only standard HTTP request and HTML parsing libraries are permitted.

**Key ADRs governing this stage:**
- ADR-001: Use Dask threaded scheduler for I/O-bound acquire (not distributed)
- ADR-006: Dual-channel logging — log to both console and file; read log config from config.yaml
- ADR-007: HTTP acquisition uses standard requests + BeautifulSoup only

**Shared state hazards (from stage manifest):**
- H1: Acquire is I/O-bound — the threaded scheduler must be used, not the distributed scheduler
- H2: A failed download must not produce a file that appears valid — file existence alone is not proof of validity

**Package name:** `kgs_pipeline`
**Module path:** `kgs_pipeline/acquire.py`

**Input:**
- `data/external/oil_leases_2020_present.txt` — lease index file (CSV format, quoted fields)
- `config.yaml` — acquire settings (base URLs, output paths, worker counts, rate-limit)

**Output:**
- Raw lease production text files saved to `data/raw/` (e.g. `lp564.txt`)

---

## Data Source Description

The lease index file (`data/external/oil_leases_2020_present.txt`) has these columns
(from `references/kgs_archives_data_dictionary.csv`):

| Column | dtype | nullable | Description |
|---|---|---|---|
| LEASE_KID | int | no | Unique lease ID assigned by KGS |
| LEASE | string | yes | Lease name |
| DOR_CODE | int | yes | Kansas Dept of Revenue ID |
| API_NUMBER | string | yes | Well API numbers linked to lease |
| FIELD | string | yes | Oil and gas field name |
| PRODUCING_ZONE | string | yes | Producing formation |
| OPERATOR | string | yes | Lease operator name |
| COUNTY | string | yes | County where lease is located |
| TOWNSHIP | int | yes | PLSS township number |
| TWN_DIR | categorical | yes | Township direction (S\|N) |
| RANGE | int | yes | PLSS range number |
| RANGE_DIR | categorical | yes | Range direction (E\|W) |
| SECTION | int | yes | PLSS section (1–36) |
| SPOT | string | yes | Legal quarter description |
| LATITUDE | float | yes | NAD 1927 latitude |
| LONGITUDE | float | yes | NAD 1927 longitude |
| MONTH-YEAR | string | no | Format "M-YYYY" (e.g. "1-2024") |
| PRODUCT | categorical | no | O = oil, G = gas |
| WELLS | int | yes | Number of contributing wells |
| PRODUCTION | float | yes | Barrels (oil) or MCF (gas) |
| URL | string | yes | URL to main lease production page |

The URL column contains lease page URLs of the form:
`https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_KID>`

---

## Download Workflow (per lease)

1. Extract lease ID from URL: parse the `f_lc` query parameter value
2. Construct MonthSave URL: `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<lease_id>`
3. HTTP GET request to MonthSave URL; parse HTML with BeautifulSoup to find the anchor
   tag whose `href` contains `anon_blobber.download`
4. Extract the `p_file_name` query parameter from the download href to determine
   the output filename (e.g. `lp564.txt`)
5. HTTP GET request to the download URL; save response content to `data/raw/<p_file_name>`
6. Sleep 0.5 seconds per worker after each download to rate-limit server load

**Filtering before constructing the download list:**
- Filter the lease index to rows where the year component of MONTH-YEAR >= 2024
- Extract year by splitting MONTH-YEAR on "-" and taking the last element
- After filtering, deduplicate by URL — the index has one row per month per lease,
  not one row per lease
- Only the deduplicated URL list is passed to the download scheduler

---

## Task 01: Implement lease index loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str | Path, min_year: int) -> pd.DataFrame`

**Description:**
Read the lease index CSV file into a pandas DataFrame. Apply the year filter and URL
deduplication required before constructing the download list.

**Steps:**
- Read the file at `index_path` using pandas; file is quoted CSV format
- Parse MONTH-YEAR column by splitting on "-" and taking the last element to extract year
- Drop rows where the extracted year is not numeric (e.g. "-1-1965", "0-1966")
- Drop rows where the integer year < `min_year`
- Deduplicate by URL column — retain one row per unique URL
- Return the deduplicated DataFrame

**Error handling:**
- If `index_path` does not exist, raise `FileNotFoundError` with a descriptive message
- If the URL column is absent from the file, raise `ValueError` naming the missing column
- If MONTH-YEAR column is absent, raise `ValueError` naming the missing column

**Dependencies:** pandas, pathlib

**Test cases:**
- Given a sample index with rows for years 2022, 2023, and 2024 with `min_year=2024`,
  assert only 2024 rows are returned
- Given rows where MONTH-YEAR has non-numeric year parts (e.g. "-1-1965"), assert those
  rows are dropped before the year filter is applied
- Given a sample where multiple rows share the same URL (simulating multiple months for
  one lease), assert the output has one row per unique URL
- Given a path to a nonexistent file, assert `FileNotFoundError` is raised
- Given a file missing the URL column, assert `ValueError` is raised

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement lease ID extractor

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:**
Parse the KGS lease page URL and extract the `f_lc` query parameter value (lease ID).

**Steps:**
- Parse the URL using `urllib.parse.urlparse` and `urllib.parse.parse_qs`
- Extract the `f_lc` query parameter value
- Return it as a string

**Error handling:**
- If the `f_lc` parameter is absent from the URL, raise `ValueError` with a message
  that includes the offending URL

**Dependencies:** urllib.parse (stdlib)

**Test cases:**
- Given `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"`,
  assert the return value is `"1001135839"`
- Given a URL with no `f_lc` parameter, assert `ValueError` is raised
- Given a URL with multiple query parameters where `f_lc` is not first, assert the
  correct value is still returned

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement MonthSave page parser

**Module:** `kgs_pipeline/acquire.py`
**Function:** `parse_download_link(html_content: str, base_url: str) -> str`

**Description:**
Parse the HTML content of a KGS MonthSave page and extract the absolute download URL
from the anchor tag whose `href` contains `"anon_blobber.download"`.

**Steps:**
- Parse `html_content` using BeautifulSoup
- Find the first anchor (`<a>`) tag whose `href` attribute contains the string
  `"anon_blobber.download"`
- If the href is relative, construct the absolute URL by combining with `base_url`
- Extract the `p_file_name` query parameter from the download URL
- Return the complete absolute download URL as a string

**Error handling:**
- If no matching anchor tag is found, raise `ValueError` with a message that includes
  the first 200 characters of `html_content` for diagnosis

**Dependencies:** beautifulsoup4, urllib.parse (stdlib)

**Test cases:**
- Given HTML containing an anchor with `href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"`,
  assert the return value is that full URL
- Given HTML containing an anchor with a relative href that includes `anon_blobber.download`,
  assert the return value is the absolute URL constructed from `base_url`
- Given HTML with no anchor containing `anon_blobber.download`, assert `ValueError` is raised

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement single-file downloader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_lease_file(lease_url: str, output_dir: Path, session: requests.Session) -> Path | None`

**Description:**
Execute the full two-step download workflow for a single lease URL: fetch the MonthSave
page, parse the download link, download the file, and save it to disk. Return the path
to the saved file, or `None` if the download was skipped (file already exists) or failed.

**Steps:**
- Extract lease ID from `lease_url` using `extract_lease_id`
- Construct MonthSave URL from the lease ID
- If the output file already exists in `output_dir` (determined after parsing the
  download link from a HEAD or partial GET), skip the download and return the existing
  path without making additional requests — idempotency requirement (TR-20)
- HTTP GET the MonthSave page URL using `session`
- Parse the HTML response with `parse_download_link` to obtain the file download URL
- Extract the output filename from the `p_file_name` parameter of the download URL
- If a file with that name already exists in `output_dir`, return its path immediately
  without downloading again (TR-20)
- HTTP GET the download URL using `session`
- Write the response content to `output_dir / <filename>` atomically: write to a
  `.tmp` file first, then rename to the final filename on success
- If the downloaded file is 0 bytes, delete it and return `None` (TR-21, H2)
- Sleep 0.5 seconds after a successful download
- Return the `Path` to the saved file

**Error handling:**
- On any `requests.RequestException` (timeout, connection error, HTTP error), log a
  warning with the lease URL and the exception, delete any partial `.tmp` file, and
  return `None`
- On `ValueError` from `parse_download_link` (no download link found), log a warning
  and return `None`
- Do not raise exceptions to the caller — all error paths return `None`

**Dependencies:** requests, pathlib, time (stdlib), logging (stdlib)

**Test cases (use `unittest.mock.patch` to mock HTTP calls):**
- Given a successful two-step fetch that returns valid HTML and file content, assert
  the file is saved to `output_dir` and the returned path exists
- Given that the target file already exists in `output_dir`, assert no HTTP requests
  are made and the existing path is returned (TR-20a, TR-20b, TR-20c)
- Given a `requests.RequestException` on the MonthSave request, assert `None` is
  returned and no file is created
- Given a successful MonthSave response but `ValueError` from `parse_download_link`,
  assert `None` is returned
- Given a download response with 0-byte content, assert `None` is returned and the
  `.tmp` file is not left on disk
- Given a network error on the file download request (after MonthSave succeeds), assert
  the `.tmp` file is cleaned up and `None` is returned

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement parallel download orchestrator

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire(config: AcquireConfig) -> AcquireSummary`

**Description:**
Orchestrate parallel downloads of all filtered lease files using Dask's threaded
scheduler. Rate-limit to a maximum of 5 concurrent workers. Return a summary of
the run results.

**Steps:**
- Load the lease index using `load_lease_index` with `min_year` from config
- Extract the list of unique lease URLs from the filtered index
- Log the total count of leases to download
- Create a `requests.Session` configured with the retry settings from config
- Build a Dask bag from the list of lease URLs
- Map `download_lease_file` over the bag using Dask's threaded scheduler with at
  most `config.max_workers` (capped at 5) threads
- Call `.compute()` once to execute all downloads
- Collect results: count of files downloaded, count skipped (already existed),
  count failed (returned `None` after a new attempt)
- Log a summary: total URLs, downloaded, skipped, failed
- Return an `AcquireSummary` dataclass with: `total`, `downloaded`, `skipped`, `failed`,
  `output_dir`

**Scheduler requirement (ADR-001, H1):**
- Must use Dask threaded scheduler (`scheduler="threads"`) — not the distributed
  scheduler, not synchronous, not multiprocessing

**Error handling:**
- If `output_dir` does not exist, create it before dispatching downloads
- If all downloads fail, log an error and raise `RuntimeError` naming the output dir
  and the failure count

**Dependencies:** dask, requests, dataclasses (stdlib), logging (stdlib), pathlib

**Test cases (use `unittest.mock.patch` to mock `download_lease_file`):**
- Given a list of 3 lease URLs with mocked `download_lease_file` returning a valid
  path for each, assert `AcquireSummary.downloaded == 3` and `failed == 0`
- Given a list of 3 URLs where one returns `None` (failed), assert
  `AcquireSummary.failed == 1` and `downloaded == 2`
- Given a list where all files already exist (mocked to return existing paths without
  downloading), assert `skipped == total` and `downloaded == 0` (TR-20a)
- Assert the Dask scheduler used is `"threads"` and not `"processes"` or `"synchronous"`
- Given all downloads returning `None`, assert `RuntimeError` is raised

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Implement AcquireConfig and AcquireSummary dataclasses

**Module:** `kgs_pipeline/acquire.py`
**Classes:** `AcquireConfig`, `AcquireSummary`

**Description:**
Define the configuration and result dataclasses used by the acquire stage.

**`AcquireConfig` fields:**
- `index_path: Path` — path to the lease index file
- `output_dir: Path` — directory for raw downloaded files
- `min_year: int` — minimum year to include (default 2024)
- `max_workers: int` — maximum parallel download threads (capped at 5)
- `request_timeout: int` — HTTP request timeout in seconds
- `monthsave_base_url: str` — base URL for MonthSave page requests
- `sleep_seconds: float` — seconds to sleep per worker after each download (default 0.5)

**`AcquireSummary` fields:**
- `total: int` — total number of lease URLs processed
- `downloaded: int` — number of new files successfully downloaded
- `skipped: int` — number of files skipped because they already existed
- `failed: int` — number of URLs where download returned `None` after a new attempt
- `output_dir: Path` — directory where files were saved

**Design notes:**
- Both classes must use `@dataclass` decorator
- All fields must have type annotations
- `AcquireConfig` must provide sensible defaults for optional fields

**Test cases:**
- Assert `AcquireConfig` can be instantiated with only required fields, with optional
  fields taking their defaults
- Assert `AcquireSummary` fields are readable as attributes after construction

**Definition of done:** Classes are implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Validate downloaded files

**Module:** `kgs_pipeline/acquire.py`
**Function:** `validate_raw_files(output_dir: Path) -> list[str]`

**Description:**
After all downloads complete, validate every file in `output_dir` for basic integrity.
Return a list of filenames that fail validation (empty list if all pass).

**Validation rules (TR-21):**
- File size must be greater than 0 bytes
- File must be readable as UTF-8 text without `UnicodeDecodeError`
- File must contain at least one data row beyond the header line (i.e., at least 2 lines)

**Steps:**
- Iterate over all `.txt` files in `output_dir`
- For each file apply the three validation rules above
- Collect the names of files that fail any rule
- Log a warning for each failing file with the reason
- Return the list of failing filenames

**Error handling:**
- If `output_dir` does not exist, raise `FileNotFoundError`
- Individual file read errors (other than `UnicodeDecodeError`) are caught, logged as
  warnings, and the filename is added to the failures list

**Test cases (TR-21a, TR-21b, TR-21c):**
- Given a directory containing one valid file (> 0 bytes, valid UTF-8, multiple lines),
  assert the returned list is empty
- Given a directory containing one 0-byte file, assert that filename appears in the
  returned list
- Given a directory containing a file with invalid UTF-8 bytes, assert that filename
  appears in the returned list
- Given a directory containing a file with only a header line and no data rows, assert
  that filename appears in the returned list
- Given a nonexistent directory, assert `FileNotFoundError` is raised

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Build environment and configuration setup for acquire

**Files:**
- `kgs_pipeline/__init__.py` — package init (may be empty)
- `config.yaml` — pipeline-wide configuration file
- `pyproject.toml` — package configuration with entry point
- `Makefile` — build targets
- `requirements.txt` — runtime and dev dependencies
- `.gitignore` — version control exclusions

**Description:**
Establish the full build environment, package entry point, and configuration structure
required by the build-env-manifest.

**`config.yaml` structure (acquire section):**
```
acquire:
  index_path: data/external/oil_leases_2020_present.txt
  output_dir: data/raw
  min_year: 2024
  max_workers: 5
  request_timeout: 30
  monthsave_base_url: https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave
  sleep_seconds: 0.5
ingest:
  input_dir: data/raw
  output_dir: data/interim
  output_filename: raw_combined
dask:
  scheduler: local
  n_workers: 4
  threads_per_worker: 1
  memory_limit: "3GB"
  dashboard_port: 8787
logging:
  log_file: logs/pipeline.log
  level: INFO
```

**`pyproject.toml` requirements:**
- Build backend must be explicitly declared (e.g. `hatchling` or `setuptools` with
  `[build-system]` table)
- Package must be pip-installable in editable mode (`pip install -e .`)
- Entry point `cogcc-pipeline` (or `kgs-pipeline`) registered under `[project.scripts]`
  pointing to the pipeline entry point function in `kgs_pipeline/pipeline.py`

**`Makefile` targets:**
- `venv` — create a clean virtual environment
- `install` — bootstrap the package installer then install all dependencies in editable mode
- `acquire` — invoke the acquire stage independently
- `ingest` — invoke the ingest stage independently
- `transform` — invoke the transform stage independently
- `features` — invoke the features stage independently
- `pipeline` — invoke the pipeline entry point for all stages in sequence (must not
  both chain stage targets as dependencies AND invoke the entry point — pick one approach)

**`.gitignore` must exclude:**
- `data/` directory
- `logs/` directory
- `large_tool_results/`
- `conversation_history/`

**Test cases:**
- Assert `pip install -e .` succeeds without errors
- Assert the registered CLI entry point is callable and prints help without error
- Assert `config.yaml` loads without YAML parse errors and all required top-level keys
  are present

**Definition of done:** All files created, package installs cleanly, entry point is
callable, ruff and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.
