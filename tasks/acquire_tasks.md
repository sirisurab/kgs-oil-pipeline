# Acquire Stage — Task Specifications

## Overview

The acquire stage downloads raw KGS oil and gas lease production files from the KGS
public data portal and delivers them to `data/raw/` for the ingest stage. The stage
reads a lease index file (`data/external/oil_leases_2020_present.txt`), filters to
leases active in 2024 or later, deduplicates by URL, and executes a three-step
download workflow for each lease in parallel using Dask's threaded scheduler (I/O-bound
workload per ADR-001). A 0.5-second sleep per worker prevents overloading the KGS
server. Maximum concurrency is 5 workers.

## Architecture constraints (from ADRs)

- **ADR-001**: Acquire is I/O-bound — use Dask threaded scheduler, not distributed.
- **ADR-005**: Task graph must remain lazy until the final write/download operation.
- **ADR-006**: All log output must go to both console and log file. Log configuration
  is read from `config.yaml` — do not configure log handlers inside acquire functions.
- **ADR-007**: HTTP acquisition uses `requests` and `BeautifulSoup` only — no browser
  automation.
- **H1 (stage manifest)**: Scheduler choice must match the I/O-bound workload.
- **H2 (stage manifest)**: A failed download must not produce a file that appears valid
  — a partial or empty file is worse than no file.

## Data flow

```
data/external/oil_leases_2020_present.txt
  → filter to year >= 2024, deduplicate by URL
  → for each unique URL: extract lease_id → fetch MonthSave page → parse download link → download file
  → data/raw/<filename>.txt   (e.g. lp564.txt)
```

## Download workflow (per lease)

1. Extract the lease ID from the URL column value.
   - URL format: `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_ID>`
   - Parse lease ID as the value of query parameter `f_lc`.
2. Make an HTTP GET request to the MonthSave endpoint:
   `https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=<LEASE_ID>`
3. Parse the HTML response with BeautifulSoup to find the anchor tag whose `href`
   contains the string `"anon_blobber.download"`. Extract that full URL.
4. Make an HTTP GET request to the download URL and save the raw response content to
   `data/raw/<p_file_name>` where `p_file_name` is the value of the `p_file_name`
   query parameter in the download URL (e.g. `lp564.txt`).
5. After saving, verify the file is non-empty (size > 0 bytes). If empty, delete the
   file and log a warning — do not leave a zero-byte file on disk.

## Filtering and deduplication rules (from task-writer-kgs.md)

- Read `data/external/oil_leases_2020_present.txt` as a delimiter-separated file.
- Parse the `MONTH-YEAR` column (format `"M-YYYY"`, e.g. `"1-2024"`): split on `"-"`,
  take the last element as year string. Drop rows where the year string is not numeric
  (e.g. `"-1-1965"`, `"0-1966"`). Drop rows where the numeric year < 2024.
- After filtering, deduplicate the remaining rows by the `URL` column — the index
  contains one row per month per lease, so deduplication is required before downloading.
- The result is the set of unique lease URLs to download.

## Idempotency requirement

Before downloading a file, check whether `data/raw/<p_file_name>` already exists and
is non-empty. If it does, skip the download and log an info message. This ensures
running the acquire stage twice does not duplicate or corrupt files (TR-20).

---

## Task A-01: Project scaffolding and configuration

**Module:** `kgs_pipeline/__init__.py`, `config.yaml`, `pyproject.toml`, `Makefile`,
`.gitignore`, `logs/` directory (created at runtime, not committed)

**Description:** Set up the installable package, configuration file, Makefile targets,
and `.gitignore`. This task establishes the foundational build environment that all
subsequent tasks depend on.

### `pyproject.toml` requirements

- Build backend must be explicitly declared (not relying on setuptools defaults).
- Package name: `kgs-pipeline` (importable as `kgs_pipeline`).
- Python version constraint: `>=3.11`.
- Runtime dependencies must include at minimum: `dask[distributed]`, `pandas`,
  `pyarrow`, `requests`, `beautifulsoup4`, `pyyaml`, `bokeh` (Dask dashboard,
  required at runtime per build-env-manifest).
- Development dependencies must include: `pytest`, `pytest-mock`, `ruff`, `mypy`,
  `pandas-stubs`, `types-requests`, `types-beautifulsoup4`.
- Register a CLI entry point: command name `cogcc-pipeline` (or `kgs-pipeline`) mapped
  to `kgs_pipeline.pipeline:main`.

### `config.yaml` requirements

Top-level sections as specified in the build-env-manifest:
```
acquire:
  lease_index: data/external/oil_leases_2020_present.txt
  raw_dir: data/raw
  month_save_url: https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave
  download_base_url: https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download
  min_year: 2024
  max_workers: 5
  sleep_seconds: 0.5
ingest:
  raw_dir: data/raw
  interim_dir: data/interim
  data_dictionary: references/kgs_monthly_data_dictionary.csv
transform:
  interim_dir: data/interim
  processed_dir: data/processed/transform
features:
  processed_dir: data/processed/transform
  output_dir: data/processed/features
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

### `Makefile` requirements

- `env` target: create a Python virtual environment.
- `install` target: bootstrap pip then install all dependencies (runtime + dev) using
  `pip install -e ".[dev]"`.
- `acquire` target: invoke the pipeline entry point for the acquire stage only.
- `ingest` target: invoke the pipeline entry point for the ingest stage only.
- `transform` target: invoke the pipeline entry point for the transform stage only.
- `features` target: invoke the pipeline entry point for the features stage only.
- `pipeline` target: invoke the pipeline entry point for all four stages in sequence;
  must depend on the individual stage targets in order.

### `.gitignore` requirements

Must exclude: `data/`, `logs/`, `large_tool_results/`, `conversation_history/`,
`.venv/`, `__pycache__/`, `*.egg-info/`, `.DS_Store`.

**Definition of done:** `pyproject.toml`, `config.yaml`, `Makefile`, `.gitignore`
exist and are syntactically valid; `pip install -e ".[dev]"` succeeds; `kgs-pipeline
--help` (or equivalent entry point) is callable; ruff and mypy report no errors on
the empty `kgs_pipeline/__init__.py`; `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task A-02: Lease index loader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `load_lease_index(index_path: str, min_year: int) -> list[str]`

**Description:** Load the KGS lease index file, filter to leases active in the target
year range, deduplicate by URL, and return a list of unique lease URLs to download.

### Input

- `index_path`: path to `data/external/oil_leases_2020_present.txt`
- `min_year`: integer minimum year (e.g. `2024`)

### Processing logic (pseudo-code)

```
read the index file as a tab- or comma-separated DataFrame
for each row:
    split MONTH-YEAR on "-" and take the last element as year_str
    if year_str is not numeric → skip row
    if int(year_str) < min_year → skip row
deduplicate the surviving rows by URL column
return the list of unique URL values (as strings), dropping any null URL values
```

### Output

`list[str]` — unique, non-null lease URLs for leases active in year >= `min_year`.

### Error handling

- If `index_path` does not exist, raise `FileNotFoundError` with a descriptive message.
- If the `URL` column is absent from the file, raise `KeyError` with a descriptive message.
- If the `MONTH-YEAR` column is absent from the file, raise `KeyError` with a
  descriptive message.
- Log the count of raw rows, rows surviving the year filter, and unique URLs after
  deduplication at INFO level.

### Test cases (in `tests/test_acquire.py`)

- **Given** a synthetic index DataFrame with rows spanning years 2022–2025 (some with
  non-numeric year components like `"-1-1965"` and `"0-1966"`), **assert** that only
  URLs for rows with a numeric year component >= 2024 are returned.
- **Given** a synthetic index with 3 rows for the same URL (one per month, all 2024),
  **assert** that only 1 URL is returned (deduplication).
- **Given** an index file that does not exist, **assert** `FileNotFoundError` is raised.
- **Given** a file missing the `URL` column, **assert** `KeyError` is raised.
- **Given** a file missing the `MONTH-YEAR` column, **assert** `KeyError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task A-03: Lease ID extractor

**Module:** `kgs_pipeline/acquire.py`
**Function:** `extract_lease_id(url: str) -> str`

**Description:** Parse a KGS lease URL and return the lease ID (`f_lc` query parameter
value).

### Input

- `url`: a URL string of the form
  `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=<LEASE_ID>`

### Processing logic (pseudo-code)

```
parse the URL using urllib.parse.urlparse and urllib.parse.parse_qs
extract the value of query parameter "f_lc"
return as a string
```

### Output

`str` — the lease ID string (e.g. `"1001135839"`).

### Error handling

- If the `f_lc` parameter is absent from the URL, raise `ValueError` with a
  descriptive message including the offending URL.

### Test cases (in `tests/test_acquire.py`)

- **Given** `https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839`,
  **assert** the function returns `"1001135839"`.
- **Given** a URL with no `f_lc` parameter, **assert** `ValueError` is raised.
- **Given** a URL with an empty `f_lc` value, **assert** `ValueError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task A-04: MonthSave page parser

**Module:** `kgs_pipeline/acquire.py`
**Function:** `parse_download_link(html_content: str) -> str`

**Description:** Parse the HTML response from the KGS MonthSave page and return the
full download URL for the lease production data file.

### Input

- `html_content`: raw HTML string from the MonthSave page for a given lease.

### Processing logic (pseudo-code)

```
parse html_content with BeautifulSoup
find all anchor tags whose href attribute contains "anon_blobber.download"
if none found → raise DownloadError
return the href attribute of the first matching anchor tag as a string
```

### Output

`str` — the full download URL (e.g.
`"https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"`).

### Custom exception

Define `DownloadError(Exception)` in `kgs_pipeline/acquire.py`. This exception is
raised whenever a download step fails and cannot be recovered from.

### Error handling

- If no anchor tag with `"anon_blobber.download"` in its href is found, raise
  `DownloadError` with a descriptive message.
- If `html_content` is empty or not parseable as HTML, raise `DownloadError`.

### Test cases (in `tests/test_acquire.py`)

- **Given** HTML containing one anchor with
  `href="...anon_blobber.download?p_file_name=lp564.txt"`, **assert** the full href
  is returned verbatim.
- **Given** HTML with no `anon_blobber.download` link, **assert** `DownloadError`
  is raised.
- **Given** an empty string as `html_content`, **assert** `DownloadError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task A-05: Single-file downloader

**Module:** `kgs_pipeline/acquire.py`
**Function:** `download_lease_file(lease_url: str, raw_dir: str, month_save_base: str, sleep_seconds: float) -> Path | None`

**Description:** Execute the full three-step download workflow for a single lease URL:
fetch the MonthSave page, parse the download link, download and save the file.
Returns the `Path` to the saved file, or `None` if the download was skipped
(file already exists) or failed non-fatally.

### Input

- `lease_url`: the lease's main URL from the index file.
- `raw_dir`: directory where the downloaded file should be saved (`data/raw/`).
- `month_save_base`: the MonthSave base URL from config.
- `sleep_seconds`: seconds to sleep after the download to rate-limit requests.

### Processing logic (pseudo-code)

```
lease_id = extract_lease_id(lease_url)
month_save_url = construct MonthSave URL using month_save_base and lease_id
response = HTTP GET to month_save_url (with timeout)
parse download link from response HTML using parse_download_link
extract p_file_name from the download URL query string
output_path = raw_dir / p_file_name

if output_path already exists and size > 0:
    log INFO "skipping <p_file_name>, already downloaded"
    return output_path

HTTP GET to the download URL (with timeout, stream=True)
write response content to output_path
sleep sleep_seconds
verify output_path size > 0 bytes; if not → delete file and log warning → return None
return output_path
```

### Output

`Path | None` — path to the downloaded file, or `None` on failure/empty file.

### Error handling

- Catch `requests.RequestException` for both HTTP GET calls; log a warning with the
  lease URL and error message; return `None`.
- Catch `DownloadError` (from `parse_download_link`); log a warning; return `None`.
- Never raise an unhandled exception — a single failed download must not abort the
  parallel batch.
- A zero-byte file must be deleted and logged as a warning (H2 stage manifest).

### Test cases (in `tests/test_acquire.py`)

- **Given** mocked HTTP responses returning valid MonthSave HTML and file content,
  **assert** a non-empty file is created at `raw_dir/<p_file_name>` and the Path is
  returned (use `tmp_path` pytest fixture for `raw_dir`).
- **Given** the target file already exists and is non-empty, **assert** no HTTP
  requests are made and the existing file path is returned unchanged (idempotency,
  TR-20a and TR-20b).
- **Given** a network error on the MonthSave GET, **assert** `None` is returned and
  no file is created.
- **Given** a `DownloadError` from `parse_download_link`, **assert** `None` is
  returned and no file is created.
- **Given** a successful download that produces a 0-byte file, **assert** the file
  is deleted and `None` is returned.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task A-06: Parallel acquire runner

**Module:** `kgs_pipeline/acquire.py`
**Function:** `run_acquire(config: dict) -> list[Path]`

**Description:** Orchestrate the full acquire stage: load the lease index, build the
download task graph using Dask's threaded scheduler (I/O-bound per ADR-001), and
execute all downloads in parallel with a maximum of `max_workers` concurrent workers.
Returns a list of successfully downloaded file paths.

### Input

- `config`: the `acquire` section of `config.yaml` as a Python dict, with keys:
  `lease_index`, `raw_dir`, `month_save_url`, `min_year`, `max_workers`,
  `sleep_seconds`.

### Processing logic (pseudo-code)

```
lease_urls = load_lease_index(config["lease_index"], config["min_year"])
log INFO "Found <N> unique lease URLs to process"

ensure raw_dir exists (create if absent)

build a Dask delayed task for each lease_url:
    delayed(download_lease_file)(url, raw_dir, month_save_url, sleep_seconds)

compute all delayed tasks using Dask threaded scheduler with num_workers=max_workers
collect results, filter out None values
log INFO "Downloaded <M> of <N> lease files successfully"
return list of successful Paths
```

### Dask scheduler constraint (ADR-001)

Use `dask.compute(*tasks, scheduler="threads", num_workers=max_workers)` — never use
the distributed scheduler in the acquire stage.

### Output

`list[Path]` — paths of all successfully downloaded files.

### Error handling

- If `load_lease_index` raises, let the exception propagate — it is a fatal error
  that should stop the acquire stage.
- Individual download failures are non-fatal (handled in `download_lease_file`) and
  result in `None` entries that are filtered from the return list.
- Log a summary of successes and failures at INFO level after the compute call.

### Test cases (in `tests/test_acquire.py`)

- **Given** a mocked `load_lease_index` returning 3 URLs and a mocked
  `download_lease_file` returning a valid `Path` for each, **assert** `run_acquire`
  returns a list of 3 Paths.
- **Given** one of the 3 mocked downloads returns `None` (failure), **assert** the
  return list contains only 2 Paths.
- **Given** `raw_dir` does not exist, **assert** it is created before any download
  is attempted.
- **Assert** that the Dask threaded scheduler is used (not distributed) — this can be
  tested by mocking `dask.compute` and inspecting the `scheduler` keyword argument.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task A-07: Acquire stage integration tests

**Module:** `tests/test_acquire.py`

**Description:** Integration-level tests validating the complete acquire workflow
against mocked HTTP endpoints. All external HTTP calls must be mocked — no real
network calls in tests.

### Test cases

- **TR-20 (Acquire idempotency):**
  - Run `run_acquire` with mocked downloads producing 3 files in `tmp_path/raw/`.
  - Run `run_acquire` a second time on the same `tmp_path/raw/`.
  - Assert the file count is the same (3, not 6).
  - Assert file contents are identical between runs.
  - Assert no exception is raised on the second run.

- **TR-21a (File size check):**
  - After a mocked acquire run, assert every file in `raw_dir` has size > 0 bytes.

- **TR-21b (UTF-8 readability):**
  - After a mocked acquire run, assert every file in `raw_dir` is readable as UTF-8
    text without a `UnicodeDecodeError`.

- **TR-21c (Minimum row count):**
  - After a mocked acquire run where mock file content includes a header and at least
    one data row, assert each file contains more than 1 line.

**Definition of done:** All integration test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.
