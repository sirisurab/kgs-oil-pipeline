# Task Specification: Data Acquisition Component

**Module:** `kgs/src/acquisition.py`
**Component purpose:** Download KGS annual oil production CSV files for years 2020–2025
from the KGS public data portal using parallel async HTTP downloads with retry logic
and file integrity verification.

---

## Overview

The KGS public production data portal hosts annual CSV files at predictable URLs
of the form `https://www.kgs.ku.edu/PRS/petro/productn/oil{YYYY}.csv`.
This component resolves the full set of target URLs, downloads each file
concurrently (bounded parallelism), verifies the downloaded payload, and
writes files to the configured raw data directory.

---

## Task 01: Define configuration dataclass and URL resolver

**Module:** `kgs/src/acquisition.py`
**Function/Class:** `AcquisitionConfig` (dataclass), `resolve_download_urls(config: AcquisitionConfig) -> list[dict]`

**Description:**
Define a frozen dataclass `AcquisitionConfig` that carries all runtime
parameters needed by the acquisition component. Implement a pure function
`resolve_download_urls` that derives the list of target file metadata dicts
from the config.

**AcquisitionConfig fields:**
- `base_url: str` — base portal URL, default `"https://www.kgs.ku.edu/PRS/petro/productn"`
- `years: list[int]` — list of years to download, e.g. `[2020, 2021, 2022, 2023, 2024, 2025]`
- `output_dir: str` — local directory for raw downloads, e.g. `"data/raw"`
- `max_concurrency: int` — maximum simultaneous HTTP connections, default `5`
- `max_retries: int` — maximum retry attempts per file, default `3`
- `retry_backoff_base: float` — exponential backoff base in seconds, default `2.0`
- `request_timeout_seconds: int` — per-request timeout, default `60`
- `checksum_algorithm: str` — hashing algorithm for integrity check, default `"md5"`

**resolve_download_urls behaviour:**
- For each year in `config.years`, construct a URL `{base_url}/oil{year}.csv`
  and an expected local filename `oil{year}.csv`.
- Return a list of dicts, each with keys: `year`, `url`, `filename`, `local_path`
  (local_path = `output_dir / filename`).
- Must not perform any I/O or network calls.

**Error handling:**
- Raise `ValueError` if `years` is empty.
- Raise `ValueError` if any year is outside the range 2000–2030.

**Dependencies:** dataclasses, pathlib

**Test cases:**
- `@pytest.mark.unit` Given a config with years `[2020, 2021]`, assert `resolve_download_urls`
  returns exactly 2 dicts with correct `url` and `local_path` values.
- `@pytest.mark.unit` Given an empty `years` list, assert `ValueError` is raised.
- `@pytest.mark.unit` Given a year of `1899`, assert `ValueError` is raised.
- `@pytest.mark.unit` Assert each returned dict contains keys `year`, `url`, `filename`,
  `local_path`.

**Definition of done:** Function and dataclass are implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 02: Implement async HTTP downloader with retry and backoff

**Module:** `kgs/src/acquisition.py`
**Function:** `download_file(session: aiohttp.ClientSession, file_meta: dict, config: AcquisitionConfig) -> dict`

**Description:**
Implement an async function that downloads a single file described by `file_meta`
(a dict as returned by `resolve_download_urls`). The function writes the file
content to `file_meta["local_path"]` and returns a result dict.

**Behaviour:**
- Use an `asyncio.Semaphore` (value = `config.max_concurrency`) to bound
  concurrent downloads. The semaphore must be created once outside this function
  and passed via the calling context (not re-created per call).
- Perform up to `config.max_retries` attempts. Between attempts use exponential
  backoff: wait `config.retry_backoff_base ** attempt` seconds using `asyncio.sleep`.
- Stream response content in chunks of 64 KB to avoid large memory allocations.
- Create parent directories of `local_path` automatically if they do not exist
  (`parents=True, exist_ok=True`).
- On final failure after all retries, do not raise; instead return a result dict
  with `success=False` and `error` message string.

**Returned result dict keys:**
- `year: int`
- `url: str`
- `local_path: str`
- `success: bool`
- `bytes_downloaded: int` (0 on failure)
- `error: str | None`

**Error handling:**
- Catch `aiohttp.ClientError` and `asyncio.TimeoutError` per attempt; log a
  warning with attempt number before retrying.
- Log an ERROR after all retries are exhausted.
- Never raise exceptions out of this function — all errors are captured in the
  result dict.

**Dependencies:** aiohttp, asyncio, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Mock `aiohttp.ClientSession.get` to return HTTP 200 with
  known byte content; assert `success=True` and `bytes_downloaded` equals
  `len(content)`.
- `@pytest.mark.unit` Mock `aiohttp.ClientSession.get` to raise
  `aiohttp.ClientConnectionError` on all attempts; assert `success=False` and
  `error` is a non-empty string.
- `@pytest.mark.unit` Mock `aiohttp.ClientSession.get` to fail on attempts 1–2
  and succeed on attempt 3; assert `success=True` (retry path works).
- `@pytest.mark.unit` Assert that when `success=False`, the local file is
  not created (no partial file on disk).

**Definition of done:** Function is implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 03: Implement file integrity verifier

**Module:** `kgs/src/acquisition.py`
**Function:** `compute_checksum(file_path: str | Path, algorithm: str = "md5") -> str`
**Function:** `verify_file_integrity(file_path: str | Path, config: AcquisitionConfig) -> dict`

**Description:**
Implement two functions for post-download integrity checking.

`compute_checksum`:
- Opens the file in binary read mode and computes the hash incrementally
  (8 KB blocks) using the `hashlib` module with the given algorithm.
- Returns the hex-digest string.

`verify_file_integrity`:
- Checks that the file at `file_path` exists, has non-zero size, and is a
  valid UTF-8 readable text file (attempt to read first 4 KB and decode).
- Computes and returns the checksum using `config.checksum_algorithm`.
- Returns a dict with keys: `file_path`, `exists: bool`, `size_bytes: int`,
  `is_valid_text: bool`, `checksum: str | None`, `error: str | None`.
- If the file does not exist, return immediately with `exists=False` and
  remaining fields set to safe defaults.

**Error handling:**
- Catch `UnicodeDecodeError` when validating UTF-8; set `is_valid_text=False`
  and populate `error`.
- Catch `OSError` when reading the file.

**Dependencies:** hashlib, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Write a known byte string to a temp file; assert
  `compute_checksum` returns the expected MD5 hex digest.
- `@pytest.mark.unit` Given a non-existent path, assert `verify_file_integrity`
  returns `exists=False` without raising.
- `@pytest.mark.unit` Given a valid UTF-8 CSV temp file, assert `is_valid_text=True`
  and `size_bytes > 0`.
- `@pytest.mark.unit` Given a binary file with invalid UTF-8 bytes, assert
  `is_valid_text=False`.

**Definition of done:** Functions are implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 04: Implement parallel download orchestrator

**Module:** `kgs/src/acquisition.py`
**Function:** `run_acquisition(config: AcquisitionConfig) -> list[dict]`

**Description:**
Implement the top-level async orchestrator that coordinates parallel downloads
for all target files.

**Behaviour:**
- Call `resolve_download_urls(config)` to get the list of file metadata dicts.
- Create a single `asyncio.Semaphore(config.max_concurrency)`.
- Create a single `aiohttp.ClientSession` with a connector that enforces
  `limit=config.max_concurrency` and a total timeout of
  `aiohttp.ClientTimeout(total=config.request_timeout_seconds)`.
- Dispatch all download coroutines concurrently using `asyncio.gather`.
- After all downloads complete, call `verify_file_integrity` for each
  successfully downloaded file.
- Collate and return the full list of result dicts, each extended with
  the integrity verification dict (merged).
- Log a summary: total files attempted, succeeded, failed, total bytes downloaded.

**Synchronous entry point:**
- Provide a synchronous wrapper `acquire(config: AcquisitionConfig) -> list[dict]`
  that calls `asyncio.run(run_acquisition(config))` so callers do not need to
  manage the event loop.

**Error handling:**
- If `output_dir` does not exist, create it before starting downloads.
- If all files fail, log an ERROR but return the list of failure result dicts
  rather than raising.

**Dependencies:** aiohttp, asyncio, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Mock all HTTP calls; assert `acquire` returns a list
  with length equal to `len(config.years)`.
- `@pytest.mark.unit` Assert that the semaphore limits concurrency: mock a
  counter tracking simultaneous active downloads and assert it never exceeds
  `config.max_concurrency`.
- `@pytest.mark.unit` When one file download fails, assert the returned list
  still contains entries for all other years with `success=True`.
- `@pytest.mark.integration` With network access available, call `acquire`
  for a single year (e.g. 2020) against the live KGS URL; assert the downloaded
  file exists on disk, `size_bytes > 0`, and `is_valid_text=True`.

**Definition of done:** Function is implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Design decisions and constraints

- All download logic is async (aiohttp + asyncio). Do NOT use `requests` or
  `concurrent.futures` for HTTP calls in this module.
- The semaphore must be constructed once per `run_acquisition` call, not once
  per file download, to correctly bound concurrency.
- Partial/incomplete files must never persist on disk after a failed download.
  Use a write-to-temp-then-rename pattern: write to `{local_path}.part`, then
  `os.replace` to final path only on success.
- File integrity verification is always performed after a successful download.
  A file that passes download but fails integrity check should have its result
  dict `success` field set to `False` and the partial file removed.
- Logging must use the standard `logging` module. Do not use `print`.
- The `acquire` synchronous wrapper must never be called from within a running
  event loop (guard with a check and raise `RuntimeError` if so).
