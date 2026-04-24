# Acquire Stage — Task Specifications

All tasks in this file implement the acquire stage for the `kgs_pipeline`
package. Read the following authoritative documents before starting:
- `/agent_docs/ADRs.md` (all ADRs)
- `/agent_docs/build-env-manifest.md`
- `/agent_docs/stage-manifest-acquire.md`
- `/task-writer-kgs.md` (project context, dataset instructions)
- `/test-requirements.xml` (test requirements TR-20, TR-21, TR-27)

All design decisions are governed by the above documents. Where those documents
speak, cite them by name — do not restate. Where a decision is not covered, the
coder may decide but must explain the choice in a module docstring.

## Context

The acquire stage downloads per-lease monthly production files from the KGS
chasm portal. It is driven by the lease index file located at
`data/external/oil_leases_2020_present.txt` (see `<datasets>` block in
`task-writer-kgs.md`). Raw files are written to `data/raw/`.

Scheduler choice, filtering rules, parallelism limit, and polite-scraping
sleep are all specified in `task-writer-kgs.md` under `<instructions>` and
`<data-filtering>`. Do not re-specify those values in code — read them from
`config.yaml` per build-env-manifest.md.

---

## Task A1: Build the download manifest from the lease index

**Module:** `kgs_pipeline/acquire.py`

**Function:** a manifest-builder that reads the lease index file and returns
the deduplicated list of lease records to download.

**Inputs:**
- Path to the lease index file (from `config.yaml` → `acquire`)
- Minimum year threshold (from `config.yaml` → `acquire`)

**Output contract:**
- A structure (list of records or DataFrame) containing at least the lease ID
  and the MainLease URL for every lease whose index contains at least one
  MONTH-YEAR row with year component `>= min_year`.
- Deduplicated by URL (one entry per lease), per the `<instructions>` block
  in `task-writer-kgs.md`.
- Rows whose MONTH-YEAR year component is not numeric are excluded before
  deduplication — see the `<data-filtering>` block in `task-writer-kgs.md`
  for the rule.

**Design decisions governed by docs:**
- Year extraction rule → `task-writer-kgs.md` `<data-filtering>`
- Deduplication key → `task-writer-kgs.md` `<instructions>`
- Data-dictionary column names (LEASE_KID, URL, MONTH-YEAR) →
  `references/kgs_archives_data_dictionary.csv`
- Nullable / non-nullable column handling → ADR-003

**Error handling:**
- Index file missing or unreadable → raise with a clear message naming the
  expected path; do not silently return an empty manifest.

**Test cases (unit):**
- Given a synthetic index fixture with leases in 2020, 2023, 2024, and 2025,
  assert only leases with at least one row at year ≥ 2024 appear in the
  manifest.
- Given duplicate rows (same URL, many months), assert one entry per URL is
  returned.
- Given rows where the year component is not numeric (e.g. "-1-1965",
  "0-1966"), assert those rows are excluded.
- Given a missing index file, assert a clear error is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task A2: Resolve a lease's MonthSave download URL

**Module:** `kgs_pipeline/acquire.py`

**Function:** given a lease ID, returns the direct download URL for the
lease's production text file by fetching the MonthSave HTML page and
extracting the anchor whose href contains `anon_blobber.download`.

**Inputs:**
- Lease ID (string)

**Output contract:**
- The fully-qualified download URL (string). The URL includes a
  `p_file_name` query parameter whose value is the raw filename (e.g.
  `lp564.txt`) to be used when saving the file locally.
- If the MonthSave page returns 200 but contains no matching anchor, the
  function signals "no download available" in a way the caller can detect
  without raising (see "Error handling").

**Design decisions governed by docs:**
- URL template and anchor identification rule → `task-writer-kgs.md`
  `<instructions>` steps 2 and 3.
- HTTP library and HTML parsing library → ADR-007, build-env-manifest
  "HTTP library for acquire". Browser automation is prohibited.

**Error handling:**
- HTTP errors (non-2xx, connection error, timeout) → retry with backoff
  up to a bounded attempt count configured in `config.yaml`, then log a
  warning and signal failure to the caller.
- Missing download anchor → return a sentinel (e.g. `None`) so the caller
  can record the lease as unavailable without aborting the whole batch.
  Acquire-stage output state in `stage-manifest-acquire.md` permits missing
  files — downstream stages handle gaps.

**Test cases (unit):**
- Given a fixture HTML page containing an `anon_blobber.download` anchor,
  assert the function returns the expected URL and the `p_file_name`
  value is extractable from it.
- Given an HTML page with no matching anchor, assert the function returns
  the documented sentinel and does not raise.
- Given an HTTP error that persists through retries (use a mocked
  transport), assert the function returns the documented sentinel and
  logs a warning.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task A3: Download a single lease production file

**Module:** `kgs_pipeline/acquire.py`

**Function:** given a resolved download URL and a target directory,
downloads the file and writes it to the target directory using the
`p_file_name` value from the URL as the filename.

**Inputs:**
- Download URL (from Task A2)
- Target directory (from `config.yaml` → `acquire`, nominally `data/raw/`)

**Output contract:**
- On success: returns the path to the written file. The file exists, is
  non-empty, and is decodable as text — per test requirement TR-21.
- On failure: returns a sentinel (e.g. `None`) and logs a warning. A file
  that cannot be validated must not be left behind on disk — this is the
  shared-state hazard H2 in `stage-manifest-acquire.md`.

**Idempotency requirement:**
- If the target file already exists on disk and is non-empty, the function
  skips the download and returns the existing path. Test requirement TR-20
  defines the exact idempotency contract.

**Design decisions governed by docs:**
- Polite-scraping sleep and concurrency ceiling → `task-writer-kgs.md`
  `<instructions>` (0.5s per worker, max 5 workers). Sleep is applied per
  download worker, not per caller.
- Browser automation prohibited → ADR-007.
- File integrity contract → TR-21.
- Idempotency contract → TR-20.

**Error handling:**
- HTTP failures (non-2xx, timeout, connection error) → retry per config,
  then return sentinel.
- Write succeeds but result is empty or not text-decodable → delete the
  partial file and return sentinel (H2).

**Test cases (unit):**
- Given a mocked HTTP transport that returns valid text content, assert
  the file is written with the `p_file_name` filename and is non-empty.
- Given an existing file at the target path, assert the function does not
  re-download and does not corrupt the existing file (TR-20 a, b, c).
- Given a mocked transport returning empty content, assert no file is
  left behind and a sentinel is returned.
- Given a mocked transport returning non-UTF-8 bytes that fail the text
  validation, assert no file is left behind and a sentinel is returned.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task A4: Orchestrate parallel acquisition of the full manifest

**Module:** `kgs_pipeline/acquire.py`

**Function:** the stage entry point `acquire(config: dict) -> None` that
builds the manifest (A1), resolves download URLs (A2), and downloads files
(A3) in parallel.

**Inputs:**
- The full pipeline config dict (parsed `config.yaml`)

**Output contract:**
- Writes raw lease files to `data/raw/` (or the path configured in
  `config.yaml`).
- Returns nothing. Summary counts (attempted, succeeded, skipped as
  already-present, failed) are logged at INFO level.

**Design decisions governed by docs:**
- Scheduler for acquire must match the I/O-bound workload — see
  stage-manifest-acquire.md hazard H1 and ADR-001 (Dask threaded scheduler
  for acquire). The distributed client initialized in the pipeline entry
  point (build-env-manifest "Dask scheduler initialization") is for the
  CPU-bound stages and is started AFTER acquire completes — acquire must
  not depend on it.
- Concurrency limit and per-worker sleep → `task-writer-kgs.md`
  `<instructions>` (max 5 workers, 0.5s sleep per worker).
- Logging → ADR-006, dual-channel.
- Config sourcing → build-env-manifest "Configuration structure".

**Error handling:**
- Individual lease failures (A2 or A3 sentinel) must not abort the batch.
  They are counted and logged, not raised.
- Only configuration or input-file errors that make the whole run
  impossible (missing index file, missing `acquire` section in config)
  propagate as raised exceptions.

**Test cases:**
- Unit: Given a manifest with 3 leases (all mocked), assert all 3 files
  are written to the target directory and the function returns cleanly.
- Unit: Given a manifest with 3 leases where 1 resolver and 1 downloader
  fail, assert 1 file is written and the function returns cleanly with
  the failures logged.
- Integration (TR-20): Run `acquire(config)` twice with `tmp_path` as the
  output directory (ADR-008); assert the file count is identical after the
  second run and file contents are unchanged.
- Integration (TR-21): After an acquire run to `tmp_path`, assert every
  file in the raw directory has size > 0, is decodable as text, and
  contains at least one row beyond the header line.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.
