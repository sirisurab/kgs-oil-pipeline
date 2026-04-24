# Ingest Stage — Task Specifications

All tasks in this file implement the ingest stage for the `kgs_pipeline`
package. Read the following authoritative documents before starting:
- `/agent_docs/ADRs.md`
- `/agent_docs/build-env-manifest.md`
- `/agent_docs/stage-manifest-ingest.md`
- `/agent_docs/boundary-ingest-transform.md`
- `/task-writer-kgs.md` (dataset, instructions, `<data-filtering>`)
- `/test-requirements.xml` (TR-17, TR-18, TR-22, TR-24, TR-27)
- `references/kgs_monthly_data_dictionary.csv` — the authoritative schema
  for all per-lease files downloaded by the acquire stage (see
  `<data-dictionaries>` in `task-writer-kgs.md`).

All design decisions are governed by those documents. Where those documents
speak, cite them by name — do not restate.

## Context

The acquire stage writes per-lease text files to `data/raw/`. Each file is
CSV-compatible despite the `.txt` extension (`<instructions>` in
`task-writer-kgs.md`). The ingest stage reads all raw files, enforces the
canonical schema defined in `kgs_monthly_data_dictionary.csv`, filters
rows per `<data-filtering>`, and writes partitioned interim Parquet to
`data/interim/` (path configured in `config.yaml`).

---

## Task I1: Load and expose the monthly data dictionary as a schema object

**Module:** `kgs_pipeline/ingest.py` (or a supporting module imported by it)

**Function:** loads `references/kgs_monthly_data_dictionary.csv` and exposes
the schema — canonical column names, data-dictionary dtypes, nullable flags,
and categorical allowed-values — in a form that the ingest, transform, and
features stages consume.

**Inputs:**
- Path to the data dictionary CSV (from `config.yaml`)

**Output contract:**
- A representation of the schema usable by all later functions for:
  - Canonical column names (order preserved)
  - Pandas dtype per column, mapped from the dictionary `dtype` and
    `nullable` columns per ADR-003 (nullable=yes → nullable-aware
    variant; nullable=no → standard variant).
  - Categorical allowed values (parsed from the `categories` column, split
    on `|`) for columns whose dtype is `categorical`.

**Design decisions governed by docs:**
- Data-dictionary-as-source-of-truth → ADR-003. The schema must never be
  inferred from a raw file.
- Nullable-aware dtype mapping rule → ADR-003 (non-nullable int must not
  collapse to float when data contains NaNs).

**Test cases (unit):**
- Given the project dictionary CSV, assert the returned schema contains
  every column listed in the dictionary in the same order.
- Assert columns with `nullable=yes` map to a nullable-aware pandas dtype
  and columns with `nullable=no` map to the non-nullable variant.
- Assert `PRODUCT` exposes the allowed-value set {"O", "G"} and
  `TWN_DIR` exposes {"S", "N"}.

**Definition of done:** Function is implemented, test cases pass, ruff and
mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task I2: Read a single raw lease file into a typed pandas DataFrame

**Module:** `kgs_pipeline/ingest.py`

**Function:** given a path to one raw lease file and the schema object
from Task I1, returns a pandas DataFrame with canonical column names and
data-dictionary dtypes, plus a `source_file` column identifying the file
of origin.

**Inputs:**
- Path to a single raw file in `data/raw/`
- Schema object (from I1)

**Output contract (every return path must satisfy this contract — including
empty, filtered, and error-free cases — per the Task Decomposition Constraint):**
- All canonical columns are present and in canonical order.
- Every column carries its data-dictionary dtype per ADR-003, including
  the `source_file` column (string).
- A zero-row DataFrame is valid and acceptable — but it must still have
  the full schema (stage-manifest-ingest H2).
- Row filter: rows whose `MONTH-YEAR` year component is `< 2024` or is
  not numeric are dropped. The filtering rule is defined in
  `<data-filtering>` in `task-writer-kgs.md` — do not restate it here.
- Categorical columns carry only declared category values; out-of-set
  values are replaced with the appropriate null sentinel before casting,
  per ADR-003.

**Absent-column handling (stage-manifest-ingest H3):**
- `nullable=yes` column absent from source → added as all-NA column at
  the correct dtype.
- `nullable=no` column absent from source → raise immediately.
- These two cases must not be collapsed into one handler.

**Expected columns in the output (canonical names):** LEASE_KID, LEASE,
DOR_CODE, API_NUMBER, FIELD, PRODUCING_ZONE, OPERATOR, COUNTY, TOWNSHIP,
TWN_DIR, RANGE, RANGE_DIR, SECTION, SPOT, LATITUDE, LONGITUDE, MONTH-YEAR,
PRODUCT, WELLS, PRODUCTION, plus `source_file`. (This matches the column
set TR-22 asserts on — `source_file` is added by ingest.) The canonical
dtype for each of the dictionary columns is derived from
`kgs_monthly_data_dictionary.csv` per ADR-003 — do not enumerate dtypes
in this task spec.

**Test cases (unit):**
- Given a small synthetic CSV with all canonical columns and mixed-year
  rows, assert only year-≥-2024 rows are returned and each column carries
  its data-dictionary dtype.
- Given a CSV missing a `nullable=yes` column, assert the returned
  DataFrame contains that column as all-NA with the correct dtype.
- Given a CSV missing a `nullable=no` column (e.g. `LEASE_KID` or
  `MONTH-YEAR`), assert the function raises.
- Given a CSV containing a `PRODUCT` value outside {"O","G"}, assert the
  out-of-set value is replaced with the declared null sentinel before
  categorical casting.
- Given a CSV whose every row is pre-2024, assert an empty DataFrame is
  returned that still has the full canonical schema with correct dtypes
  (H2 — empty but schema-conformant).
- Given a CSV containing a row with `MONTH-YEAR` = `"-1-1965"` or
  `"0-1966"`, assert the row is dropped.

**Definition of done:** Function is implemented, test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task I3: Ingest the full raw directory into a Dask DataFrame and write
## partitioned Parquet to `data/interim/`

**Module:** `kgs_pipeline/ingest.py`

**Function:** the stage entry point `ingest(config: dict) -> None`.

**Inputs:**
- Pipeline config dict

**Behavior (output contract is on disk — Parquet at the configured interim
path):**
- Reads every raw file in the configured raw directory via the I2 function.
- Combines per-file DataFrames into a single Dask DataFrame.
- Writes the result to the configured interim Parquet path.

**Output guarantees at stage exit (boundary-ingest-transform.md — upstream
guarantees):**
- Column names conform to the canonical schema defined in
  `kgs_monthly_data_dictionary.csv`.
- All columns carry data-dictionary dtypes.
- Nullable absent columns are filled with NA at the correct dtype.
- Partition count is `max(10, min(n, 50))` per ADR-004, applied as the
  last operation before writing per ADR-004.
- Output is unsorted (boundary-ingest-transform.md).

**Design decisions governed by docs:**
- Parallelization mechanism → ADR-001 (Dask distributed scheduler for
  CPU-bound stages; ingest is CPU-bound). Standard-library multiprocessing
  is not the chosen mechanism — the stage uses Dask.
- Scheduler client reuse → build-env-manifest "Dask scheduler
  initialization" (stages reuse the pipeline's client, do not start their
  own).
- Laziness → ADR-005. No `.compute()` calls inside the stage before the
  final write.
- Partition count formula and position → ADR-004.
- Schema enforcement on every partition — including empty partitions →
  stage-manifest-ingest H2.
- Inter-stage format → ADR-004 (Parquet only).
- Logging → ADR-006.

**Error handling:**
- A single unreadable file must not abort the stage. Log and continue.
  However, every returned DataFrame must still satisfy the schema
  contract (H2) — an empty, fully-typed DataFrame is an acceptable
  failure output for a single file; a missing file entirely is not
  acceptable in the merged Dask DataFrame (just skip it).
- Config errors (missing sections, missing paths) propagate as raised
  exceptions.

**Test cases:**
- Unit: Given a small directory of 3 synthetic raw files (each with 5–10
  rows, all post-2024), run the ingest function against a `tmp_path`
  interim directory; assert the output Parquet is readable (TR-18), every
  expected column is present, and every column carries its
  data-dictionary dtype.
- Unit (TR-17): Assert that intermediate functions operating on the Dask
  graph return Dask DataFrames rather than pandas DataFrames (no internal
  `.compute()`).
- Unit (TR-22): Read a sample of ≥3 partitions from the interim output
  and assert every partition contains these columns: LEASE_KID, LEASE,
  API_NUMBER, MONTH_YEAR (or `MONTH-YEAR` per the data-dictionary name —
  see note below), PRODUCT, PRODUCTION, WELLS, OPERATOR, COUNTY,
  source_file. No expected column is missing or renamed in any sampled
  partition.

  Note on column-name form: TR-22 lists `MONTH_YEAR` while the data
  dictionary uses `MONTH-YEAR`. The canonical name is whatever
  `kgs_monthly_data_dictionary.csv` defines — ADR-003 makes the
  dictionary the source of truth. If the TR-22 test asserts on the
  dictionary-form name, the test passes; the coder must use the
  dictionary name and not silently rename.
- Integration (TR-24): Run `ingest(config)` on 2–3 real raw files (from
  the acquire output or a checked-in fixture) using a config dict whose
  interim path points to `tmp_path` (ADR-008). Assert interim Parquet
  is written and readable, every column carries its data-dictionary
  dtype, the output satisfies all upstream guarantees in
  `boundary-ingest-transform.md`, and all schema-required columns are
  present.
- Integration (TR-27) end-to-end: see pipeline_tasks.md.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.
