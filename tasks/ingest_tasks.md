# Ingest Stage Tasks

**Module:** `kgs_pipeline/ingest.py`  
**Test file:** `tests/test_ingest.py`  
**Governing docs:** stage-manifest-ingest.md, boundary-ingest-transform.md, ADR-001,
ADR-002, ADR-003, ADR-004, ADR-005, ADR-006, ADR-007, build-env-manifest.md

---

## Task I-01: Load and validate the data dictionary

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `load_data_dictionary(path: str) -> pd.DataFrame`

**Description:**  
Read `references/kgs_monthly_data_dictionary.csv` into a pandas DataFrame. The path is
supplied by the caller (from config). The returned DataFrame is the authoritative schema
source for all downstream ingest functions — column names, dtypes, nullable flags, and
categorical value sets are derived exclusively from this file (see ADR-003).

**Input:** `path: str` — path to `kgs_monthly_data_dictionary.csv`.  
**Output:** `pd.DataFrame` with columns: `column`, `description`, `dtype`, `nullable`,
`categories`. Every row corresponds to one column of the raw production file.

**Constraints:**
- Schema decisions: see ADR-003. The data dictionary is the sole source of truth; no
  schema inference from data is permitted.
- No retry logic, no caching.

**Test cases:**
- Assert the returned DataFrame contains exactly the columns: `column`, `description`,
  `dtype`, `nullable`, `categories`.
- Assert each value in the `dtype` column is one of the known type labels used in the
  data dictionary (`int`, `float`, `string`, `categorical`).
- Assert no row has a null value in the `column` or `dtype` field.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-02: Build the dtype mapping from the data dictionary

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `build_dtype_map(data_dict: pd.DataFrame) -> dict[str, Any]`

**Description:**  
Produce a mapping from column name to the pandas dtype that must be applied during ingest.
The mapping must be derived entirely from the `dtype` and `nullable` columns of the data
dictionary (see ADR-003). The mapping rules are:

- `dtype="int"`, `nullable="no"` → `"int64"` (standard non-nullable integer)
- `dtype="int"`, `nullable="yes"` → `pd.Int64Dtype()` (nullable integer extension type)
- `dtype="float"`, any nullable → `"float64"` (float64; nulls represented as `np.nan`)
- `dtype="string"`, any nullable → `pd.StringDtype()`
- `dtype="categorical"`, any nullable → `pd.StringDtype()` (see stage-manifest-ingest.md H5)

**Input:** `data_dict: pd.DataFrame` — output of `load_data_dictionary`.  
**Output:** `dict[str, Any]` mapping column name → pandas dtype.

**Constraints:**
- Type mapping rules: see ADR-003. Do not infer types from column name heuristics or
  from data values.
- Float null sentinel is `np.nan`, not `pd.NA` — see ADR-003.
- `pd.NA` is valid only for nullable extension types (`Int64`, `StringDtype`) — see ADR-003.
- No retry logic, no caching.

**Test cases:**
- Given a data dictionary row with `dtype="int"` and `nullable="no"`, assert the mapping
  value is `"int64"`.
- Given a data dictionary row with `dtype="int"` and `nullable="yes"`, assert the mapping
  value is `pd.Int64Dtype()`.
- Given a data dictionary row with `dtype="float"`, assert the mapping value is
  `"float64"`.
- Given a data dictionary row with `dtype="string"`, assert the mapping value is
  `pd.StringDtype()`.
- Given a data dictionary row with `dtype="categorical"`, assert the mapping value is
  `pd.StringDtype()`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-03: Read and schema-enforce a single raw file

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `read_raw_file(file_path: str, dtype_map: dict[str, Any], data_dict: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Read one raw `.txt` file (treated as CSV) from `data/raw/` into a pandas DataFrame.
Apply the dtype mapping from `dtype_map` to enforce schema on all columns. Add a
`source_file` column set to the basename of `file_path` (string dtype). After reading,
filter rows to year >= 2024 by extracting the year component from the MONTH-YEAR column
(format "M-YYYY", split on "-", take last element). Drop rows where the year component is
not numeric, then drop rows where year < 2024. Enforce absent-column handling per
stage-manifest-ingest.md H3:

- Any column declared `nullable=no` in the data dictionary that is absent from the file
  must raise a `ValueError` immediately.
- Any column declared `nullable=yes` that is absent must be added as an all-NA column
  at the correct dtype — do not raise.

Return a schema-conformant `pd.DataFrame` even if it has zero rows (a zero-row DataFrame
with correct column names and dtypes is valid — see stage-manifest-ingest.md H2).

**Input:**
- `file_path: str` — path to one raw `.txt` file
- `dtype_map: dict[str, Any]` — output of `build_dtype_map`
- `data_dict: pd.DataFrame` — output of `load_data_dictionary`

**Output:** `pd.DataFrame` with canonical schema, data-dictionary dtypes, and
`source_file` column. Contains only rows with year >= 2024.

**Constraints:**
- Schema enforcement: see ADR-003 and stage-manifest-ingest.md H1, H2, H3.
- Row filtering logic: see task-writer-kgs.md `<data-filtering>`.
- String operations for year extraction must use vectorized str accessor methods — see
  ADR-002.
- The `source_file` column dtype is `pd.StringDtype()`.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-22):**
- Given a raw file with columns matching the data dictionary, assert the returned DataFrame
  contains all expected columns including `source_file`.
- Given a raw file containing rows with MONTH-YEAR values "1-2023", "6-2024", "3-2025",
  assert only the 2024 and 2025 rows are returned.
- Given a raw file with MONTH-YEAR values "-1-1965" and "0-1966", assert those rows are
  dropped.
- Given a raw file missing a `nullable=no` column, assert `ValueError` is raised.
- Given a raw file missing a `nullable=yes` column, assert the column is present in the
  output as all-NA with the correct dtype, and no exception is raised.
- Assert an empty result (all rows filtered) returns a zero-row DataFrame with correct
  column names and dtypes.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-04: Build the Dask graph for all raw files

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `build_dask_dataframe(raw_dir: str, dtype_map: dict[str, Any], data_dict: pd.DataFrame) -> dd.DataFrame`

**Description:**  
Glob all `.txt` files in `raw_dir`. For each file, create a `dask.delayed` call to
`read_raw_file`. Combine the delayed objects into a Dask DataFrame using `dd.from_delayed`.
Meta must be derived by calling `read_raw_file` on a minimal real input (one of the raw
files, sliced to zero rows) — not constructed from the data dictionary or from an
empty-frame builder function (see ADR-003). The returned Dask DataFrame is lazy; do not
call `.compute()` inside this function (see ADR-005, TR-17).

**Input:**
- `raw_dir: str` — path to `data/raw/`
- `dtype_map: dict[str, Any]` — output of `build_dtype_map`
- `data_dict: pd.DataFrame` — output of `load_data_dictionary`

**Output:** `dd.DataFrame` — lazy Dask DataFrame with canonical schema and
`source_file` column.

**Constraints:**
- Parallelization mechanism: one `dask.delayed` per file, combined via `dd.from_delayed`
  — see stage-manifest-ingest.md H4 (Dask bag must not be used for per-file reads).
- Meta derivation: call the actual `read_raw_file` function on a real input and slice to
  zero rows — see ADR-003. No helper functions, no empty-frame builders.
- The function must not call `.compute()` — see ADR-005 and TR-17.
- Scheduler for CPU-bound stages: see ADR-001 (distributed).
- No retry logic, no caching.

**Test cases (TR-17):**
- Given a directory with 3 raw `.txt` files, assert the return type is `dd.DataFrame`
  (not `pd.DataFrame`).
- Assert the returned Dask DataFrame has the correct column names including `source_file`.
- Assert `.compute()` is not called inside the function (mock or inspect the call graph).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-05: Write interim Parquet output

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `ingest(config: dict) -> None`

**Description:**  
Orchestrate the full ingest stage. Call `load_data_dictionary`, `build_dtype_map`, and
`build_dask_dataframe` to construct the lazy Dask DataFrame. Repartition to
`max(10, min(n, 50))` where `n` is the number of raw source files (see ADR-004). The
repartition step must be the last structural operation before writing (see ADR-004).
Write the result to the interim Parquet directory given by
`config["ingest"]["interim_dir"]` using `dask.dataframe.to_parquet`. Initialize the
distributed scheduler before computing — see ADR-001 and build-env-manifest.md. The task
graph must remain lazy until the write operation (see ADR-005).

**Input:** `config: dict` — full config loaded from `config.yaml`.  
**Output:** None. Side effect: Parquet files written to interim directory.

**Constraints:**
- Scheduler: distributed — see ADR-001.
- Partition count formula: `max(10, min(n, 50))` where `n` is the raw file count — see
  ADR-004.
- Repartition must be the last structural operation before write — see ADR-004.
- Task graph must remain lazy until Parquet write — see ADR-005.
- Output format: Parquet — see ADR-004 and ADR-007.
- Logging: see ADR-006.
- No retry logic, no caching.

**Test cases (TR-17, TR-18, TR-22, TR-24):**
- Assert the function writes at least one Parquet file to the interim directory.
- Assert the written Parquet files are readable by `dd.read_parquet` or `pd.read_parquet`
  without error (TR-18).
- Assert all expected columns are present in every sampled partition: `LEASE_KID`,
  `LEASE`, `API_NUMBER`, `MONTH_YEAR`, `PRODUCT`, `PRODUCTION`, `WELLS`, `OPERATOR`,
  `COUNTY`, `source_file` (TR-22).
- Assert the output satisfies all guarantees in boundary-ingest-transform.md: canonical
  column names, data-dictionary dtypes, nullable columns present (TR-24).
- Run `ingest()` on 2–3 real raw source files with all output paths pointing to
  `tmp_path` — assert Parquet files are written and readable, all columns carry
  data-dictionary dtypes (TR-24).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
