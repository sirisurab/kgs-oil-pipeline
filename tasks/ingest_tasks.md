# Ingest Stage Tasks

**Module:** `kgs_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

Read `stage-manifest-ingest.md`, `boundary-ingest-transform.md`, `ADRs.md`
(ADR-001, ADR-003, ADR-004, ADR-005, ADR-007, ADR-008), and
`build-env-manifest.md` before implementing any task in this file.

The canonical schema and all dtype/nullable mappings are defined in
`references/kgs_monthly_data_dictionary.csv` — this is the authoritative source
per ADR-003. Do not infer schema from data.

---

## Task I-01: Build the dtype map from the data dictionary

**Module:** `kgs_pipeline/ingest.py`
**Function:** `build_dtype_map(dict_path: str | Path) -> dict[str, Any]`

**Description:** Read `references/kgs_monthly_data_dictionary.csv` and construct a
mapping from column name to pandas dtype. Apply the nullable-aware type mapping
required by ADR-003: columns marked `nullable=yes` in the dictionary must use
nullable-aware pandas dtype variants (e.g. `pd.Int64Dtype()` for integers,
`pd.StringDtype()` for strings). Columns marked `nullable=no` must use standard
non-nullable variants. The mapping must cover every column listed in
`kgs_monthly_data_dictionary.csv`. Return the mapping as a plain Python dict.

**Error handling:**
- If `dict_path` does not exist, raise `FileNotFoundError`.
- If any `dtype` value in the dictionary is unrecognised, raise `ValueError` with
  the offending column name and value.

**Dependencies:** pandas, pathlib

**Test cases (unit):**
- Assert the returned dict contains an entry for every column in
  `kgs_monthly_data_dictionary.csv`.
- Assert that `nullable=yes` integer columns map to `pd.Int64Dtype()`, not `int64`.
- Assert that `nullable=no` integer columns map to `int64`.
- Assert that `nullable=yes` string columns map to `pd.StringDtype()`.
- Assert that `float` columns (all nullable in the dictionary) map to `Float64` or
  `float64` as appropriate.
- Assert that `categorical` columns map to `pd.CategoricalDtype()`.
- Given a dict_path that does not exist, assert `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-02: Parse a single raw lease file

**Module:** `kgs_pipeline/ingest.py`
**Function:** `parse_raw_file(file_path: str | Path, dtype_map: dict[str, Any]) -> pd.DataFrame`

**Description:** Read the raw lease `.txt` file at `file_path` as a CSV (the format
is CSV despite the `.txt` extension, per `task-writer-kgs.md`). Apply `dtype_map`
to enforce column types on read. Add a `source_file` column containing the filename
(stem only, not the full path) of `file_path` to support downstream traceability.
Return the resulting pandas DataFrame.

Schema enforcement requirements (per `stage-manifest-ingest.md` H1, H2, H3 and
ADR-003):
- All column names must conform to the canonical names in
  `kgs_monthly_data_dictionary.csv` regardless of how the source file names them.
- Columns marked `nullable=no` that are absent from the file must cause an immediate
  `ValueError` — their absence indicates a structural problem.
- Columns marked `nullable=yes` that are absent from the file must be added as
  all-NA columns at the correct dtype — their absence is expected and valid.
- An empty file (zero data rows) must return a zero-row DataFrame with the full
  canonical column set and correct dtypes, not raise an exception.

**Error handling:**
- Encoding errors must be handled by falling back to `latin-1` after a failed
  `utf-8` attempt; log a warning when the fallback is used.
- If a `nullable=no` column is absent from the parsed file, raise `ValueError`.

**Dependencies:** pandas, pathlib, logging

**Test cases (unit):**
- Given a synthetic CSV with all canonical columns present, assert the returned
  DataFrame has the expected dtypes on all columns per `kgs_monthly_data_dictionary.csv`.
- Given a synthetic CSV missing a `nullable=yes` column, assert the column is
  present in the result as an all-NA column at the correct dtype.
- Given a synthetic CSV missing a `nullable=no` column, assert `ValueError` is raised.
- Given a zero-byte file, assert an empty DataFrame is returned with correct column
  names and dtypes.
- Given a file encoded in `latin-1`, assert it is parsed without error and a warning
  is logged.
- Assert the `source_file` column is present in every return path.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-03: Filter rows to target date range

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_date_range(df: pd.DataFrame, min_year: int = 2024) -> pd.DataFrame`

**Description:** Filter the DataFrame to rows where the year component of
`MONTH-YEAR` is >= `min_year`. The `MONTH-YEAR` column has format `"M-YYYY"` —
extract the year by splitting on `"-"` and taking the last element. Drop rows where
the year component is not numeric (e.g. `"-1-1965"`, `"0-1966"`). This filter
implements the constraint in `task-writer-kgs.md` under `<data-filtering>`. Return
the filtered DataFrame with the same schema as the input.

Row filtering using string operations must be done inside a partition-level function
(ADR-004) — this function operates on a pandas partition, not a Dask DataFrame.

**Error handling:**
- If `MONTH-YEAR` is absent from `df`, raise `KeyError`.
- Malformed year values (non-numeric) must be dropped without raising an exception;
  log the count of dropped rows at DEBUG level.

**Dependencies:** pandas, logging

**Test cases (unit):**
- Given rows with years 2022, 2023, 2024, 2025, assert only 2024 and 2025 rows are
  returned.
- Given rows with malformed `MONTH-YEAR` values `"-1-1965"` and `"0-1966"`, assert
  they are dropped and no exception is raised.
- Given a DataFrame with `min_year=2025`, assert only 2025 rows are returned.
- Given a DataFrame where all rows are before 2024, assert an empty DataFrame is
  returned with the same columns and dtypes as the input.
- Given a DataFrame with `MONTH-YEAR` absent, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I-04: Ingest all raw files in parallel and write interim Parquet

**Module:** `kgs_pipeline/ingest.py`
**Function:** `ingest(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full ingest stage. Discover all `.txt` files in
the raw data directory specified by `config`. For each file, call `parse_raw_file`
and `filter_date_range` inside a Dask `map_partitions` or by constructing a
`dask.dataframe.from_delayed` graph — use the Dask distributed scheduler per
ADR-001 (CPU-bound stage). Concatenate all per-file results into a single Dask
DataFrame. Repartition to `max(10, min(n, 50))` partitions as required by ADR-004
and `boundary-ingest-transform.md`. Write the result to the interim Parquet path
specified in `config` using `dask.dataframe.to_parquet`. Return the Dask DataFrame
(lazy — do not call `.compute()` before returning, per TR-17 and ADR-005).

The task graph must remain lazy until the Parquet write — no intermediate
`.compute()` calls are permitted inside this function.

All path settings must be read from `config`.

**Error handling:**
- If no `.txt` files are found in the raw directory, raise `FileNotFoundError`.
- Per-file parse errors must be logged and that file skipped; they must not abort
  the run.

**Dependencies:** dask, dask.dataframe, pandas, pyarrow, logging, pathlib

**Test cases (unit):**
- Given a config pointing to a directory with synthetic `.txt` files, assert the
  returned object is a `dask.dataframe.DataFrame` (TR-17).
- Assert the return type is `dask.dataframe.DataFrame`, not `pd.DataFrame`.
- Given a config pointing to an empty raw directory, assert `FileNotFoundError`
  is raised.
- Given one malformed file among otherwise valid files, assert the run completes
  with the valid files and logs a warning for the malformed one.

**Test cases (TR-22 — schema completeness, unit):**
- Read at least 3 sample interim Parquet partitions after a mocked ingest run;
  assert all expected columns are present in every partition:
  `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`, `PRODUCTION`,
  `WELLS`, `OPERATOR`, `COUNTY`, `source_file`.
- Assert no expected column is missing or renamed in any sampled partition.

**Test cases (TR-18 — Parquet readability, integration):**
- Assert every Parquet file written to `tmp_path` by `ingest` is readable by a
  fresh `dask.dataframe.read_parquet` call without error.

**Test cases (TR-24 — ingest integration test, integration):**
- Run `ingest()` on 2–3 real raw source files using a config dict with all output
  paths pointing to pytest's `tmp_path`.
- Assert interim Parquet files are written and readable.
- Assert all columns carry data-dictionary dtypes.
- Assert the output satisfies all upstream guarantees in `boundary-ingest-transform.md`.
- Assert all schema-required columns are present.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
