# Ingest Stage — Task Specifications

**Module:** `kgs_pipeline/ingest.py`
**Stage manifest:** `agent_docs/stage-manifest-ingest.md`
**Boundary contract:** `agent_docs/boundary-ingest-transform.md`
**ADRs in force:** ADR-001, ADR-003, ADR-004, ADR-005, ADR-006, ADR-007, ADR-008

---

## Task ING-01: Implement data dictionary schema loader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `load_schema(dict_path: str) -> dict[str, dict]`

**Description:**
Read `references/kgs_monthly_data_dictionary.csv` and return a dict keyed by column
name. Each value is a dict with keys `"dtype"`, `"nullable"`, and `"categories"`.
The categories value is either a list of strings (if the data dictionary row has
a non-empty categories cell) or `None`. This schema dict is the single source of
truth for all downstream type enforcement in the ingest stage (ADR-003).

The mapping from data-dictionary `dtype` strings to pandas-compatible types is:
- `"int"` + `nullable=yes` → `"Int64"` (nullable integer)
- `"int"` + `nullable=no` → `"int64"` (non-nullable integer)
- `"float"` + `nullable=yes` → `"Float64"` (nullable float)
- `"float"` + `nullable=no` → `"float64"`
- `"string"` → `"string"` (pandas StringDtype)
- `"categorical"` → `"category"`

Apply this mapping within `load_schema` so callers receive resolved pandas dtype
strings, not raw data dictionary strings.

**Error handling:**
- If `dict_path` does not exist, raise `FileNotFoundError`.
- If required columns (`column`, `dtype`, `nullable`) are absent from the CSV,
  raise `ValueError`.

**Dependencies:** pandas

**Test cases (unit — ADR-003):**
- Assert the function returns a dict with 21 keys matching the column names in
  `kgs_monthly_data_dictionary.csv`.
- Assert that `LEASE_KID` maps to `nullable=no` and dtype `"int64"`.
- Assert that `WELLS` maps to `nullable=yes` and dtype `"Int64"`.
- Assert that `PRODUCT` has categories `["O", "G"]`.
- Assert that `TOWNSHIP` maps to `nullable=yes` and dtype `"Int64"`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-02: Implement raw file reader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: Path) -> pandas.DataFrame`

**Description:**
Read a single KGS lease production `.txt` file (comma-separated, quoted strings)
into a pandas DataFrame. All columns must be read as `object` dtype at this stage —
type enforcement is handled in a separate step. Add a `source_file` column
containing the filename (not the full path) of the source file.

**Error handling:**
- If the file does not exist, raise `FileNotFoundError`.
- If the file cannot be parsed as CSV (e.g. malformed quoting), log a warning with
  the filename and raise `ValueError`.
- An empty file (0 bytes or header-only) must not raise — return an empty DataFrame
  with the expected column names.

**Dependencies:** pandas, pathlib

**Test cases (unit):**
- Given a synthetic `.txt` file with valid KGS-format rows, assert the returned
  DataFrame has a `source_file` column equal to the filename.
- Given an empty file (0 bytes), assert the function returns an empty DataFrame
  without raising.
- Given a file with only a header row, assert the function returns an empty DataFrame
  without raising.
- Given a non-existent path, assert `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-03: Implement date filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_by_year(df: pandas.DataFrame, min_year: int) -> pandas.DataFrame`

**Description:**
Apply the data-filtering constraint from task-writer-kgs.md to a DataFrame that has
a `MONTH-YEAR` column. The filtering logic must:

1. Extract the year component by splitting `MONTH-YEAR` on `"-"` and taking the last
   element.
2. Drop rows where the year element is not a numeric string (e.g. `"-1-1965"` splits
   to last element `"1965"` which is numeric, but `"0-1966"` must also be
   evaluated — the rule is: rows where the year component, after stripping, is not
   convertible to int are dropped).
3. Drop rows where the parsed year integer is less than `min_year`.
4. Return the filtered DataFrame.

Note: The raw KGS lease files contain full production history going back to the
1960s. This filter is the authoritative post-read filter applied before any schema
enforcement.

**Dependencies:** pandas

**Test cases (unit — from data-filtering constraint in task-writer-kgs.md):**
- Given a DataFrame with `MONTH-YEAR` values `"1-2024"`, `"12-2025"`, `"3-2022"`,
  `"-1-1965"`, `"0-1966"`, assert that with `min_year=2024` only `"1-2024"` and
  `"12-2025"` rows are retained.
- Given a DataFrame where all rows are before 2024, assert the returned DataFrame
  is empty.
- Given a DataFrame with no non-numeric year tokens, assert no rows are dropped
  beyond those failing the year >= min_year check.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-04: Implement schema enforcer

**Module:** `kgs_pipeline/ingest.py`
**Function:** `enforce_schema(df: pandas.DataFrame, schema: dict[str, dict]) -> pandas.DataFrame`

**Description:**
Apply the canonical schema from the data dictionary to a DataFrame, enforcing
column presence, naming, and dtypes. The function must:

1. For every column in the schema:
   - If the column is present, cast it to the schema-specified pandas dtype. For
     categorical columns, replace values outside the declared category set with
     `pd.NA` before casting (ADR-003).
   - If the column is absent and `nullable=yes`: add the column as an all-`NA`
     column at the correct dtype. Do not raise.
   - If the column is absent and `nullable=no`: raise `ValueError` immediately.
     These two cases must not be collapsed (stage-manifest-ingest.md H3).
2. Drop columns not present in the schema (e.g. the `URL` column in the lease index
   is not in the monthly data dictionary schema and must not pass downstream).
3. Return the DataFrame with only schema columns in schema-defined order.

**Error handling:**
- Raise `ValueError` if a `nullable=no` column is absent.
- Casting errors for individual columns must be caught, logged at WARNING level, and
  the column set to all-`NA` if nullable, or re-raised if non-nullable.

**Dependencies:** pandas

**Test cases (unit — ADR-003):**
- Given a DataFrame missing a `nullable=yes` column (`LEASE`), assert the returned
  DataFrame contains `LEASE` as an all-`NA` string column.
- Given a DataFrame missing a `nullable=no` column (`LEASE_KID`), assert
  `ValueError` is raised.
- Given a DataFrame with `PRODUCT` containing values `"O"`, `"G"`, and `"X"`, assert
  the returned DataFrame has `"X"` replaced with `pd.NA` before the category cast.
- Given a DataFrame with a column not in the schema (e.g. `URL`), assert it is
  absent from the returned DataFrame.
- Assert that `TWN_DIR` column is returned as pandas `CategoricalDtype` with
  categories `["S", "N"]`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-05: Implement single-file ingest worker

**Module:** `kgs_pipeline/ingest.py`
**Function:** `ingest_file(file_path: Path, schema: dict[str, dict], min_year: int) -> pandas.DataFrame`

**Description:**
Process a single raw lease file into a schema-enforced, year-filtered pandas
DataFrame. The function must:

1. Call `read_raw_file(file_path)` to load the raw data.
2. Call `filter_by_year(df, min_year)` to apply the date filter.
3. Call `enforce_schema(df, schema)` to enforce column names and dtypes.
4. Return the resulting DataFrame.

If the file yields an empty DataFrame after filtering (e.g. all rows pre-2024),
return the empty DataFrame — do not raise.

**Error handling:**
- Propagate `FileNotFoundError` from `read_raw_file`.
- Propagate `ValueError` from `enforce_schema` for missing non-nullable columns.
- Catch and log any other exception, return an empty DataFrame with schema columns.

**Dependencies:** pandas, pathlib, logging

**Test cases (unit):**
- Given a synthetic file with rows spanning 2020–2025, assert the returned DataFrame
  contains only rows with year >= 2024 and all schema columns present.
- Given a synthetic file with all rows before 2024, assert an empty DataFrame is
  returned without raising.
- Given a synthetic file missing the non-nullable `LEASE_KID` column, assert
  `ValueError` propagates.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-06: Implement parallel ingest orchestrator

**Module:** `kgs_pipeline/ingest.py`
**Function:** `run_ingest(config: dict, client) -> dask.dataframe.DataFrame`

**Description:**
Orchestrate parallel ingestion of all raw files using Dask. The function must:

1. Resolve the raw data directory from `config["ingest"]["raw_dir"]`.
2. Glob all `.txt` files in that directory.
3. Load the schema using `load_schema`.
4. Use `dask.bag.from_sequence` over the list of file paths, then map
   `ingest_file` over each path (with `schema` and `min_year` bound from config).
   The Dask distributed scheduler (CPU-bound — ADR-001) is used — the `client`
   argument is the already-initialized Dask distributed `Client` passed in from the
   pipeline entry point (build-env-manifest.md).
5. Convert the resulting bag of DataFrames to a Dask DataFrame using
   `dask.dataframe.from_delayed`.
6. Apply repartition to `max(10, min(n, 50))` where `n` is the number of input
   files (ADR-004).
7. Return the Dask DataFrame without calling `.compute()` (ADR-005, TR-17).

**Error handling:**
- If no `.txt` files are found in `raw_dir`, log a warning and raise `FileNotFoundError`.

**Dependencies:** dask, pathlib, logging

**Test cases (unit — TR-17):**
- Given a config pointing to a temp directory containing 3 synthetic raw files,
  assert the function returns a `dask.dataframe.DataFrame` (not a pandas DataFrame).
- Assert the returned Dask DataFrame has the expected schema columns (TR-22).
- Given a config pointing to an empty directory, assert `FileNotFoundError` is
  raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-07: Implement Parquet writer

**Module:** `kgs_pipeline/ingest.py`
**Function:** `write_interim(ddf: dask.dataframe.DataFrame, config: dict) -> None`

**Description:**
Write the ingested Dask DataFrame to partitioned Parquet files in the interim
directory. The function must:

1. Resolve the output directory from `config["ingest"]["interim_dir"]`; create it
   if absent.
2. Write using `ddf.to_parquet(output_dir, write_index=False, overwrite=True)`.
3. The repartition step was applied in `run_ingest` — do not re-repartition here
   (ADR-004: repartition is the last operation before writing).
4. Log the output path and partition count at INFO level.

**Error handling:**
- If the write fails due to a disk or permission error, log and re-raise.

**Dependencies:** dask, pathlib, logging

**Test cases (integration — TR-18):**
- Given a small synthetic Dask DataFrame (3 partitions), assert that after
  `write_interim` the output directory contains `.parquet` files.
- Assert that each written file is readable by `pandas.read_parquet` without error.
- Assert that the schema of a file read back matches the schema of the input
  DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-08: Implement schema completeness validator

**Module:** `kgs_pipeline/ingest.py`
**Function:** `validate_interim_schema(interim_dir: Path, expected_columns: list[str]) -> None`

**Description:**
Read a sample of up to 3 Parquet partitions from `interim_dir` and assert that
every expected column is present in every sampled partition. If any column is
missing from any partition, raise `ValueError` with the partition path and missing
column name. This implements TR-22.

**Expected columns** (from kgs_monthly_data_dictionary.csv canonical schema plus
the `source_file` column added in ING-02):
`LEASE_KID, LEASE, DOR_CODE, API_NUMBER, FIELD, PRODUCING_ZONE, OPERATOR, COUNTY,
TOWNSHIP, TWN_DIR, RANGE, RANGE_DIR, SECTION, SPOT, LATITUDE, LONGITUDE,
MONTH-YEAR, PRODUCT, WELLS, PRODUCTION, source_file`

**Dependencies:** pandas, pathlib

**Test cases (unit — TR-22):**
- Given a temp directory with 3 synthetic Parquet files each containing all
  expected columns, assert the function returns without raising.
- Given a Parquet file missing the `OPERATOR` column, assert `ValueError` is raised
  naming the missing column.
- Given a Parquet file missing the `source_file` column, assert `ValueError` is
  raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task ING-09: Wire ingest stage entry point

**Module:** `kgs_pipeline/ingest.py`
**Function:** `ingest(config: dict, client) -> None`

**Description:**
Top-level entry point for the ingest stage, called by the pipeline orchestrator.
The function must:

1. Call `run_ingest(config, client)` to produce a Dask DataFrame.
2. Call `write_interim(ddf, config)` to write Parquet output.
3. Call `validate_interim_schema` on the output directory with the canonical
   expected column list.
4. Log a stage-complete summary at INFO level including row count estimate and
   partition count (using graph-level properties — do not call `.compute()` for
   these, use `ddf.npartitions` and `len(ddf)` only if Dask can resolve it lazily,
   otherwise log partition count only).

**Dependencies:** dask, logging

**Test cases (unit — all mocked):**
- Given mocked `run_ingest` returning a synthetic Dask DataFrame and mocked
  `write_interim` that is a no-op, assert `ingest` completes without error and logs
  a stage-complete message.
- Given mocked `validate_interim_schema` raising `ValueError`, assert the exception
  propagates and is not swallowed.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.
