# Ingest Component — Task Specifications

## Overview

The ingest component reads all raw per-lease `.txt` files from `data/raw/`, parses them using
the KGS monthly data dictionary schema, enriches them with lease-level metadata from the
archive index, filters out non-monthly records (yearly summaries and starting cumulative rows),
and writes a unified interim Parquet store partitioned by `LEASE_KID` to `data/interim/`.

All functions return lazy Dask DataFrames. The only `.compute()` call is inside
`write_interim_parquet()`.

**Source module:** `kgs_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`
**Config:** `kgs_pipeline/config.py`

---

## Data Schema Reference

Column names and types are defined by `references/kgs_monthly_data_dictionary.csv`:

| Column         | Type        | Notes                                              |
|----------------|-------------|----------------------------------------------------|
| LEASE KID      | str         | Unique lease ID (note the space in column name)    |
| LEASE          | str         | Lease name                                         |
| DOR_CODE       | str         | Kansas Dept of Revenue ID                          |
| API_NUMBER     | str         | One or more comma-separated well API numbers       |
| FIELD          | str         | Oil/gas field name                                 |
| PRODUCING_ZONE | str         | Producing formation name                           |
| OPERATOR       | str         | Operator of the lease                              |
| COUNTY         | str         | County where lease is located                      |
| TOWNSHIP       | str         | PLSS township number                               |
| TWN_DIR        | str         | Township direction (always "S" in Kansas)          |
| RANGE          | str         | PLSS range number                                  |
| RANGE_DIR      | str         | Range direction (E or W)                           |
| SECTION        | str         | PLSS section (1–36)                                |
| SPOT           | str         | Legal quarter description (NE/NW/SE/SW)            |
| LATITUDE       | float       | NAD 1927 latitude                                  |
| LONGITUDE      | float       | NAD 1927 longitude                                 |
| MONTH-YEAR     | str         | Format "M-YYYY"; Month=0 means yearly, Month=-1 means start cumulative |
| PRODUCT        | str         | "O" = oil, "G" = gas                               |
| WELLS          | int         | Number of wells contributing to the lease          |
| PRODUCTION     | float       | Barrels (oil) or MCF (gas)                         |

---

## Design Decisions & Constraints

- Use `dask.dataframe.read_csv()` with `dtype=str` for safe initial loading of all raw files.
- Filter out records where the month portion of `MONTH-YEAR` is `"0"` (yearly summary) or
  `"-1"` (starting cumulative). Only monthly records (month 1–12) are passed downstream.
- A `source_file` column (string, basename of the raw file) is added for data lineage.
- Lease-level metadata from the archive index (`data/external/oil_leases_2020_present.txt`)
  is left-joined onto the raw data using `LEASE KID` as the join key.
- Output Parquet is written to `data/interim/` partitioned by `LEASE_KID` using
  `dask.dataframe.to_parquet()` with `write_index=False` and `schema="infer"`.
- All functions except the write function must return `dask.dataframe.DataFrame`.
- The interim store schema must be stable — `pyarrow.schema` is passed explicitly at write
  time to avoid per-partition type inference drift.
- Logging via Python's standard `logging` module at DEBUG/INFO/WARNING/ERROR levels.

---

## Task 06: Implement raw file discovery

**Module:** `kgs_pipeline/ingest.py`
**Function:** `discover_raw_files(raw_dir: Path) -> list[Path]`

**Description:**
- Scan `raw_dir` for all files matching the glob pattern `lp*.txt`.
- Return a sorted list of `Path` objects (sorted by filename for deterministic ordering).
- Log the count of discovered files at INFO level.

**Error handling:**
- If `raw_dir` does not exist, raise `FileNotFoundError` with a descriptive message.
- If no files are found, log a WARNING (do not raise; the caller decides whether to proceed).

**Dependencies:** `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a `tmp_path` directory containing 3 files named `lp1.txt`,
  `lp2.txt`, `lp3.txt`, assert the function returns a list of 3 `Path` objects sorted
  alphabetically.
- `@pytest.mark.unit` — Given a `tmp_path` directory containing files with mixed names
  (`lp1.txt`, `other.csv`, `lp2.txt`), assert only the `lp*.txt` files are returned.
- `@pytest.mark.unit` — Given an empty `tmp_path` directory, assert the function returns an
  empty list and logs a WARNING.
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given `data/raw/` containing real downloaded files, assert the
  returned list is non-empty and all paths have the `.txt` suffix.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement raw CSV loader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `load_raw_files(file_paths: list[Path]) -> dask.dataframe.DataFrame`

**Description:**
- Accept a list of raw file `Path` objects.
- Build a list of string glob patterns or pass the file list to
  `dask.dataframe.read_csv()` using `dtype=str` for all columns to avoid premature type
  inference.
- Add a `source_file` column equal to the basename of the source file for each row. Use a
  Dask `map_partitions` call to assign the column from each partition's metadata.
- Return a lazy Dask DataFrame with the columns defined in the monthly data dictionary plus
  the `source_file` column.
- Log the number of partitions and the schema at DEBUG level.

**Error handling:**
- If `file_paths` is empty, raise `ValueError("No raw files provided to load_raw_files")`.
- If a file cannot be parsed (malformed CSV), log a WARNING for that file and skip it.

**Dependencies:** `dask.dataframe`, `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given 2 small in-memory CSV strings written to `tmp_path`, assert the
  function returns a `dask.dataframe.DataFrame` (not `pandas.DataFrame`).
- `@pytest.mark.unit` — Assert all columns from the monthly data dictionary are present in the
  returned DataFrame schema.
- `@pytest.mark.unit` — Assert the `source_file` column is present in the returned DataFrame
  schema.
- `@pytest.mark.unit` — Given an empty `file_paths` list, assert `ValueError` is raised.
- `@pytest.mark.unit` — Assert all column dtypes in the returned Dask DataFrame are `object`
  (string) — verifying that `dtype=str` was applied.
- `@pytest.mark.integration` — Given real files in `data/raw/`, assert the returned DataFrame
  has more than 0 rows when `.compute()` is called in the test.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement non-monthly record filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_monthly_records(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Filter the input Dask DataFrame to retain only rows where the month portion of `MONTH-YEAR`
  is a digit between 1 and 12 (inclusive).
- The `MONTH-YEAR` field uses the format `M-YYYY` or `MM-YYYY`.
- Extract the month portion by splitting on `"-"` and taking the first element.
- Exclude rows where the month portion is `"0"` (yearly summary) or `"-1"` (starting
  cumulative) or any non-integer value.
- Return a lazy Dask DataFrame with only monthly records.
- Log the approximate percentage of records filtered out at INFO level (use the Dask
  `len()` approximation if exact count is expensive).

**Error handling:**
- If the `MONTH-YEAR` column is absent, raise `KeyError` with a descriptive message.
- Rows where `MONTH-YEAR` is null are dropped with a WARNING logged.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with rows for months 0, -1, 1, 6, 12, assert only
  rows with months 1, 6, 12 are retained.
- `@pytest.mark.unit` — Given a DataFrame where all `MONTH-YEAR` values are `"0-2023"` (yearly
  summary), assert the returned DataFrame is empty.
- `@pytest.mark.unit` — Given a DataFrame where `MONTH-YEAR` contains nulls, assert those rows
  are excluded.
- `@pytest.mark.unit` — Given a DataFrame without a `MONTH-YEAR` column, assert `KeyError` is
  raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement lease metadata enrichment

**Module:** `kgs_pipeline/ingest.py`
**Function:**
`enrich_with_metadata(df: dask.dataframe.DataFrame, index_file: Path) -> dask.dataframe.DataFrame`

**Description:**
- Load the lease archive index from `index_file` using `pandas.read_csv()` (not Dask — the
  metadata file is small enough for in-memory loading).
- Select only the metadata columns not already present in the raw monthly data files, plus
  `LEASE KID` as the join key. Metadata-only columns include: `DOR_CODE`, `FIELD`,
  `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`,
  `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`.
- Rename `LEASE KID` to `LEASE_KID` in both the metadata and the raw DataFrame (strip the
  space) for safe Parquet column naming.
- Perform a Dask left-join of `df` on the metadata pandas DataFrame using
  `df.merge(meta_df, on="LEASE_KID", how="left")`.
- Return the enriched lazy Dask DataFrame.
- Log the number of metadata rows joined at DEBUG level.

**Error handling:**
- If `index_file` does not exist, raise `FileNotFoundError`.
- If `LEASE KID` is absent from the index file, raise `KeyError`.
- If after the join more than 5% of rows have null `OPERATOR` values, log a WARNING.

**Dependencies:** `dask.dataframe`, `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with 3 `LEASE_KID` values and a matching
  pandas metadata DataFrame, assert the returned DataFrame contains the `OPERATOR` column.
- `@pytest.mark.unit` — Given a `LEASE_KID` that is absent from the metadata, assert the
  corresponding rows still appear in the output (left-join semantics) with null metadata
  columns.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a metadata file path that does not exist, assert
  `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Assert the column `LEASE KID` (with space) does not appear in the
  output schema — only `LEASE_KID` (with underscore).
- `@pytest.mark.integration` — Given the real `LEASE_INDEX_FILE`, assert that the returned
  DataFrame has an `OPERATOR` column and that fewer than 20% of rows have null `OPERATOR`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement interim Parquet writer

**Module:** `kgs_pipeline/ingest.py`
**Function:** `write_interim_parquet(df: dask.dataframe.DataFrame, output_dir: Path) -> None`

**Description:**
- Accept an enriched, filtered, lazy Dask DataFrame and write it to `output_dir` as a
  partitioned Parquet dataset.
- Partition by `LEASE_KID` using `df.to_parquet(output_dir, partition_on=["LEASE_KID"], ...)`.
- Use `write_index=False`, `engine="pyarrow"`, and `compression="snappy"`.
- Pass an explicit `pyarrow.Schema` derived from the expected column list and dtypes to
  prevent type-inference drift across partitions.
- This is the only function in the ingest module that triggers `.compute()` (implicitly via
  `to_parquet`).
- Create `output_dir` if it does not exist.
- Log start and completion times with row count at INFO level.

**Error handling:**
- If `output_dir` cannot be created (permissions), let the `OSError` propagate.
- If `df` is empty after filtering, log a WARNING and return without writing.

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with 3 partitions and a `LEASE_KID`
  column, assert that `write_interim_parquet` creates at least 1 Parquet file in `tmp_path`.
- `@pytest.mark.unit` — Assert that each written Parquet file is readable by
  `pandas.read_parquet()` without error.
- `@pytest.mark.unit` — Assert that partition directories are named by `LEASE_KID` value.
- `@pytest.mark.unit` — Given an empty Dask DataFrame, assert the function returns without
  writing any files and logs a WARNING.
- `@pytest.mark.integration` — Run the full ingest pipeline on real data and assert that
  `data/interim/` contains at least one Parquet file that is readable.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 11: Implement ingest pipeline entry-point

**Module:** `kgs_pipeline/ingest.py`
**Function:**
`run_ingest_pipeline(raw_dir: Path | None = None, index_file: Path | None = None, output_dir: Path | None = None) -> None`

**Description:**
- Synchronous entry-point that orchestrates the ingest workflow in order:
  1. `discover_raw_files(raw_dir)` → `file_paths`
  2. `load_raw_files(file_paths)` → `raw_ddf`
  3. `filter_monthly_records(raw_ddf)` → `filtered_ddf`
  4. `enrich_with_metadata(filtered_ddf, index_file)` → `enriched_ddf`
  5. `write_interim_parquet(enriched_ddf, output_dir)`
- Resolves default values from `config.py` if arguments are `None`.
- Logs total pipeline elapsed time at INFO level.

**Error handling:**
- Re-raises `FileNotFoundError` from `discover_raw_files` or `enrich_with_metadata` with
  additional context.
- Any unhandled exception is logged at CRITICAL level before re-raising.

**Dependencies:** `pathlib`, `logging`, `kgs_pipeline.config`, `kgs_pipeline.ingest`

**Test cases:**
- `@pytest.mark.unit` — Patch each of the four sub-functions; assert they are called in order
  with the correct arguments.
- `@pytest.mark.unit` — Patch `discover_raw_files` to raise `FileNotFoundError`; assert
  `run_ingest_pipeline` re-raises `FileNotFoundError`.
- `@pytest.mark.unit` — Assert that `None` arguments resolve to config defaults without
  raising.
- `@pytest.mark.integration` — Run `run_ingest_pipeline()` end-to-end on real raw data; assert
  `data/interim/` is non-empty and at least one partition is readable by
  `pandas.read_parquet()`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
