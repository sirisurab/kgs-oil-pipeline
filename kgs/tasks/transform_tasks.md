# Transform (Clean) Component — Task Specifications

## Overview

The transform component loads the interim Parquet store written by the ingest component,
applies comprehensive data cleaning (type casting, date parsing, column renaming, deduplication,
physical bound validation), explodes comma-separated API numbers into per-well rows, adds
derived columns for unit labelling, sorts records chronologically per well, and writes the
cleaned, per-well data to `data/processed/` partitioned by `well_id`.

All functions except the writer return lazy Dask DataFrames. The only `.compute()` call is
inside `write_processed_parquet()`.

**Source module:** `kgs_pipeline/clean.py`
**Test file:** `tests/test_clean.py`
**Config:** `kgs_pipeline/config.py`

---

## Data Schema After Transformation

All column names are renamed to `snake_case`. After transformation the processed Parquet
schema is:

| Column              | Type              | Notes                                                     |
|---------------------|-------------------|-----------------------------------------------------------|
| well_id             | str               | Single API number (one row per well per month)           |
| lease_kid           | str               | Unique KGS lease ID                                       |
| lease_name          | str               | Lease name                                                |
| dor_code            | str               | Kansas Dept of Revenue ID                                 |
| field_name          | str               | Oil/gas field name                                        |
| producing_zone      | str               | Producing formation                                       |
| operator            | str               | Operator of the lease                                     |
| county              | str               | County name                                               |
| township            | str               | PLSS township                                             |
| twn_dir             | str               | Township direction                                        |
| range_num           | str               | PLSS range number                                         |
| range_dir           | str               | Range direction                                           |
| section             | str               | PLSS section                                              |
| spot                | str               | Legal quarter description                                 |
| latitude            | float32           | NAD 1927 latitude                                         |
| longitude           | float32           | NAD 1927 longitude                                        |
| production_date     | datetime64[ns]    | First day of the production month, parsed from MONTH-YEAR |
| product             | str               | "O" (oil) or "G" (gas)                                   |
| wells               | Int16             | Number of wells (nullable integer)                        |
| production          | float64           | Production volume (BBL for oil, MCF for gas)              |
| unit                | str               | "BBL" (oil) or "MCF" (gas)                                |
| outlier_flag        | bool              | True if production > 50,000 BBL/month for a single well  |
| source_file         | str               | Raw filename for data lineage                             |

---

## Design Decisions & Constraints

- Zero production (value `0.0`) is a valid measurement and must NOT be converted to `NaN`.
  Only truly missing/null values are represented as `NaN`.
- Negative production values are physically impossible — set them to `NaN` and log a WARNING
  with the count of affected rows.
- Oil production above 50,000 BBL/month for a single well is flagged as a potential unit error
  with `outlier_flag = True` but the value is retained (not replaced).
- `MONTH-YEAR` format is `M-YYYY` or `MM-YYYY`. Parse to a `datetime64[ns]` by interpreting
  the value as the first day of that month.
- The `API_NUMBER` column may contain a comma-separated list of well API numbers. Explode this
  into one row per well. If `API_NUMBER` is null or empty for a lease row, assign `well_id` to
  the `lease_kid` prefixed with `"LEASE_"` as a synthetic identifier.
- Column renaming map (raw → processed):
  - `LEASE KID` → `lease_kid`
  - `LEASE` → `lease_name`
  - `DOR_CODE` → `dor_code`
  - `API_NUMBER` → `well_id` (after explosion)
  - `FIELD` → `field_name`
  - `PRODUCING_ZONE` → `producing_zone`
  - `OPERATOR` → `operator`
  - `COUNTY` → `county`
  - `TOWNSHIP` → `township`
  - `TWN_DIR` → `twn_dir`
  - `RANGE` → `range_num`
  - `RANGE_DIR` → `range_dir`
  - `SECTION` → `section`
  - `SPOT` → `spot`
  - `LATITUDE` → `latitude`
  - `LONGITUDE` → `longitude`
  - `MONTH-YEAR` → `production_date`
  - `PRODUCT` → `product`
  - `WELLS` → `wells`
  - `PRODUCTION` → `production`
- Deduplication key: `[well_id, production_date, product]` — keep the first occurrence.
- Output is written to `data/processed/` partitioned by `well_id`.
- An explicit `pyarrow.Schema` is passed at write time.
- All functions must return `dask.dataframe.DataFrame` (verified in tests via type check).
- Logging via Python's standard `logging` module at DEBUG/INFO/WARNING/ERROR levels.

---

## Task 12: Implement interim Parquet loader

**Module:** `kgs_pipeline/clean.py`
**Function:** `load_interim_parquet(interim_dir: Path) -> dask.dataframe.DataFrame`

**Description:**
- Load the partitioned Parquet dataset from `interim_dir` using `dask.dataframe.read_parquet()`.
- Return a lazy Dask DataFrame.
- Log the number of partitions and detected schema at DEBUG level.

**Error handling:**
- If `interim_dir` does not exist or contains no Parquet files, raise `FileNotFoundError` with
  a descriptive message.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Write a minimal Parquet file to `tmp_path` and assert the function
  returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (not pandas).
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given real `data/interim/` data, assert the returned DataFrame
  has a non-zero partition count.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 13: Implement column renaming and snake_case standardisation

**Module:** `kgs_pipeline/clean.py`
**Function:** `rename_columns(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Apply the column renaming map defined in the design decisions section above.
- Do not rename `source_file` — it is already lowercase.
- Return a lazy Dask DataFrame with renamed columns.
- Raise `KeyError` if any expected source column is missing from the input schema.

**Error handling:**
- If any column in the renaming map is absent from `df`, raise `KeyError` listing the missing
  columns.

**Dependencies:** `dask.dataframe`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with the raw column names, assert the output
  contains `lease_kid`, `lease_name`, `production_date`, `field_name`, `well_id` (pre-
  explosion, still `api_number` at this stage — note: rename `API_NUMBER` → `api_number` here;
  explosion into `well_id` happens in a later task).
- `@pytest.mark.unit` — Assert `LEASE KID` (with space) is absent from the output schema.
- `@pytest.mark.unit` — Assert `MONTH-YEAR` is absent from the output schema.
- `@pytest.mark.unit` — Given a DataFrame missing the `MONTH-YEAR` column, assert `KeyError` is
  raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 14: Implement date parsing

**Module:** `kgs_pipeline/clean.py`
**Function:**
`parse_production_date(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Accept the renamed DataFrame (with column `production_date` still holding the raw string
  `M-YYYY` or `MM-YYYY`).
- Parse the `production_date` string column into `datetime64[ns]` by interpreting each value
  as the first day of that month (e.g., `"3-2021"` → `2021-03-01`).
- Use `pandas.to_datetime()` with format `"%m-%Y"` applied via `map_partitions`.
- Rows where `production_date` cannot be parsed must have `NaT` assigned and a WARNING is
  logged with the count of unparseable rows.
- Return a lazy Dask DataFrame.

**Error handling:**
- If the `production_date` column is absent, raise `KeyError`.
- Parsing failures are coerced to `NaT` (not raised as exceptions).

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given `"3-2021"`, assert the parsed date is
  `pandas.Timestamp("2021-03-01")`.
- `@pytest.mark.unit` — Given `"12-2023"`, assert the parsed date is
  `pandas.Timestamp("2023-12-01")`.
- `@pytest.mark.unit` — Given an unparseable string `"0-2023"`, assert the result is `NaT`.
- `@pytest.mark.unit` — Assert the `production_date` column dtype after parsing is
  `datetime64[ns]`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 15: Implement dtype casting

**Module:** `kgs_pipeline/clean.py`
**Function:** `cast_dtypes(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Cast columns to the target dtypes defined in the processed schema:
  - `latitude`, `longitude` → `float32`
  - `production` → `float64` (coerce errors to `NaN`)
  - `wells` → `Int16` (nullable integer; coerce errors to `pd.NA`)
  - All remaining string columns → remain as `object` (no change needed)
- For `production`: if the raw string is empty or non-numeric, coerce to `NaN`.
- Preserve `0.0` as `0.0` — do not coerce to `NaN`.
- Return a lazy Dask DataFrame.

**Error handling:**
- Type coercion errors are handled with `errors="coerce"` — log WARNING with counts.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a row with `production="123.45"`, assert the cast value is
  `123.45` as `float64`.
- `@pytest.mark.unit` — Given a row with `production="0"`, assert the cast value is `0.0`
  (not `NaN`).
- `@pytest.mark.unit` — Given a row with `production=""` (empty string), assert the cast value
  is `NaN`.
- `@pytest.mark.unit` — Given a row with `production="-5.0"`, assert the cast value is `-5.0`
  as `float64` (negative filtering is a separate step).
- `@pytest.mark.unit` — Assert `latitude` dtype is `float32` after casting.
- `@pytest.mark.unit` — Assert `wells` dtype is `Int16` after casting.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 16: Implement API number explosion (per-well row creation)

**Module:** `kgs_pipeline/clean.py`
**Function:** `explode_api_numbers(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- The `api_number` column may contain a comma-separated list of well API numbers.
- Split on `","` and strip whitespace from each element.
- Explode the list so each row contains exactly one well API number in a new `well_id` column.
- Drop the original `api_number` column from the result.
- Rows where `api_number` is null or empty string: assign `well_id = "LEASE_" + lease_kid`
  as a synthetic identifier.
- Use `map_partitions` with a pandas `explode` operation to preserve Dask laziness.
- Return a lazy Dask DataFrame.

**Error handling:**
- If neither `api_number` nor `lease_kid` is present, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a row with `api_number="1500112345, 1500112346"`, assert the
  function produces 2 rows with `well_id = "1500112345"` and `well_id = "1500112346"`.
- `@pytest.mark.unit` — Given a row with `api_number="1500112345"` (single value), assert the
  function produces exactly 1 row.
- `@pytest.mark.unit` — Given a row with `api_number=None` and `lease_kid="1001135839"`,
  assert `well_id = "LEASE_1001135839"`.
- `@pytest.mark.unit` — Assert `api_number` column is absent from the output schema.
- `@pytest.mark.unit` — Assert `well_id` column is present in the output schema.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 17: Implement physical bound validation

**Module:** `kgs_pipeline/clean.py`
**Function:** `validate_physical_bounds(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Validate that `production` values satisfy the following domain rules:
  1. Production cannot be negative: set negative values to `NaN` and log a WARNING with the
     count of affected rows.
  2. Oil production (`product == "O"`) above 50,000 BBL/month for a single well is a likely
     unit error: set `outlier_flag = True` for those rows (do not replace the value).
  3. Gas production (`product == "G"`) above 1,000,000 MCF/month is similarly flagged.
  4. `latitude` must be between 36.0 and 40.5 (Kansas bounding box). Values outside this range
     are set to `NaN` with a WARNING logged.
  5. `longitude` must be between -102.5 and -94.0 (Kansas bounding box). Values outside this
     range are set to `NaN` with a WARNING logged.
- Initialize `outlier_flag = False` for all rows before applying checks.
- Return a lazy Dask DataFrame with the `outlier_flag` column added.

**Error handling:**
- If `production` or `product` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a row with `production=-10.0`, assert the output `production` is
  `NaN`.
- `@pytest.mark.unit` — Given a row with `production=0.0` and `product="O"`, assert the output
  `production` is `0.0` (zero is valid).
- `@pytest.mark.unit` — Given a row with `production=75000.0` and `product="O"`, assert
  `outlier_flag = True` and `production` value is unchanged at `75000.0`.
- `@pytest.mark.unit` — Given a row with `production=2000000.0` and `product="G"`, assert
  `outlier_flag = True`.
- `@pytest.mark.unit` — Given a row with `latitude=45.0`, assert the output `latitude` is
  `NaN`.
- `@pytest.mark.unit` — Assert the `outlier_flag` column dtype is `bool`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 18: Implement unit labelling

**Module:** `kgs_pipeline/clean.py`
**Function:** `add_unit_column(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Add a `unit` column (string) derived from `product`:
  - `product == "O"` → `unit = "BBL"`
  - `product == "G"` → `unit = "MCF"`
  - Any other value → `unit = "UNKNOWN"`, log a WARNING.
- Return a lazy Dask DataFrame.

**Error handling:**
- If `product` column is absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given `product="O"`, assert `unit="BBL"`.
- `@pytest.mark.unit` — Given `product="G"`, assert `unit="MCF"`.
- `@pytest.mark.unit` — Given `product="X"` (unknown), assert `unit="UNKNOWN"`.
- `@pytest.mark.unit` — Assert the `unit` column dtype is `object` (string).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 19: Implement deduplication

**Module:** `kgs_pipeline/clean.py`
**Function:** `deduplicate(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Drop duplicate rows based on the composite key `[well_id, production_date, product]`,
  keeping the first occurrence.
- Use `map_partitions` with `pandas.DataFrame.drop_duplicates()` for partition-level
  deduplication.
- Perform a second Dask-level deduplication pass after repartitioning by `well_id` to catch
  cross-partition duplicates.
- Log the count of duplicate rows removed at INFO level (approximate, based on partition
  counts).
- Return a lazy Dask DataFrame.

**Error handling:**
- If any of `[well_id, production_date, product]` are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with 2 identical rows on `[well_id,
  production_date, product]`, assert the output has 1 row.
- `@pytest.mark.unit` — Given a DataFrame with no duplicates, assert the row count is
  unchanged.
- `@pytest.mark.unit` — Assert the deduplication is idempotent: applying `deduplicate` twice
  produces the same result as applying it once.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert the output row count is less than or equal to the input row
  count (never greater).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 20: Implement chronological sort and well-based repartitioning

**Module:** `kgs_pipeline/clean.py`
**Function:** `sort_and_repartition(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Sort the DataFrame by `[well_id, production_date]` in ascending order.
- Repartition using `df.set_index("well_id", sorted=True, drop=False)` to co-locate all rows
  for each well in the same partition set.
- After setting the index, reset it so `well_id` remains a regular column.
- The resulting DataFrame must be sorted such that, for any single well, records appear in
  ascending `production_date` order.
- Return a lazy Dask DataFrame.

**Error handling:**
- If `well_id` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given 3 rows for the same `well_id` in random date order, assert the
  output rows for that well are in ascending date order.
- `@pytest.mark.unit` — Assert that all rows for a given `well_id` are in the same partition
  (use `.compute()` in the test and group by `well_id`).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert the last `production_date` of one Parquet partition for a
  well is earlier than the first `production_date` of the next partition for the same well,
  if the well spans multiple partitions.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 21: Implement processed Parquet writer

**Module:** `kgs_pipeline/clean.py`
**Function:** `write_processed_parquet(df: dask.dataframe.DataFrame, output_dir: Path) -> None`

**Description:**
- Write the cleaned, sorted, per-well Dask DataFrame to `output_dir` as partitioned Parquet.
- Partition by `well_id` using `df.to_parquet(output_dir, partition_on=["well_id"], ...)`.
- Use `write_index=False`, `engine="pyarrow"`, `compression="snappy"`.
- Pass an explicit `pyarrow.Schema` matching the processed schema defined in this document to
  prevent schema drift across partitions with all-null columns.
- Create `output_dir` if it does not exist.
- This is the only `.compute()` trigger in the clean module.
- Log start/end times and total row count at INFO level.

**Error handling:**
- If `output_dir` cannot be created, let `OSError` propagate.
- If the DataFrame is empty after all cleaning steps, log a WARNING and return without writing.

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small processed Dask DataFrame, assert `write_processed_parquet`
  creates at least 1 Parquet file in `tmp_path`.
- `@pytest.mark.unit` — Assert each written Parquet file is readable by `pandas.read_parquet()`
  without error.
- `@pytest.mark.unit` — Assert partition directories are named by `well_id` value.
- `@pytest.mark.unit` — Assert each partition file contains data for exactly one `well_id`
  value (no cross-partition contamination).
- `@pytest.mark.unit` — Sample two different well partitions and assert they have identical
  column names and dtypes (schema stability across partitions).
- `@pytest.mark.unit` — Given an empty DataFrame, assert no files are written and a WARNING
  is logged.
- `@pytest.mark.integration` — Run the full clean pipeline on real interim data; assert
  `data/processed/` is non-empty and all Parquet files are readable.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 22: Implement clean pipeline entry-point

**Module:** `kgs_pipeline/clean.py`
**Function:**
`run_clean_pipeline(interim_dir: Path | None = None, output_dir: Path | None = None) -> None`

**Description:**
- Synchronous entry-point that orchestrates the cleaning workflow in order:
  1. `load_interim_parquet(interim_dir)` → `df`
  2. `rename_columns(df)` → `df`
  3. `parse_production_date(df)` → `df`
  4. `cast_dtypes(df)` → `df`
  5. `explode_api_numbers(df)` → `df`
  6. `validate_physical_bounds(df)` → `df`
  7. `add_unit_column(df)` → `df`
  8. `deduplicate(df)` → `df`
  9. `sort_and_repartition(df)` → `df`
  10. `write_processed_parquet(df, output_dir)`
- Resolves defaults from `config.py` when arguments are `None`.
- Logs total elapsed time at INFO level.

**Error handling:**
- Re-raises `FileNotFoundError` from `load_interim_parquet` with context.
- Any unhandled exception is logged at CRITICAL level before re-raising.

**Dependencies:** `pathlib`, `logging`, `kgs_pipeline.config`, `kgs_pipeline.clean`

**Test cases:**
- `@pytest.mark.unit` — Patch each of the sub-functions; assert they are called in the correct
  order with the correct arguments.
- `@pytest.mark.unit` — Patch `load_interim_parquet` to raise `FileNotFoundError`; assert
  `run_clean_pipeline` re-raises `FileNotFoundError`.
- `@pytest.mark.unit` — Assert `None` arguments resolve to config defaults without raising.
- `@pytest.mark.integration` — Run `run_clean_pipeline()` end-to-end on real interim data;
  assert `data/processed/` is non-empty and at least one Parquet file is readable.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Domain-Specific Test Cases for `tests/test_clean.py`

The following domain-specific tests must be implemented in `tests/test_clean.py` and use
realistic synthetic data or real processed data. Each test is tagged with the appropriate
marker.

### Physical bound validation
- `@pytest.mark.unit` — Assert no `production` value in the processed output is negative.
- `@pytest.mark.unit` — Assert `production = 0.0` rows exist in the output if they existed
  in the input (zero is preserved as zero, not converted to NaN).

### Unit consistency
- `@pytest.mark.unit` — Assert all rows where `product == "O"` have `unit == "BBL"`.
- `@pytest.mark.unit` — Assert all rows where `product == "G"` have `unit == "MCF"`.
- `@pytest.mark.unit` — Assert no individual well-month oil production value exceeds 50,000 BBL
  without the corresponding `outlier_flag` being `True`.

### Well completeness
- `@pytest.mark.integration` — For a sample of 10 wells in the processed output, assert that
  the count of monthly records equals the number of months between the first and last
  `production_date` for that well (no gaps).

### Zero production handling
- `@pytest.mark.unit` — Given input data with an explicit `production=0.0` row, assert the
  processed output contains `production=0.0` (not `NaN`) for that well-month.

### Data integrity
- `@pytest.mark.integration` — For a random sample of 50 well-month rows from the raw data,
  assert the corresponding `production` value in the processed Parquet matches the raw value
  (after accounting for type casting, with tolerance `1e-4`).

### Deduplication correctness
- `@pytest.mark.unit` — Assert the processed row count is less than or equal to the raw row
  count.
- `@pytest.mark.unit` — Apply the full clean pipeline twice on the same interim data; assert
  the row counts and values are identical (idempotency).

### Partition correctness
- `@pytest.mark.unit` — Assert each Parquet partition file in `data/processed/` contains rows
  for exactly one unique `well_id` value.

### Schema stability
- `@pytest.mark.integration` — Sample the schema from 5 randomly chosen well partition files;
  assert all 5 schemas are identical in column names and dtypes.

### Sort stability
- `@pytest.mark.unit` — For a multi-month well, assert records are in ascending
  `production_date` order after cleaning.

### Lazy evaluation
- `@pytest.mark.unit` — Assert every transformation function (`rename_columns`,
  `parse_production_date`, `cast_dtypes`, `explode_api_numbers`, `validate_physical_bounds`,
  `add_unit_column`, `deduplicate`, `sort_and_repartition`) returns a `dask.dataframe.DataFrame`
  and not a `pandas.DataFrame`.

### Parquet readability
- `@pytest.mark.integration` — Assert every Parquet file in `data/processed/` is readable by
  `pandas.read_parquet()` without raising an exception.
