# Transform Component Tasks

**Module:** `kgs_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

## Overview

The transform component reads interim Parquet files from `data/interim/`, performs
data cleaning and standardisation (column renaming, type casting, duplicate removal,
outlier handling, zero/null distinction), and writes clean Parquet files to
`data/processed/`. It also produces per-well monthly records that are the input to the
features stage.

The raw KGS data schema (after ingest normalisation) has columns:
`LEASE_KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`,
`COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`,
`LONGITUDE`, `MONTH_YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`, `source_file`.

The transform stage reshapes this into a well-centric wide format where each row
represents one lease-month with separate columns for oil and gas production.

## Design Decisions

- All transform functions accept and return `dask.dataframe.DataFrame` — never call
  `.compute()` internally (TR-17). Use `map_partitions` for row-level operations.
- String column `meta=` arguments always use `pd.StringDtype()` — never `"object"`.
- After reading interim Parquet, immediately repartition to `min(npartitions, 50)`.
- Output: write to `data/processed/` as Parquet. Target `max(1, total_rows // 500_000)`
  partitions. Never partition_on a high-cardinality column.
- Logging: structured JSON via `kgs_pipeline.logging_utils`; log cleaning operation
  counts (rows dropped for duplicates, rows dropped for bad dates, outlier rows flagged).
- Domain units: oil production in BBL, gas production in MCF, water in BBL.
- `PRODUCT` column values: `"O"` = oil, `"G"` = gas. The pipeline pivots these to
  separate `oil_bbl` and `gas_mcf` columns.
- `MONTH_YEAR` is parsed into a `production_date` column (first day of the month) as
  `datetime64[ns]`. Rows with MONTH_YEAR = `"0-YYYY"` (annual summary rows) and
  MONTH_YEAR starting with `"-1"` (cumulative start rows) must be dropped before the
  pivot — they are not monthly observations.
- `LEASE_KID` is renamed to `lease_kid` and used as the primary well/lease identifier.
- All identifier and string columns use `pd.StringDtype()` in Parquet output to
  prevent pyarrow type mismatches.
- Outlier flag: a production value > 50,000 BBL/month for oil or > 500,000 MCF/month
  for gas on a single lease is flagged with a boolean `is_outlier` column (TR-02).
  Flagged rows are NOT removed — they are retained with `is_outlier=True`. Downstream
  ML workflows decide whether to exclude them.

---

## Task 01: Read and repartition interim Parquet

**Module:** `kgs_pipeline/transform.py`
**Function:** `read_interim(interim_dir: str) -> dd.DataFrame`

**Description:**
Read all Parquet files from `interim_dir` using `dd.read_parquet(interim_dir)`.
Immediately repartition to `min(ddf.npartitions, 50)` before returning. Log the number
of partitions and the interim directory path. Raise `FileNotFoundError` if `interim_dir`
does not exist or contains no Parquet files.

**Dependencies:** `dask.dataframe`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a temporary directory with 2 valid Parquet files written
  from a small synthetic DataFrame, assert `read_interim` returns a
  `dask.dataframe.DataFrame` with the correct columns.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Assert `npartitions <= 50` after the call.
- `@pytest.mark.unit` — Given a non-existent path, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given `data/interim/` populated by the ingest stage,
  assert `read_interim` returns a non-empty Dask DataFrame.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Drop non-monthly rows and parse production date

**Module:** `kgs_pipeline/transform.py`
**Function:** `parse_production_date(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Filter out non-monthly rows and parse `MONTH_YEAR` into a `production_date` column.

Inside a `map_partitions` function:
1. Drop rows where MONTH_YEAR starts with `"0-"` (annual summary, month=0) or `"-1-"`
   (cumulative start rows). These are metadata rows, not monthly production observations.
2. For remaining rows, split MONTH_YEAR on `"-"` to extract month (first token) and
   year (last token). Construct a `production_date` as the first day of that month:
   `pd.to_datetime({"year": year_int, "month": month_int, "day": 1}, errors="coerce")`.
3. Drop rows where `production_date` is NaT (unparseable dates).
4. Drop the original `MONTH_YEAR` column.
5. Return the updated DataFrame.

The `meta` passed to `map_partitions` must include a `production_date` column with
dtype `datetime64[ns]` and exclude `MONTH_YEAR`.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given rows with MONTH_YEAR values `["1-2024", "0-2024",
  "-1-2024", "12-2024"]`, assert the result contains exactly 2 rows (only `"1-2024"`
  and `"12-2024"` survive) with a `production_date` column of dtype `datetime64[ns]`.
- `@pytest.mark.unit` — Given `MONTH_YEAR = "6-2024"`, assert `production_date` equals
  `pd.Timestamp("2024-06-01")`.
- `@pytest.mark.unit` — Given a row with an unparseable MONTH_YEAR (e.g. `"abc-2024"`),
  assert it is dropped and no exception is raised.
- `@pytest.mark.unit` — Assert `MONTH_YEAR` column is absent in the output DataFrame.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Column standardisation and type casting

**Module:** `kgs_pipeline/transform.py`
**Function:** `standardise_columns(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Rename and cast columns to their canonical types for the processed schema:

1. Rename `LEASE_KID` → `lease_kid`, `LEASE` → `lease_name`, `API_NUMBER` → `api_number`,
   `OPERATOR` → `operator`, `COUNTY` → `county`, `FIELD` → `field`,
   `PRODUCING_ZONE` → `producing_zone`, `PRODUCT` → `product`,
   `PRODUCTION` → `production`, `WELLS` → `well_count`,
   `LATITUDE` → `latitude`, `LONGITUDE` → `longitude`.
   Retain `source_file` unchanged. Drop columns that are not in the canonical set:
   `DOR_CODE`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`
   (these are retained as optional — keep them if present but they are not required
   downstream; do not raise if they are absent).
2. Cast `production` to `float64` (coerce errors to NaN).
3. Cast `well_count` to `float64` (coerce errors to NaN).
4. Cast `latitude` and `longitude` to `float64` (coerce errors to NaN).
5. Cast `lease_kid`, `api_number`, `operator`, `county`, `lease_name`, `field`,
   `producing_zone`, `product`, `source_file` to `pd.StringDtype()`.
6. Apply inside `map_partitions` with explicit meta covering the output schema.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a synthetic DataFrame with original KGS column names,
  assert the output contains `lease_kid`, `operator`, `county`, `production`,
  `well_count`, `production_date`, `product` and that all string columns have
  `pd.StringDtype()`.
- `@pytest.mark.unit` — Given a `PRODUCTION` value of `"abc"` (non-numeric string),
  assert it is coerced to `NaN` (not exception).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Assert `DOR_CODE` is not present in output if
  implementation drops it.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Deduplicate records

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Remove duplicate rows from the Dask DataFrame. A duplicate is defined as two rows
sharing the same `(lease_kid, product, production_date)` tuple. Keep the first
occurrence.

Implementation: use `map_partitions` to apply `pd.DataFrame.drop_duplicates` on each
partition with `subset=["lease_kid", "product", "production_date"], keep="first"`.
This is a within-partition deduplication; cross-partition duplicates are handled by
ensuring the caller repartitions appropriately before this step (document this
assumption in the docstring).

Log the count of rows before and after deduplication at INFO level.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a DataFrame with 10 rows where 3 are duplicates of
  existing rows (same lease_kid, product, production_date), assert the result has 7 rows.
- `@pytest.mark.unit` — Given a DataFrame with no duplicates, assert the result has the
  same row count as the input.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Test cases (TR-15):**

- `@pytest.mark.unit` TR-15a — Given a synthetic DataFrame, assert that after
  deduplication the row count is less than or equal to the original row count.
- `@pytest.mark.unit` TR-15b — Apply deduplication twice on the same DataFrame and
  assert the output of the second application is identical in shape and values to the
  first (idempotency).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Pivot oil and gas production to wide format

**Module:** `kgs_pipeline/transform.py`
**Function:** `pivot_products(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Transform the long-format DataFrame (one row per lease-month-product) to a wide format
(one row per lease-month with separate columns for oil and gas production).

Implementation steps (inside `map_partitions`):

1. Separate oil rows (`product == "O"`) and gas rows (`product == "G"`).
2. Rename `production` → `oil_bbl` in the oil subset; rename `production` → `gas_mcf`
   in the gas subset.
3. Define the merge key as `["lease_kid", "production_date", "api_number", "operator",
   "county", "field", "producing_zone", "well_count", "latitude", "longitude",
   "source_file"]`.
4. Outer-merge the oil and gas subsets on the merge key.
5. Fill `oil_bbl` NaN values with `0.0` where oil data is absent (gas-only lease-month).
6. Fill `gas_mcf` NaN values with `0.0` where gas data is absent (oil-only lease-month).
7. Drop the `product` column.
8. Return the wide DataFrame.

The meta for `map_partitions` must include `oil_bbl` (`float64`), `gas_mcf` (`float64`),
and omit `production` and `product`.

**Domain context (TR-05):**
Zero production values (`0.0`) in the raw data must remain `0.0` in the output — not
be converted to NaN. Only missing data (where the product type was entirely absent for
a lease-month) is filled with `0.0` during the pivot.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a long DataFrame with oil row (`product="O"`,
  `production=100.0`) and gas row (`product="G"`, `production=500.0`) for the same
  lease-month, assert the wide output has a single row with `oil_bbl=100.0` and
  `gas_mcf=500.0`.
- `@pytest.mark.unit` — Given a lease-month with only an oil row, assert the wide
  output has `oil_bbl=<value>` and `gas_mcf=0.0`.
- `@pytest.mark.unit` TR-05 — Given a raw row with `production=0.0` for product `"O"`,
  assert `oil_bbl=0.0` in the output (not NaN).
- `@pytest.mark.unit` — Assert `product` and `production` columns are absent in output.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Outlier flagging

**Module:** `kgs_pipeline/transform.py`
**Function:** `flag_outliers(ddf: dd.DataFrame, oil_threshold: float = 50_000.0, gas_threshold: float = 500_000.0) -> dd.DataFrame`

**Description:**
Add a boolean `is_outlier` column to the DataFrame. A row is flagged as an outlier if
`oil_bbl > oil_threshold` OR `gas_mcf > gas_threshold`. Outlier rows are retained
in the dataset — they are not removed. The `is_outlier` column has dtype `bool`.

Also validate physical bounds inside `map_partitions` (TR-01, TR-02):
- Log a warning (do not drop) for any row where `oil_bbl < 0` or `gas_mcf < 0`.
- These physically impossible values are NOT automatically removed here — they are
  flagged via a separate `has_negative_production` boolean column.

Both columns (`is_outlier`, `has_negative_production`) must appear in the `meta`.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` TR-01 — Given a row with `oil_bbl = -5.0`, assert
  `has_negative_production = True` and the row is retained (not dropped).
- `@pytest.mark.unit` TR-02 — Given a row with `oil_bbl = 100_000.0` (above 50,000
  threshold), assert `is_outlier = True`.
- `@pytest.mark.unit` TR-02 — Given a row with `oil_bbl = 49_999.0`, assert
  `is_outlier = False`.
- `@pytest.mark.unit` — Given a row with `gas_mcf = 600_000.0`, assert `is_outlier = True`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Assert both `is_outlier` and `has_negative_production` columns
  are present in the output.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Sort and well-continuity validation

**Module:** `kgs_pipeline/transform.py`
**Function:** `sort_by_well_date(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Sort the Dask DataFrame by `["lease_kid", "production_date"]` in ascending order.
Use `ddf.sort_values(["lease_kid", "production_date"])`. This materialises the sort
lazily (Dask will add a shuffle step). Document in the docstring that the resulting
Dask DataFrame has a modified partition structure after the shuffle.

After sorting, return the Dask DataFrame. The caller is responsible for repartitioning
before writing.

**Dependencies:** `dask.dataframe`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` TR-16 — Given a synthetic DataFrame with rows out of order for
  two leases, assert that after `.compute()` the rows for each lease_kid are sorted
  ascending by production_date with no date inversion.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` TR-04 — Given a synthetic well with production records for
  January through December 2024 (12 rows), assert that after sorting the row count
  per lease_kid equals 12.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Write processed Parquet output

**Module:** `kgs_pipeline/transform.py`
**Function:** `write_processed_parquet(ddf: dd.DataFrame, output_dir: str) -> int`

**Description:**
Write the cleaned, wide-format Dask DataFrame to `data/processed/` as Parquet files.

1. Compute target npartitions: `max(1, ddf.npartitions // 5)` (aim for 20-50 files).
2. Repartition: `ddf.repartition(npartitions=N)`.
3. Write: `ddf.to_parquet(output_dir, write_index=False, overwrite=True)`.
4. Return the count of `.parquet` files written.
5. Log the file count and output directory.

Never use `partition_on` with `lease_kid` or any other high-cardinality column.

**Dependencies:** `dask`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a small synthetic Dask DataFrame (5 partitions, 50 rows),
  assert `write_processed_parquet` returns >= 1 and the output directory contains
  Parquet files.
- `@pytest.mark.unit` — Assert written Parquet files can be read back with
  `pd.read_parquet` without error (TR-18).
- `@pytest.mark.unit` — Assert the number returned equals the actual file count on disk.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Transform orchestrator and CLI

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform(interim_dir: str, output_dir: str) -> int`
**Entry point:** `main()` and `if __name__ == "__main__"` block

**Description:**
Chain all transform functions:
1. `read_interim(interim_dir)` → raw Dask DataFrame.
2. `parse_production_date(ddf)` → date-parsed DataFrame.
3. `standardise_columns(ddf)` → standardised DataFrame.
4. `deduplicate(ddf)` → deduplicated DataFrame.
5. `pivot_products(ddf)` → wide-format DataFrame.
6. `flag_outliers(ddf)` → flagged DataFrame.
7. `sort_by_well_date(ddf)` → sorted DataFrame.
8. `write_processed_parquet(ddf, output_dir)` → returns file count.

`main()` uses `argparse`:
- `--interim-dir`: default from `config.INTERIM_DATA_DIR`
- `--output-dir`: default from `config.PROCESSED_DATA_DIR`

Register console script: `kgs-transform = "kgs_pipeline.transform:main"`

**Test cases:**

- `@pytest.mark.unit` — Mock all sub-functions; assert `run_transform` calls them in
  correct order and returns the mocked file count.
- `@pytest.mark.integration` — Given real `data/interim/` input, assert `run_transform`
  writes at least 1 Parquet file and returns a positive integer.

**Definition of done:** Orchestrator and CLI implemented, all tests pass, `ruff` and
`mypy` report no errors, console script registered. `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 10: Domain and technical correctness tests (TR-01 to TR-05, TR-11 to TR-16)

**Module:** `tests/test_transform.py`

**Description:**
Implement all domain and technical correctness tests for the transform stage.

**Test cases:**

- `@pytest.mark.unit` TR-01 — Physical bounds: assert that the processed Parquet output
  (computed from a synthetic input that includes a row with `oil_bbl=-1.0`) contains the
  row with `has_negative_production=True` and `is_outlier=False`.
- `@pytest.mark.unit` TR-02 — Unit consistency: given a synthetic DataFrame with
  `oil_bbl=100_001.0`, assert `is_outlier=True` in the output.
- `@pytest.mark.unit` TR-04 — Well completeness: given a synthetic well with records
  for months 1-12 of 2024 (12 rows), assert 12 rows survive the full transform pipeline.
- `@pytest.mark.unit` TR-05 — Zero production: given a raw row with `production=0.0`
  for product `"O"`, assert `oil_bbl=0.0` (not NaN) in the processed output.
- `@pytest.mark.integration` TR-11 — Data integrity: read 10 randomly sampled
  lease-months from `data/processed/` and compare their `oil_bbl` values against the
  corresponding raw files in `data/raw/`. Assert values match within floating-point
  tolerance.
- `@pytest.mark.integration` TR-12 — Data cleaning validation: read all Parquet files
  from `data/processed/` and assert: no duplicates on `(lease_kid, production_date)`,
  `production_date` dtype is `datetime64[ns]`, `oil_bbl` dtype is `float64`.
- `@pytest.mark.integration` TR-13 — Partition correctness: for each Parquet partition
  in `data/processed/`, assert that the partition contains only one `lease_kid` value
  (no cross-lease mixing). Note: this test applies only if `write_processed_parquet`
  uses `partition_on=["lease_kid"]`; if not, skip or document why this check is
  not applicable and substitute a check that each file does not mix counties.
- `@pytest.mark.integration` TR-14 — Schema stability: read Parquet schemas from at
  least 3 partition files in `data/processed/` and assert all have identical column
  names and dtypes.
- `@pytest.mark.unit` TR-15 — Row count reconciliation: assert deduplicated row count
  <= raw row count and that running deduplication twice yields identical output.
- `@pytest.mark.integration` TR-16 — Sort stability: for a given `lease_kid` that
  spans multiple Parquet files, assert the last `production_date` in the first file
  is earlier than the first `production_date` in the second file.
- `@pytest.mark.integration` TR-17 — Lazy evaluation: assert `read_interim`,
  `parse_production_date`, `standardise_columns`, `deduplicate`, `pivot_products`,
  `flag_outliers`, and `sort_by_well_date` all return `dask.dataframe.DataFrame`
  (not `pd.DataFrame`).
- `@pytest.mark.integration` TR-18 — Parquet readability: read every `.parquet` file
  in `data/processed/` with `pd.read_parquet` and assert no exception is raised.

**Definition of done:** All test cases implemented, all unit tests pass, `ruff` and
`mypy` report no errors. `requirements.txt` updated with all third-party packages
imported in this task.
