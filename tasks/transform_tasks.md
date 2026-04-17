# Transform Stage — Task Specifications

## Context and Design Decisions

The transform stage reads the consolidated interim Parquet files produced by ingest,
cleans and validates the data, derives the `production_date` column, indexes by entity
(LEASE_KID), sorts by production date within each partition, casts categorical columns
to their declared category sets, and writes analysis-ready Parquet to `data/interim/`
as the final input for the features stage.

**Key ADRs governing this stage:**
- ADR-001: Use Dask distributed scheduler for CPU-bound transform
- ADR-002: All operations must be vectorized — no per-row iteration
- ADR-003: Data dictionary is the single source of truth for schema
- ADR-004: Parquet inter-stage format; partition count = max(10, min(n, 50));
  repartition is the last structural operation before writing
- ADR-005: Task graph stays lazy until the final write; batch all independent computations
- ADR-006: Dual-channel logging; log config from config.yaml
- ADR-007: Python 3.11+; pytest for tests

**Input state (from ingest boundary contract):**
- Canonical schema established
- Data-dictionary dtypes enforced
- All nullable columns present (NA-filled if absent from source)
- Partitioned to max(10, min(n, 50))
- Unsorted

**Output state (guaranteed to features boundary contract):**
- Entity-indexed on LEASE_KID
- Sorted by `production_date` within each partition
- Categoricals cast to declared category sets
- Invalid values replaced with appropriate null sentinel
- Partitioned to max(10, min(n, 50))

**Shared state hazards (from stage manifest):**
- H1: Sort by `production_date` must be valid at stage exit regardless of execution order;
  the distributed shuffle from entity indexing destroys prior row ordering
- H2: Categorical columns must carry only declared category values at stage exit
- H3: Partition count at stage exit must equal max(10, min(n, 50)); no operation after
  repartition may alter partition structure

**Package name:** `kgs_pipeline`
**Module path:** `kgs_pipeline/transform.py`

**Input:**
- Partitioned Parquet in `data/interim/` (output of ingest stage)

**Output:**
- Partitioned Parquet in `data/interim/transformed/`

---

## Canonical Column Reference

The transform stage works with the full canonical schema from ingest plus the derived
`production_date` column it creates. The complete column set at stage exit:

| Column | dtype (pandas) | Notes |
|---|---|---|
| LEASE_KID | Int64 | Entity identifier; used as partition index |
| LEASE | StringDtype | |
| DOR_CODE | Int64 | |
| API_NUMBER | StringDtype | |
| FIELD | StringDtype | |
| PRODUCING_ZONE | StringDtype | |
| OPERATOR | StringDtype | |
| COUNTY | StringDtype | |
| TOWNSHIP | Int64 | |
| TWN_DIR | CategoricalDtype(["S","N"]) | |
| RANGE | Int64 | |
| RANGE_DIR | CategoricalDtype(["E","W"]) | |
| SECTION | Int64 | |
| SPOT | StringDtype | |
| LATITUDE | float64 | |
| LONGITUDE | float64 | |
| MONTH-YEAR | StringDtype | |
| PRODUCT | CategoricalDtype(["O","G"]) | |
| WELLS | Int64 | |
| PRODUCTION | float64 | Physical bounds: must be >= 0 (TR-01) |
| source_file | StringDtype | |
| production_date | datetime64[ns] | Derived from MONTH-YEAR; Month=0 and Month=-1 rows dropped |

---

## Task 01: Implement production_date derivation

**Module:** `kgs_pipeline/transform.py`
**Function:** `derive_production_date(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Parse the MONTH-YEAR column into a proper `production_date` datetime column. This
function operates on a pandas partition (called via `map_partitions`).

**Steps:**
- Split MONTH-YEAR on "-" to extract month (first element) and year (last element)
- Convert month and year to integers
- Drop rows where month == 0 (yearly aggregate rows per KGS data dictionary) or
  month == -1 (starting cumulative marker per KGS data dictionary)
- Drop rows where month is not in 1–12 or year < 2024 (defensive re-filter)
- Construct `production_date` as the first day of each month:
  `pd.to_datetime({"year": year_series, "month": month_series, "day": 1})`
- Cast `production_date` to `datetime64[ns]`
- Return the DataFrame with the new `production_date` column appended

**Error handling:**
- Non-numeric month or year values that survived ingest filtering must be handled
  without raising — drop those rows and log a warning with the count dropped

**Meta for map_partitions (ADR-003):**
- The meta must be derived by calling `derive_production_date` on a minimal real
  input (one-row DataFrame) and slicing to zero rows — not constructed separately

**Test cases:**
- Given a partition with MONTH-YEAR = "3-2024", assert `production_date` is
  `pd.Timestamp("2024-03-01")`
- Given a partition with MONTH-YEAR = "0-2024" (yearly aggregate), assert that row
  is dropped
- Given a partition with MONTH-YEAR = "-1-2024" (starting cumulative), assert that
  row is dropped
- Given a partition with MONTH-YEAR = "12-2024", assert `production_date` is
  `pd.Timestamp("2024-12-01")`
- Assert the returned DataFrame has dtype `datetime64[ns]` for `production_date`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement physical bounds validation and cleaning

**Module:** `kgs_pipeline/transform.py`
**Function:** `validate_physical_bounds(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Enforce physical constraints on production volumes. This function operates on a pandas
partition (called via `map_partitions`). Implements TR-01, TR-02, TR-05.

**Rules:**
- PRODUCTION must be >= 0 (TR-01): set negative values to `pd.NA` (null), not 0 —
  a negative production value is a data error, not a zero-production reading (TR-05)
- WELLS must be >= 0 (TR-01): set negative values to `pd.NA`
- Preserve existing zero values — zeros are valid measurements, not missing data (TR-05)
- Oil production (PRODUCT == "O") in PRODUCTION column is in BBL; gas (PRODUCT == "G")
  is in MCF — no unit conversion is applied here, but flag records where PRODUCT == "O"
  and PRODUCTION > 50000 as a potential unit error by adding a boolean column
  `production_unit_flag` (True if flagged, False otherwise) (TR-02)

**Steps:**
- For PRODUCTION < 0: replace with `pd.NA`, log count of replacements
- For WELLS < 0: replace with `pd.NA`, log count of replacements
- For PRODUCT == "O" and PRODUCTION > 50000: set `production_unit_flag = True`;
  for all other rows: `production_unit_flag = False`
- Return the modified DataFrame

**Test cases (TR-01, TR-02, TR-05):**
- Given a row with PRODUCTION = -5.0, assert the output PRODUCTION is `pd.NA`
- Given a row with PRODUCTION = 0.0, assert the output PRODUCTION is 0.0 (not NA)
- Given a row with PRODUCTION = 100.0, assert it passes through unchanged
- Given a row with PRODUCT == "O" and PRODUCTION = 60000.0, assert `production_unit_flag = True`
- Given a row with PRODUCT == "G" and PRODUCTION = 60000.0, assert `production_unit_flag = False`
  (gas units are MCF, 60000 MCF is plausible)
- Given a row with WELLS = -1, assert output WELLS is `pd.NA`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement duplicate removal

**Module:** `kgs_pipeline/transform.py`
**Function:** `remove_duplicates(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Remove duplicate records from a pandas partition. This function operates on a pandas
partition (called via `map_partitions`).

**Deduplication key:** `(LEASE_KID, MONTH-YEAR, PRODUCT)` — a lease can have at most
one oil production row and one gas production row per month.

**Steps:**
- Call `df.drop_duplicates(subset=["LEASE_KID", "MONTH-YEAR", "PRODUCT"], keep="first")`
- Return the deduplicated DataFrame

**Test cases (TR-15):**
- Given a DataFrame with two rows sharing the same `(LEASE_KID, MONTH-YEAR, PRODUCT)`,
  assert the output has one row
- Given a DataFrame with no duplicates, assert row count is unchanged
- Assert idempotency: calling `remove_duplicates` twice on the same input produces
  the same output as calling it once (TR-15)
- Assert output row count is always <= input row count (TR-15)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement categorical column casting

**Module:** `kgs_pipeline/transform.py`
**Function:** `cast_categoricals(df: pd.DataFrame, schema: dict[str, dict]) -> pd.DataFrame`

**Description:**
Cast all categorical columns to their declared category sets. Replace values outside
the declared set with `pd.NA` before casting. Operates on a pandas partition.

**Steps:**
- For each column where schema dtype == "categorical":
  - Identify values in the column not in the declared categories list
  - Replace those out-of-set values with `pd.NA`
  - Cast the column to `pd.CategoricalDtype(categories=declared_categories, ordered=False)`
- Return the modified DataFrame

**Test cases (TR-12, H2 from stage manifest):**
- Given PRODUCT column containing value "X" (not in ["O","G"]), assert it becomes `pd.NA`
  after casting
- Given TWN_DIR column containing only valid values "S" and "N", assert they are
  preserved and the dtype is `CategoricalDtype(["S","N"])`
- Given RANGE_DIR containing "X", assert it becomes `pd.NA`
- Assert the output CategoricalDtype for PRODUCT has exactly the categories ["O","G"]

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement well completeness gap detection

**Module:** `kgs_pipeline/transform.py`
**Function:** `check_well_completeness(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
For each unique `LEASE_KID`, verify that the monthly production record has no unexpected
gaps between the first and last production month. Add a boolean column
`has_date_gap` indicating whether any gap months are missing for that lease.

This function operates on a pandas partition (called via `map_partitions`). TR-04.

**Steps:**
- For each unique LEASE_KID in the partition:
  - Find the minimum and maximum `production_date` for that lease in the partition
  - Count the expected number of months between min and max (inclusive)
  - Count the actual number of distinct `production_date` values for that lease
  - If actual count < expected count, the lease has gaps → `has_date_gap = True`
    for all rows belonging to that lease; otherwise `has_date_gap = False`
- Return the DataFrame with the `has_date_gap` boolean column appended

**Implementation constraint (ADR-002):**
- Must use vectorized grouped operations (e.g. `groupby` + `transform`) — no iteration
  over unique LEASE_KID values

**Test cases (TR-04):**
- Given a lease with production in January, February, and March 2024, assert
  `has_date_gap = False` for all rows of that lease
- Given a lease with production in January and March 2024 but missing February, assert
  `has_date_gap = True` for all rows of that lease
- Given a lease with a single month of production, assert `has_date_gap = False`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Implement entity indexing and sort

**Module:** `kgs_pipeline/transform.py`
**Function:** `index_and_sort(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Set LEASE_KID as the Dask DataFrame index (entity index) and sort within each partition
by `production_date`. This must produce correct temporal ordering at stage exit
regardless of execution order (H1 from stage manifest).

**Steps:**
- Set LEASE_KID as the Dask index using `ddf.set_index("LEASE_KID", sorted=False,
  drop=False)` — `drop=False` preserves LEASE_KID as a regular column as well
- After setting the index, sort within each partition by `production_date` using
  `map_partitions(lambda df: df.sort_values("production_date"))` — this ensures
  temporal ordering is valid at stage exit after the distributed shuffle
- Return the lazy Dask DataFrame

**Meta for map_partitions (ADR-003):**
- Derive meta by calling the sort lambda on a minimal real input and slicing to zero rows

**Test cases (TR-16, H1):**
- Given a Dask DataFrame with rows in arbitrary date order, assert the output partitions
  have rows sorted ascending by `production_date`
- Assert the index of the output Dask DataFrame is LEASE_KID
- Assert the last row of any given partition (by sort key) has a production_date <=
  the first row of the same partition sorted ascending (sort stability within partition)
  (TR-16)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement data integrity spot-check

**Module:** `kgs_pipeline/transform.py`
**Function:** `spot_check_integrity(ddf_input: dd.DataFrame, ddf_output: dd.DataFrame, sample_size: int) -> dict`

**Description:**
Randomly sample `sample_size` (LEASE_KID, MONTH-YEAR, PRODUCT) combinations from the
input and verify that the PRODUCTION values in the output match the input values exactly
for that sample. Return a dict with keys `checked`, `passed`, `failed`. TR-11.

**Steps:**
- Sample `sample_size` rows from `ddf_input` using Dask's `sample` method
- For each sampled (LEASE_KID, MONTH-YEAR, PRODUCT) combination, look up the
  corresponding row in `ddf_output`
- Compare PRODUCTION values between input and output for each sampled row
  (allowing for NA where input was negative, per Task 02 rules)
- Return `{"checked": n, "passed": n_passed, "failed": n_failed}`

**Implementation note:**
- This function may call `.compute()` on the sample only — it is explicitly a
  diagnostic function, not part of the main transformation chain

**Test cases (TR-11):**
- Given identical input and output DataFrames (no transformations applied), assert
  `failed == 0` for a sample of 5 rows
- Given an output where one PRODUCTION value was changed, assert `failed >= 1`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement transform Parquet writer

**Module:** `kgs_pipeline/transform.py`
**Function:** `write_transform_parquet(ddf: dd.DataFrame, output_dir: Path, n_partitions: int) -> None`

**Description:**
Repartition and write the transformed Dask DataFrame as partitioned Parquet. The
repartition step is the last structural operation before writing (ADR-004, H3).

**Steps:**
- Repartition to `n_partitions` using `ddf.repartition(npartitions=n_partitions)`
- Write to `output_dir` using `ddf.to_parquet(output_dir, write_index=True, overwrite=True)`
- Log the output path and partition count

**Error handling:**
- Allow write errors to propagate to the caller with original context

**Test cases (TR-13, TR-14, TR-18):**
- Given a small Dask DataFrame, assert at least one `.parquet` file is written to
  `output_dir`
- Assert every output Parquet file is readable by a fresh `pandas.read_parquet` call
  (TR-18)
- Assert no single Parquet partition file contains rows from more than one `LEASE_KID`
  value when partition count equals the number of unique leases — verifies single-entity
  partitioning intent (TR-13)
- Read two different partition files and assert their column names and dtypes match
  exactly (TR-14)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement transform stage entry point

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform(config: TransformConfig, client: distributed.Client) -> TransformSummary`

**Description:**
Orchestrate the full transform stage: load interim Parquet, apply all transformations
in sequence, write output Parquet. All transformations are composed as a lazy Dask
pipeline and executed in a single `.compute()` call via the Parquet write (ADR-005).

**Transformation sequence:**
1. Read interim Parquet from `config.input_dir` as a Dask DataFrame
2. Apply `derive_production_date` via `map_partitions`
3. Apply `remove_duplicates` via `map_partitions`
4. Apply `validate_physical_bounds` via `map_partitions`
5. Apply `cast_categoricals` via `map_partitions`
6. Apply `check_well_completeness` via `map_partitions`
7. Apply `index_and_sort`
8. Compute `n_partitions = max(10, min(ddf.npartitions, 50))`
9. Write output via `write_transform_parquet`

**No intermediate `.compute()` calls are permitted between steps 1–8** (ADR-005, TR-17).
All transformations must remain in the lazy task graph until the write in step 9.

**`TransformConfig` fields (dataclass):**
- `input_dir: Path` — interim Parquet directory (output of ingest)
- `output_dir: Path` — output directory for transformed Parquet
- `dict_path: Path` — path to data dictionary CSV

**`TransformSummary` fields (dataclass):**
- `output_dir: Path`
- `partition_count: int`

**Test cases (TR-17):**
- Assert the Dask DataFrame returned from each `map_partitions` application is of type
  `dask.dataframe.DataFrame` (not `pd.DataFrame`) — no intermediate `.compute()` (TR-17)
- Given a small synthetic interim Parquet dataset, assert `run_transform` completes
  without error and produces output Parquet files in `config.output_dir`
- Assert `TransformSummary.partition_count` equals `max(10, min(n, 50))`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement map_partitions meta schema consistency tests

**Module:** `tests/test_transform.py`

**Description:**
Write tests that verify the `meta=` argument for every `map_partitions` call in the
transform stage matches the actual function output schema. TR-23.

**Test cases (TR-23):**
For each of the following transform functions: `derive_production_date`,
`validate_physical_bounds`, `remove_duplicates`, `cast_categoricals`,
`check_well_completeness`:

- Construct a minimal synthetic input DataFrame matching the expected input schema
- Call the function on that input and capture the output schema (column names, dtypes)
- Construct a zero-row meta DataFrame using `ddf._meta.copy()` and call the same
  function on it
- Assert the output column list from the actual call matches the column list from
  the meta call exactly (including column order)
- Assert the output dtypes from the actual call match the dtypes from the meta call
  for every column

This verifies that the meta= argument supplied to `map_partitions` accurately reflects
the actual function output, preventing Dask metadata mismatch errors at compute time.

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 11: Implement schema stability cross-partition test

**Module:** `tests/test_transform.py`

**Description:**
After a complete transform run, read multiple output Parquet partition files and verify
that all have identical column names and dtypes. TR-14.

**Test cases (TR-14):**
- Run `run_transform` on a synthetic dataset that produces at least 2 partition files
- Read each partition file independently using `pandas.read_parquet`
- Assert the column names of each partition match those of the first partition exactly
- Assert the dtype of each column matches across partitions (including categorical dtype
  with the same declared categories)
- Assert the `production_date` column has dtype `datetime64[ns]` in every partition

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 12: Implement zero production preservation test

**Module:** `tests/test_transform.py`

**Description:**
Verify that zero production values in raw data are preserved as zeros (not nulled) through
the full transform pipeline. TR-05.

**Test cases (TR-05):**
- Create a synthetic partition with PRODUCTION = 0.0 for a valid MONTH-YEAR/PRODUCT row
- Run the partition through `validate_physical_bounds`
- Assert PRODUCTION remains 0.0 in the output (not `pd.NA`, not NaN)
- Create a second synthetic partition with PRODUCTION = -1.0
- Assert PRODUCTION becomes `pd.NA` in the output (negative → null, not zero)
- Verify that after a complete `run_transform`, zero-production rows from the input
  appear as zero-production rows in the output Parquet

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
