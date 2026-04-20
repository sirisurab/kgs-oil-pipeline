# Transform Stage — Task Specifications

**Module:** `kgs_pipeline/transform.py`
**Stage manifest:** `agent_docs/stage-manifest-transform.md`
**Boundary contracts:** `agent_docs/boundary-ingest-transform.md` (input),
`agent_docs/boundary-transform-features.md` (output)
**ADRs in force:** ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006, ADR-007, ADR-008

---

## Task TRN-01: Implement production date parser

**Module:** `kgs_pipeline/transform.py`
**Function:** `parse_production_date(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Convert the `MONTH-YEAR` string column to a `production_date` datetime column and
add it to the DataFrame. The `MONTH-YEAR` format is `"M-YYYY"` (e.g. `"1-2024"`).

The function must:
1. Split `MONTH-YEAR` on `"-"` to extract month (first element) and year (last
   element).
2. Construct a date as the first day of the given month and year
   (e.g. `"1-2024"` → `2024-01-01`).
3. Cast to `datetime64[ns]`.
4. Add the resulting series as the `production_date` column.
5. Drop the original `MONTH-YEAR` column.
6. Return the modified DataFrame.

Rows where `MONTH-YEAR` has month value `0` (yearly summary records indicated by
the data dictionary) must be dropped before parsing — they are not monthly
production records and must not be passed downstream.

**Error handling:**
- Rows where parsing fails after the month-0 filter must be replaced with `NaT` and
  logged at WARNING level — do not raise; NaT rows will be dropped in TRN-02.

**Dependencies:** pandas

**Test cases (unit):**
- Given a DataFrame with `MONTH-YEAR` values `"1-2024"` and `"12-2025"`, assert the
  `production_date` column contains `2024-01-01` and `2025-12-01` as datetime values.
- Given a row with `MONTH-YEAR` of `"0-2024"` (monthly=0, yearly summary), assert
  it is dropped from the result.
- Assert that the returned DataFrame does not contain the `MONTH-YEAR` column.
- Given a malformed `MONTH-YEAR` value that cannot be parsed, assert the row
  receives `NaT` (not an exception).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-02: Implement null and invalid value cleaner

**Module:** `kgs_pipeline/transform.py`
**Function:** `clean_invalid_values(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Remove rows with invalid or unresolvable values that must not propagate to the
features stage. The function must:

1. Drop rows where `production_date` is `NaT`.
2. Drop rows where `LEASE_KID` is null (non-nullable column — its absence indicates
   a structural problem per stage-manifest-ingest.md H3 semantics).
3. Drop rows where `PRODUCT` is null (non-nullable column).
4. Retain rows where `PRODUCTION` is `0.0` — zero production is a valid measurement
   distinguishable from missing data (TR-05). Do not drop or impute zero production
   values.
5. Where `PRODUCTION` is null (not zero), impute with `0.0` only if the row has a
   valid `WELLS` count greater than 0 (the lease was active but production was not
   reported). Otherwise leave as null.
6. Validate that `PRODUCTION` values are non-negative. Rows with `PRODUCTION < 0`
   must be dropped and logged at WARNING level (TR-01 — production volumes cannot
   be negative).
7. Return the cleaned DataFrame.

**Error handling:**
- All row-drop operations must be logged at DEBUG level with a count of dropped rows.

**Dependencies:** pandas, logging

**Test cases (unit — TR-01, TR-05):**
- Given a row with `PRODUCTION = 0.0`, assert it is retained in the output (TR-05).
- Given a row with `PRODUCTION = None`, assert it is handled per the imputation rule
  and not silently promoted to zero for inactive leases.
- Given a row with `PRODUCTION = -50.0`, assert it is dropped and a WARNING is
  logged (TR-01).
- Given a row with `NaT` in `production_date`, assert it is dropped.
- Given a row with null `LEASE_KID`, assert it is dropped.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-03: Implement unit range validator

**Module:** `kgs_pipeline/transform.py`
**Function:** `validate_production_units(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Validate that production volumes fall within realistic physical ranges. Oil
production is in BBL and gas production is in MCF. The function must:

1. For rows where `PRODUCT = "O"` (oil): flag rows where `PRODUCTION > 50000` as
   likely unit errors (TR-02 — oil rates above 50,000 BBL/month for a single well
   are almost certainly a unit error). Log these rows at WARNING level with the
   `LEASE_KID` and value. Do not drop them — log and retain for domain review.
2. For rows where `PRODUCT = "G"` (gas): no upper bound flagging is required (gas
   volumes in MCF can legitimately be large).
3. Return the DataFrame unchanged except for the log warnings emitted.

This function enforces the domain constraint in TR-02 without silent suppression.

**Dependencies:** pandas, logging

**Test cases (unit — TR-02):**
- Given an oil row with `PRODUCTION = 60000`, assert a WARNING is logged containing
  the lease ID and production value.
- Given an oil row with `PRODUCTION = 49000`, assert no WARNING is logged.
- Given a gas row with `PRODUCTION = 200000`, assert no WARNING is logged.
- Assert the returned DataFrame has the same number of rows as the input regardless
  of flagged values.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-04: Implement deduplicator

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Remove duplicate rows from a partition-level DataFrame. A duplicate is defined as
a row with identical values in `(LEASE_KID, PRODUCT, production_date)`. On
duplicates, retain the first occurrence. The function must:

1. Identify duplicate rows on the composite key `(LEASE_KID, PRODUCT,
   production_date)`.
2. Drop all but the first occurrence per group.
3. Return the deduplicated DataFrame.

The deduplication operation must be idempotent — running it twice on the same data
must produce the same result as running it once (TR-15).

**Dependencies:** pandas

**Test cases (unit — TR-15):**
- Given a DataFrame with 3 rows where 2 share the same `(LEASE_KID, PRODUCT,
  production_date)`, assert the returned DataFrame has 2 rows.
- Assert that running `deduplicate` twice on the same output produces the same row
  count as running it once (idempotency — TR-15).
- Given a DataFrame with no duplicates, assert the returned DataFrame has the same
  row count as the input.
- Assert the output row count is always <= the input row count (TR-15).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-05: Implement entity indexer and temporal sorter

**Module:** `kgs_pipeline/transform.py`
**Function:** `index_and_sort(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Set the entity index and sort by production date per the transform output state in
stage-manifest-transform.md. The function must:

1. Set `LEASE_KID` as the Dask DataFrame index using `ddf.set_index("LEASE_KID",
   drop=False, sorted=False)`. This triggers a distributed shuffle.
2. Sort rows by `production_date` within each partition using
   `ddf.map_partitions(lambda df: df.sort_values("production_date"))`.
3. Return the resulting Dask DataFrame.

The shuffle in step 1 destroys prior row ordering; the sort in step 2 re-establishes
temporal ordering per the stage-manifest-transform.md H1 hazard.

The `meta` argument for `map_partitions` must be derived by calling the actual sort
function on `ddf._meta.copy()` — not constructed separately (ADR-003).

**Error handling:**
- If `LEASE_KID` is not present as a column, raise `KeyError`.
- If `production_date` is not present, raise `KeyError`.

**Dependencies:** dask, pandas

**Test cases (unit — TR-16, TR-17):**
- Given a synthetic Dask DataFrame with rows in unsorted temporal order, assert
  that after `index_and_sort` the rows within each partition are sorted by
  `production_date` ascending.
- Assert the return type is `dask.dataframe.DataFrame` (not pandas — TR-17).
- Assert that sort stability is maintained: the last row date of one partition is
  <= the first row date of the next partition when the data is sorted globally
  (TR-16, verified on the computed result of a small synthetic dataset).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-06: Implement well completeness checker

**Module:** `kgs_pipeline/transform.py`
**Function:** `check_well_completeness(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
For each `(LEASE_KID, PRODUCT)` group, verify that the production record has no
unexpected gaps in its monthly sequence. A gap is defined as a span between two
consecutive `production_date` values that exceeds one calendar month. The function
must:

1. Group by `(LEASE_KID, PRODUCT)` and sort by `production_date` within each group.
2. Compute the difference between consecutive dates.
3. For any gap larger than 31 days, log a WARNING containing `LEASE_KID`, `PRODUCT`,
   and the date range of the gap.
4. Return the DataFrame unchanged — this function is diagnostic, not filtering.

This function implements TR-04 as a diagnostic validation. It does not modify data.

**Dependencies:** pandas, logging

**Test cases (unit — TR-04):**
- Given a lease with monthly records from January to December 2024 (12 rows), assert
  no WARNING is logged.
- Given a lease with records for January, February, and April 2024 (gap in March),
  assert a WARNING is logged identifying the gap.
- Assert the returned DataFrame has the same shape as the input.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-07: Implement transform map_partitions orchestrator

**Module:** `kgs_pipeline/transform.py`
**Function:** `apply_partition_transforms(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Chain all partition-level transform operations into a single `map_partitions` call
per logical step, keeping the Dask task graph lazy. The function must:

1. Apply `parse_production_date` via `map_partitions`.
2. Apply `clean_invalid_values` via `map_partitions`.
3. Apply `validate_production_units` via `map_partitions`.
4. Apply `deduplicate` via `map_partitions`.
5. Apply `check_well_completeness` via `map_partitions`.
6. Return the resulting Dask DataFrame without calling `.compute()` (ADR-005, TR-17).

For each `map_partitions` call, the `meta` argument must be derived by calling the
actual partition function on `ddf._meta.copy()` (ADR-003, TR-23).

Row filtering using string operations (e.g. filtering on `PRODUCT`) must be
performed inside the partition-level function, not on the Dask DataFrame directly
(ADR-004).

**Dependencies:** dask, pandas

**Test cases (unit — TR-17, TR-23):**
- Assert the return type is `dask.dataframe.DataFrame`.
- For each `map_partitions` call: assert that calling the wrapped function on
  `ddf._meta.copy()` produces output whose column names and dtypes match the `meta`
  passed to `map_partitions` (TR-23).
- Given a small synthetic Dask DataFrame, assert the task graph contains the
  expected number of `map_partitions` nodes (one per applied function).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-08: Implement transform Parquet writer

**Module:** `kgs_pipeline/transform.py`
**Function:** `write_transform(ddf: dask.dataframe.DataFrame, config: dict) -> None`

**Description:**
Write the transformed Dask DataFrame to partitioned Parquet files in the transform
output directory. The function must:

1. Resolve the output directory from `config["transform"]["processed_dir"]`; create
   it if absent.
2. Apply repartition to `max(10, min(n, 50))` where `n` is `ddf.npartitions`.
   Repartition must be the last operation before writing (ADR-004).
3. Write using `ddf.to_parquet(output_dir, write_index=True, overwrite=True)`.
4. Log the output path and partition count at INFO level.

**Error handling:**
- Propagate disk or permission errors with logging.

**Dependencies:** dask, pathlib, logging

**Test cases (integration — TR-13, TR-14, TR-18):**
- Given a small synthetic Dask DataFrame, assert written Parquet files are readable
  by `pandas.read_parquet` (TR-18).
- Assert that each Parquet partition contains only one unique `LEASE_KID` value if
  the DataFrame was partitioned per entity — or more precisely, assert no partition
  contains rows from well IDs that would be unexpected given the partition structure
  (TR-13).
- Assert that the schema (column names and dtypes) of two randomly sampled partitions
  are identical (TR-14).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-09: Implement data integrity spot-checker

**Module:** `kgs_pipeline/transform.py`
**Function:** `spot_check_integrity(interim_dir: Path, processed_dir: Path, sample_size: int) -> None`

**Description:**
Perform a spot-check comparing raw ingested data to transformed output for a random
sample of `(LEASE_KID, PRODUCT, production_date)` tuples. For each sampled tuple:

1. Locate the corresponding row in the interim Parquet files.
2. Locate the corresponding row in the processed Parquet files.
3. Assert that the `PRODUCTION` value in the processed file matches the raw interim
   value (no production value should be altered by cleaning unless it was invalid).
4. Log any mismatches at WARNING level.

This function implements TR-11. It is a diagnostic function — it logs mismatches but
does not raise.

**Dependencies:** dask, pandas, pathlib, logging

**Test cases (integration — TR-11):**
- Given synthetic interim and processed Parquet directories with matching data,
  assert the function completes with no WARNING logs.
- Given a processed file where a production value differs from interim, assert a
  WARNING is logged identifying the lease, product, date, and values.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task TRN-10: Wire transform stage entry point

**Module:** `kgs_pipeline/transform.py`
**Function:** `transform(config: dict, client) -> None`

**Description:**
Top-level entry point for the transform stage, called by the pipeline orchestrator.
The function must:

1. Load the interim Parquet using `dask.dataframe.read_parquet` from
   `config["transform"]["interim_dir"]`.
2. Call `apply_partition_transforms(ddf)` to chain partition-level transforms.
3. Call `index_and_sort(ddf)` to set the entity index and sort by date.
4. Call `write_transform(ddf, config)` to write output Parquet.
5. Call `spot_check_integrity` for diagnostic logging.
6. Log a stage-complete summary at INFO level.

The task graph must remain lazy until `write_transform` triggers computation
(ADR-005). No intermediate `.compute()` calls are permitted between steps 1–4.

**Dependencies:** dask, logging

**Test cases (unit — all mocked):**
- Given mocked reads and writes, assert `transform` completes without error and
  logs a stage-complete message.
- Assert that no `.compute()` call is made before `write_transform` is invoked
  (TR-17 — verify return type of intermediate results is `dask.dataframe.DataFrame`).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.
