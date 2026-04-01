# Transform Component — Task Specifications

## Overview

The transform component loads interim Parquet files produced by the ingest stage from
`data/interim/`, performs data cleaning (null handling, outlier capping, deduplication,
string standardisation, date validation, physical-bound enforcement), and writes the cleaned
result to `data/processed/clean/` as Parquet files. The output is per-lease production data
in tidy, ML-ready format with correct dtypes, no duplicates, physically valid values, and a
continuous monthly date index per lease. All heavy operations use Dask with `map_partitions`
for partition-level parallelism.

**Module:** `kgs_pipeline/transform.py`
**Entry-point CLI:** invoked by `pipeline.py --transform`
**Test file:** `tests/test_transform.py`

---

## Canonical column schema after transform

After the transform stage completes, the processed Parquet files must contain at minimum
the following columns with these types:

| Column name       | Dtype               | Notes                                             |
|-------------------|---------------------|---------------------------------------------------|
| LEASE_KID         | pd.StringDtype()    | Lease identifier                                  |
| LEASE             | pd.StringDtype()    | Lease name (uppercased, stripped)                 |
| API_NUMBER        | pd.StringDtype()    | Well API numbers (uppercased, stripped)           |
| FIELD             | pd.StringDtype()    | Field name (uppercased, stripped)                 |
| PRODUCING_ZONE    | pd.StringDtype()    | Formation (uppercased, stripped)                  |
| OPERATOR          | pd.StringDtype()    | Operator name (uppercased, stripped)              |
| COUNTY            | pd.StringDtype()    | County (uppercased, stripped)                     |
| PRODUCT           | pd.StringDtype()    | "O" or "G" (uppercased, stripped)                 |
| MONTH-YEAR        | pd.StringDtype()    | Original "M-YYYY" string retained                 |
| production_date   | datetime64[ns]      | First day of the production month                 |
| PRODUCTION        | float64             | Oil BBL or gas MCF; zero is valid, negatives capped|
| WELLS             | float64             | Number of wells; may be NaN                       |
| LATITUDE          | float64             | May be NaN                                        |
| LONGITUDE         | float64             | May be NaN                                        |
| source_file       | pd.StringDtype()    | Raw source filename                               |

---

## Design decisions and constraints

- All Parquet reads and writes use Dask. No `.compute()` calls inside module functions — all
  functions return `dask.dataframe.DataFrame`. `[TR-17]`
- After reading interim Parquet, immediately repartition to `min(npartitions, 50)`.
- String filtering and row-level operations must be implemented inside `map_partitions`
  functions. Do not apply `.str` accessor directly on Dask Series. All `meta=` arguments use
  `pd.StringDtype()` for string columns, never `"object"`.
- Output file count: repartition to `max(1, total_rows // 500_000)` before writing. Never
  more than 200 output files.
- Zero PRODUCTION values are valid measurements and must be preserved as-is throughout. `[TR-05]`
- Negative PRODUCTION values are physically impossible and must be capped to 0.0 during
  cleaning. `[TR-01]`
- Physical-bound violations for PRODUCTION values above the unit-error threshold
  (> 50,000 BBL/month for a single lease record) must be logged as WARNING but are not removed —
  they are capped via the IQR outlier method. `[TR-02]`
- Deduplication is on the composite key `(LEASE_KID, MONTH-YEAR, PRODUCT)`. `[TR-15]`
- String standardisation: uppercase + strip all string-typed columns.
- `requirements.txt` must be updated with all third-party packages imported in this module.

---

## Task 01: Implement interim Parquet reader

**Module:** `kgs_pipeline/transform.py`
**Function:** `load_interim(interim_dir: str) -> dask.dataframe.DataFrame`

**Description:**
Read all Parquet files from `interim_dir` into a Dask DataFrame. Immediately repartition to
`min(npartitions, 50)` partitions before returning. Raise `ValueError` if `interim_dir` is
empty or contains no Parquet files.

**Inputs:** `interim_dir: str` — path to `data/interim/`.

**Outputs:** `dask.dataframe.DataFrame` — repartitioned, not yet computed.

**Edge cases:**
- Directory does not exist → raise `FileNotFoundError`.
- Directory exists but has no Parquet files → raise `ValueError`.

**Test cases:**
- `@pytest.mark.unit` — Write a minimal Parquet file to `tmp_path`, call `load_interim`; assert
  the return type is `dask.dataframe.DataFrame`. `[TR-17]`
- `@pytest.mark.unit` — Assert the returned DataFrame has `npartitions <= 50`.
- `@pytest.mark.unit` — Given an empty directory, assert `ValueError` is raised.
- `@pytest.mark.integration` — Load from actual `data/interim/`; assert return type and
  partition count. `[TR-18]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement null handling

**Module:** `kgs_pipeline/transform.py`
**Function:** `handle_nulls(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Handle missing values using `map_partitions` to apply per-partition Pandas logic:
1. Numeric columns (`PRODUCTION`, `WELLS`, `LATITUDE`, `LONGITUDE`): impute NaN with the
   **partition-level median**. If the entire column in a partition is NaN, leave as NaN
   (do not impute with 0 or a cross-partition median — this avoids computing a global stat
   inside Dask).
2. Categorical/string columns (`LEASE`, `OPERATOR`, `COUNTY`, `PRODUCT`, `PRODUCING_ZONE`,
   `FIELD`): impute NaN/NA with the partition-level **mode** (most frequent value). If the
   column is entirely null in a partition, fill with the sentinel string `"UNKNOWN"`.
3. `LEASE_KID`, `API_NUMBER`, `MONTH-YEAR`, `source_file`: do not impute — these are identity
   columns; rows where these are null should be DROPPED.
4. After dropping identity-null rows, return the modified Dask DataFrame.

All operations must be inside `map_partitions` with correct `meta=` typed with
`pd.StringDtype()` for string columns.

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Given a partition with 3 rows where PRODUCTION is [100.0, NaN, 200.0],
  assert the NaN is imputed to 150.0 (median).
- `@pytest.mark.unit` — Given a partition where COUNTY is ["Allen", NaN, "Allen"], assert the
  NaN is imputed to "Allen" (mode).
- `@pytest.mark.unit` — Given a partition where LEASE_KID is null for one row, assert that row
  is dropped. `[TR-12]`
- `@pytest.mark.unit` — Assert that a row with PRODUCTION=0.0 (explicit zero) is NOT imputed.
  `[TR-05]`
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`. `[TR-17]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement duplicate removal

**Module:** `kgs_pipeline/transform.py`
**Function:** `remove_duplicates(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Remove duplicate records based on the composite key `(LEASE_KID, MONTH-YEAR, PRODUCT)`.
Implementation steps:
1. Use `map_partitions` to apply `pd.DataFrame.drop_duplicates(subset=[...], keep="first")`
   within each partition.
2. After partition-level deduplication, apply a cross-partition deduplication by computing the
   full DataFrame, deduplicating globally, and converting back to Dask. This is required because
   duplicates may span partition boundaries after the repartition step. Call `.compute()`
   **only for this cross-partition dedup step**, then immediately convert back to Dask via
   `dask.dataframe.from_pandas`.
3. Repartition the result to `min(npartitions, 50)` before returning.
4. Return `dask.dataframe.DataFrame`.

The post-dedup row count must be <= the pre-dedup row count (never greater). `[TR-15]`
Running this function twice must produce the same row count as running it once (idempotent).
`[TR-15]`

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with 5 rows where 2 rows share the same
  `(LEASE_KID, MONTH-YEAR, PRODUCT)`, assert output has 4 rows (one duplicate removed).
  `[TR-15]`
- `@pytest.mark.unit` — Apply `remove_duplicates` twice to the same input; assert the row
  count after the second call equals the row count after the first call (idempotency). `[TR-15]`
- `@pytest.mark.unit` — Assert that row count after deduplication is <= row count before. `[TR-15]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement physical-bound enforcement and outlier capping

**Module:** `kgs_pipeline/transform.py`
**Function:** `cap_outliers(ddf: dask.dataframe.DataFrame, iqr_multiplier: float = 1.5) -> dask.dataframe.DataFrame`

**Description:**
Apply outlier detection and capping to the `PRODUCTION` column using the IQR method:
1. **Physical lower bound:** any PRODUCTION value < 0.0 is capped to 0.0 first. Negative
   production is physically impossible regardless of statistical distribution. `[TR-01]`
2. **IQR upper bound:** compute Q1 and Q3 across all data (this requires `.compute()` once to
   get global quantiles — call `.compute()` only on the quantile calculation, not the full
   DataFrame). Upper fence = Q3 + `iqr_multiplier` × (Q3 − Q1). Any value above the upper
   fence is capped to the upper fence value.
3. **Unit-error warning:** after capping, if any PRODUCTION value > 50,000.0 remains for a
   single monthly record (O product, oil in BBL), log a WARNING listing the LEASE_KID and
   MONTH-YEAR. These records are suspicious but are not removed. `[TR-02]`
4. Apply the capping via `map_partitions` so the actual transformation is lazy.
5. Return `dask.dataframe.DataFrame`.

**Inputs:**
- `ddf: dask.dataframe.DataFrame`
- `iqr_multiplier: float` — IQR fence multiplier (default 1.5, configurable via pipeline config).

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Given a PRODUCTION column with values [100, 200, 150, -50, 99999],
  assert the -50 is capped to 0.0 and the 99999 is capped to the IQR upper fence value. `[TR-01]`
- `@pytest.mark.unit` — Assert no PRODUCTION value in the output is < 0.0. `[TR-01]`
- `@pytest.mark.unit` — Assert no PRODUCTION value exceeds the computed upper fence after capping.
- `@pytest.mark.unit` — Given a PRODUCTION value of exactly 0.0, assert it is NOT changed
  (physical zero is valid). `[TR-05]`
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`. `[TR-17]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement string standardisation

**Module:** `kgs_pipeline/transform.py`
**Function:** `standardise_strings(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Standardise all string-typed columns to uppercase with no leading/trailing whitespace.
String columns to standardise: `LEASE`, `OPERATOR`, `COUNTY`, `PRODUCT`, `PRODUCING_ZONE`,
`FIELD`, `API_NUMBER`, `LEASE_KID`, `SPOT`, `TWN_DIR`, `RANGE_DIR`.

Implement using `map_partitions`. For each string column, apply `.str.upper().str.strip()`.
Do not modify numeric or datetime columns.

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame` with standardised string columns.

**Test cases:**
- `@pytest.mark.unit` — Given COUNTY values [" Allen ", "barton", "CHASE"], assert output is
  ["ALLEN", "BARTON", "CHASE"].
- `@pytest.mark.unit` — Given PRODUCT values ["o", "g", " O "], assert output is ["O", "G", "O"].
- `@pytest.mark.unit` — Assert numeric column PRODUCTION is unchanged by this function.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement production_date derivation

**Module:** `kgs_pipeline/transform.py`
**Function:** `derive_production_date(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Create a `production_date` column of type `datetime64[ns]` from the `MONTH-YEAR` string column.
Steps:
1. Inside a `map_partitions` function, parse "M-YYYY" by splitting on "-" and constructing a
   date string "YYYY-MM-01" (zero-pad the month to 2 digits).
2. Use `pd.to_datetime` with `format="%Y-%m-%d"` to convert. Non-parseable values become NaT.
3. Drop rows where `production_date` is NaT.
4. The `MONTH-YEAR` string column is retained alongside `production_date`.

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame` with an additional `production_date` column.

**Edge cases:**
- MONTH-YEAR "1-2024" → production_date `2024-01-01`.
- MONTH-YEAR "12-2024" → production_date `2024-12-01`.
- Malformed MONTH-YEAR → NaT → row is dropped.
- MONTH-YEAR "0-2024" (yearly summary) → should have been filtered in ingest; if present here,
  it will produce an invalid month → NaT → dropped.

**Test cases:**
- `@pytest.mark.unit` — Given MONTH-YEAR "3-2024", assert production_date is `pd.Timestamp("2024-03-01")`.
- `@pytest.mark.unit` — Given MONTH-YEAR "12-2024", assert production_date is `pd.Timestamp("2024-12-01")`.
- `@pytest.mark.unit` — Given a malformed MONTH-YEAR value "bad-data", assert the row is
  dropped (NaT → drop).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement well completeness check

**Module:** `kgs_pipeline/transform.py`
**Function:** `check_well_completeness(ddf: dask.dataframe.DataFrame) -> pd.DataFrame`

**Description:**
Produce a diagnostic summary DataFrame that, for each `(LEASE_KID, PRODUCT)` group, reports:
- `first_date`: earliest `production_date`
- `last_date`: latest `production_date`
- `expected_months`: number of calendar months between first and last date (inclusive)
- `actual_months`: count of distinct `production_date` values
- `gap_months`: `expected_months - actual_months` (number of missing monthly records)
- `has_gaps`: boolean, True if `gap_months > 0`

This function calls `.compute()` to materialise the result as a Pandas DataFrame (it is a
diagnostic/reporting function, not a transformation function). Log a WARNING for each
LEASE_KID/PRODUCT pair that has `has_gaps == True`, listing the gap count.

This function is called by `run_transform` after writing the cleaned output, for logging
purposes only — its return value is not written to Parquet. `[TR-04]`

**Inputs:** `ddf: dask.dataframe.DataFrame` — cleaned Dask DataFrame with `production_date`.

**Outputs:** `pd.DataFrame` — one row per `(LEASE_KID, PRODUCT)` with completeness stats.

**Test cases:**
- `@pytest.mark.unit` — Given a lease with production months Jan-2024, Feb-2024, Apr-2024
  (March missing), assert `gap_months == 1` and `has_gaps == True`. `[TR-04]`
- `@pytest.mark.unit` — Given a lease with a continuous Jan-2024 to Dec-2024 (12 months),
  assert `gap_months == 0` and `has_gaps == False`. `[TR-04]`
- `@pytest.mark.unit` — Assert the output is a `pd.DataFrame` (this function is allowed to
  call `.compute()`).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement data integrity spot-check

**Module:** `kgs_pipeline/transform.py`
**Function:** `spot_check_integrity(raw_dir: str, clean_ddf: dask.dataframe.DataFrame, sample_n: int = 20) -> pd.DataFrame`

**Description:**
Randomly sample `sample_n` `(LEASE_KID, MONTH-YEAR, PRODUCT)` combinations from the cleaned
Dask DataFrame. For each sampled combination, read the corresponding raw file (using
`source_file` to identify it), look up the matching raw row(s), and compare the PRODUCTION
value to the cleaned value. Return a Pandas DataFrame with columns:
`LEASE_KID`, `MONTH-YEAR`, `PRODUCT`, `raw_production`, `clean_production`, `match`.

`match` is True if `abs(raw_production - clean_production) < 1e-6` (no transformation applied)
or if the cleaned value equals the IQR upper fence (capped outlier — expected difference).

This function calls `.compute()` (it is a diagnostic function). `[TR-11]`

**Inputs:**
- `raw_dir: str` — directory containing original raw `.txt` files.
- `clean_ddf: dask.dataframe.DataFrame`
- `sample_n: int` — number of records to spot-check.

**Outputs:** `pd.DataFrame` — spot-check results with `match` column.

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic raw file and a clean Dask DataFrame built from it
  (with no outlier capping applied), assert all `match` values are True. `[TR-11]`
- `@pytest.mark.unit` — Given a row with a capped outlier in the clean data, assert the
  `match` column for that row is True (capped value is an accepted difference). `[TR-11]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement transform orchestrator

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform(interim_dir: str, output_dir: str, raw_dir: str, iqr_multiplier: float = 1.5) -> dask.dataframe.DataFrame`

**Description:**
Main entry point for the transform stage. Chains all cleaning steps and writes output:
1. `load_interim(interim_dir)` → `ddf`
2. `handle_nulls(ddf)` → `ddf`
3. `remove_duplicates(ddf)` → `ddf`
4. `cap_outliers(ddf, iqr_multiplier)` → `ddf`
5. `standardise_strings(ddf)` → `ddf`
6. `derive_production_date(ddf)` → `ddf`
7. Repartition to `max(1, estimated_rows // 500_000)` — estimate rows from metadata without
   calling `.compute()` on the full DataFrame.
8. Write to `output_dir` via `ddf.to_parquet(output_dir, write_index=False)`.
9. Re-read from `output_dir`: `ddf_clean = dask.dataframe.read_parquet(output_dir)`.
10. Repartition to `min(npartitions, 50)`.
11. Call `check_well_completeness(ddf_clean)` (diagnostic logging only).
12. Call `spot_check_integrity(raw_dir, ddf_clean)` (diagnostic logging only).
13. Return `ddf_clean`.

The returned Dask DataFrame must NOT have been computed. `[TR-17]`

**Inputs:**
- `interim_dir: str` — path to interim Parquet files.
- `output_dir: str` — path to `data/processed/clean/`.
- `raw_dir: str` — path to `data/raw/` (used by spot-check only).
- `iqr_multiplier: float` — passed to `cap_outliers`.

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Run `run_transform` on a `tmp_path`-based fixture with 10 synthetic
  rows; assert return type is `dask.dataframe.DataFrame`. `[TR-17]`
- `@pytest.mark.unit` — Assert the output Parquet directory contains between 1 and 200 files.
- `@pytest.mark.unit` — Read back the written Parquet with a fresh `dask.dataframe.read_parquet`
  call; assert it succeeds without error. `[TR-18]`
- `@pytest.mark.unit` — Assert all required schema columns are present in the output. `[TR-14]`
- `@pytest.mark.unit` — Assert `production_date` column has dtype `datetime64[ns]`. `[TR-12]`
- `@pytest.mark.unit` — Assert no PRODUCTION value < 0.0 in the output. `[TR-01]`
- `@pytest.mark.unit` — Assert row count after transform <= row count in interim input. `[TR-15]`
- `@pytest.mark.unit` — Assert zero PRODUCTION values are preserved (not nulled or dropped).
  `[TR-05]`
- `@pytest.mark.unit` — Sample two partition files from output, assert identical column names
  and dtypes (schema stability). `[TR-14]`
- `@pytest.mark.integration` — Run against actual `data/interim/`; assert output is in
  `data/processed/clean/` and readable. `[TR-18]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement sort stability and partition correctness tests

**Module:** `tests/test_transform.py`
**Test functions:** `test_sort_stability`, `test_partition_single_well_correctness`

**Description:**

**`test_sort_stability`** `[TR-16]`:
Write a fixture that produces a cleaned Parquet output partitioned into at least 2 files for
the same lease. For each adjacent pair of Parquet files for the same `LEASE_KID`, read them
in order and assert that the last `production_date` value in file N is earlier than or equal
to the first `production_date` value in file N+1. Sort stability must hold across partition
boundaries.

**`test_partition_single_well_correctness`** `[TR-13]`:
Write a fixture that produces a multi-partition Parquet output. For each partition file,
read it as a Pandas DataFrame and assert that all rows in that partition share the same
`LEASE_KID` if partition_on was used. If no `partition_on` was used, this test verifies
the data is not mixed across wells in any single partition.

Note: given the non-negotiable constraint that `partition_on` with a high-cardinality column
is forbidden, the transform stage writes without `partition_on`. The test instead verifies
that sort order is maintained and that data integrity is upheld.

**Test cases:**
- `@pytest.mark.unit` — `test_sort_stability`: assert last date in partition N < first date in
  partition N+1 for the same sorted partition sequence. `[TR-16]`
- `@pytest.mark.unit` — `test_partition_single_well_correctness`: assert that schema is
  consistent (same column names and dtypes) across all sampled partitions. `[TR-13, TR-14]`
- `@pytest.mark.integration` — Run both tests against actual `data/processed/clean/` output.

**Definition of done:** Test functions implemented, all assertions pass, ruff and mypy report
no errors.
