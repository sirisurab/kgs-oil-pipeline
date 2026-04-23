# Transform Stage Tasks

**Module:** `kgs_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

Read `stage-manifest-transform.md`, `boundary-ingest-transform.md`,
`boundary-transform-features.md`, `ADRs.md` (ADR-001, ADR-002, ADR-003, ADR-004,
ADR-005, ADR-007, ADR-008), and `build-env-manifest.md` before implementing any
task in this file.

Input state is defined in `boundary-ingest-transform.md`. Output state is defined
in `boundary-transform-features.md`. Every task must satisfy both contracts at its
respective stage boundary.

---

## Task T-01: Parse and validate the production date column

**Module:** `kgs_pipeline/transform.py`
**Function:** `parse_production_date(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. Derive a `production_date`
column of dtype `datetime64[ns]` from the `MONTH-YEAR` column (format `"M-YYYY"`).
Rows where `MONTH-YEAR` has a month component of `0` (yearly summary rows) or `-1`
(starting cumulative rows) must be dropped — they do not represent monthly
production records. Rows where the parsed date is invalid must be dropped and a
count logged at WARNING level. Return the partition with the `production_date`
column added; the `MONTH-YEAR` column must be retained as-is.

Row filtering using string operations must be done inside a partition-level function
(ADR-004 consequence).

**Error handling:**
- If `MONTH-YEAR` is absent from `df`, raise `KeyError`.
- Date parsing errors on individual rows must be dropped without raising; log the
  count at WARNING.

**Dependencies:** pandas, logging

**Test cases (unit):**
- Given a partition with valid `MONTH-YEAR` values `"1-2024"` and `"12-2024"`,
  assert `production_date` is parsed correctly to `datetime64` for both.
- Given rows with `MONTH-YEAR` month component `"0"` and `"-1"`, assert those rows
  are dropped.
- Given a row with an unparseable `MONTH-YEAR` value, assert it is dropped and no
  exception is raised.
- Assert `MONTH-YEAR` is still present in the returned DataFrame.
- Given a partition with `MONTH-YEAR` absent, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-02: Validate and clean production values

**Module:** `kgs_pipeline/transform.py`
**Function:** `clean_production_values(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. Apply the following cleaning
rules to the `PRODUCTION` column, logging all decisions (TR-12):
- Negative `PRODUCTION` values are physically invalid (TR-01) — replace them with
  `NA` and log the count at WARNING.
- Zero `PRODUCTION` values are valid measurements and must be preserved as `0.0`,
  not converted to `NA` (TR-05).
- Oil rates above 50,000 BBL/month for a single lease are treated as probable unit
  errors (TR-02) — replace with `NA` and log at WARNING; this bound applies only to
  rows where `PRODUCT == "O"`.
- Rows where `PRODUCT == "O"` and `PRODUCTION` is not in BBL units, or where
  `PRODUCT == "G"` and `PRODUCTION` is not in MCF units, must be flagged; the
  pipeline does not perform unit conversion — it replaces suspect values with `NA`
  and logs a WARNING.
- Do not modify the `WELLS` column in this function.

Return the cleaned partition with identical schema to the input.

**Error handling:**
- If `PRODUCTION` or `PRODUCT` columns are absent, raise `KeyError`.

**Dependencies:** pandas, logging

**Test cases (unit — TR-01, TR-02, TR-05, TR-12):**
- Given a row with `PRODUCTION = -100`, assert it is replaced with `NA` (TR-01).
- Given a row with `PRODUCTION = 0.0`, assert it remains `0.0` and is not nulled
  (TR-05).
- Given a row with `PRODUCT = "O"` and `PRODUCTION = 60000.0`, assert it is
  replaced with `NA` (TR-02 unit error bound).
- Given a row with `PRODUCT = "G"` and `PRODUCTION = 60000.0`, assert it is
  retained (the 50,000 bound applies to oil only).
- Assert a WARNING is logged for each replacement operation.
- Given a partition with `PRODUCTION` absent, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-03: Deduplicate records

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. Drop duplicate rows where
the combination of `LEASE_KID`, `MONTH-YEAR`, and `PRODUCT` is identical; keep the
first occurrence. Log the count of dropped duplicate rows at INFO level. Return the
deduplicated partition with the same schema as the input.

**Error handling:**
- If any of `LEASE_KID`, `MONTH-YEAR`, or `PRODUCT` are absent, raise `KeyError`.

**Dependencies:** pandas, logging

**Test cases (unit — TR-15):**
- Given a partition with two identical rows for the same `LEASE_KID`/`MONTH-YEAR`/
  `PRODUCT` combination, assert one is dropped.
- Assert the result row count is <= the input row count (TR-15).
- Assert that calling `deduplicate` twice on the same partition produces the same
  result as calling it once — idempotency (TR-15).
- Given a partition with no duplicates, assert no rows are dropped.
- Given a partition missing `LEASE_KID`, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-04: Cast categorical columns

**Module:** `kgs_pipeline/transform.py`
**Function:** `cast_categoricals(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. Cast `PRODUCT`, `TWN_DIR`,
and `RANGE_DIR` to `pd.CategoricalDtype` using their declared category sets from
`kgs_monthly_data_dictionary.csv` (`PRODUCT`: `["O", "G"]`; `TWN_DIR`: `["S", "N"]`;
`RANGE_DIR`: `["E", "W"]`). Values outside the declared set must be replaced with the
appropriate null sentinel before casting — never allowed to propagate (ADR-003).
Return the partition with the three columns recast and all other columns unchanged.

**Error handling:**
- If any of the three categorical columns is absent, raise `KeyError`.

**Dependencies:** pandas

**Test cases (unit — ADR-003, stage-manifest-transform.md H2):**
- Given a partition with only valid `PRODUCT` values `"O"` and `"G"`, assert
  `PRODUCT` is dtype `CategoricalDtype` with categories `["O", "G"]`.
- Given a partition with a `PRODUCT` value of `"X"` (outside the declared set),
  assert it is replaced with `NA` before the cast — not allowed to propagate as an
  unknown category.
- Assert `TWN_DIR` and `RANGE_DIR` are cast with their declared category sets.
- Given a partition with a categorical column absent, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-05: Fill gaps for continuous well date ranges

**Module:** `kgs_pipeline/transform.py`
**Function:** `fill_date_gaps(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition that has been grouped by
`LEASE_KID` and `PRODUCT`. For each such group, generate a complete monthly
`production_date` range spanning from the group's first to last observed date. For
months with no production record, insert a row with `PRODUCTION = 0.0` (zero is a
valid measurement for a shut-in month, not missing data — TR-05, TR-04). All other
columns for inserted rows must carry the appropriate null sentinel for their dtype.
Return the filled partition with the same schema as the input.

Per ADR-002 all per-entity aggregations must use vectorized grouped transformations.
Per-row iteration is prohibited.

**Error handling:**
- If `production_date`, `LEASE_KID`, or `PRODUCT` are absent, raise `KeyError`.

**Dependencies:** pandas, logging

**Test cases (unit — TR-04, TR-05):**
- Given a well with records for January and March 2024 (February absent), assert
  the returned DataFrame contains a February row with `PRODUCTION = 0.0`.
- Assert the inserted row has `PRODUCTION = 0.0`, not `NA` (TR-05).
- Given a well with a continuous date range, assert no rows are inserted.
- Assert total row count per well equals the number of months between first and
  last date inclusive (TR-04).
- Given a partition missing `production_date`, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-06: Orchestrate transform stage

**Module:** `kgs_pipeline/transform.py`
**Function:** `transform(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full transform stage. Read the interim Parquet from
the path in `config`. Per `boundary-ingest-transform.md`, the input carries
canonical schema, data-dictionary dtypes, and `max(10, min(n, 50))` partitions —
no schema reconciliation or re-partitioning is needed on read. Apply
`parse_production_date`, `clean_production_values`, `deduplicate`,
`cast_categoricals`, and `fill_date_gaps` via `map_partitions` in the order that
satisfies `stage-manifest-transform.md` H1 (sort after indexing) and H4 (`set_index`
is last structural operation before write). For every `map_partitions` call, derive
`meta` by calling the actual function on a zero-row copy of `_meta` — no
independently constructed meta is permitted (ADR-003). Sort each partition by
`production_date` within the Dask graph. Set the entity index to `LEASE_KID`
(`set_index` must be the last structural operation before write — H4). Repartition
to `max(10, min(n, 50))` and write to the processed Parquet path in `config`.
Return the Dask DataFrame (lazy — do not call `.compute()`, per TR-17 and ADR-005).

**Error handling:**
- If the interim Parquet path does not exist or is empty, raise `FileNotFoundError`.
- Per-partition errors must be surfaced; do not silently swallow partition-level
  exceptions.

**Dependencies:** dask, dask.dataframe, pandas, pyarrow, logging, pathlib

**Test cases (unit — TR-17):**
- Assert the return type is `dask.dataframe.DataFrame`, not `pd.DataFrame`.

**Test cases (TR-11 — data integrity, unit):**
- For a statistically meaningful sample of synthetic wells and months, assert
  that `PRODUCTION` values in the transform output match the input values for
  rows that are not cleaned (spot-check).

**Test cases (TR-12 — data cleaning validation, unit):**
- Assert nulls, missing values, outliers, and duplicates are handled per T-02
  and T-03 rules.
- Assert data types in the processed output match the expected types (datetime,
  float, string).

**Test cases (TR-13 — partition correctness, unit):**
- Assert that for a synthetic dataset with multiple `LEASE_KID` values, no single
  Parquet partition contains rows from multiple `LEASE_KID` values after
  `set_index`.

**Test cases (TR-14 — schema stability, unit):**
- Assert that the column names and dtypes of a partition sampled from one well's
  data match those of a partition sampled from another well's data.

**Test cases (TR-16 — sort stability, unit):**
- Given a dataset partitioned across multiple files for the same well, assert that
  the last `production_date` in one partition is earlier than the first
  `production_date` in the next partition.

**Test cases (TR-23 — map_partitions meta consistency, unit):**
- For every `map_partitions` call wrapping `parse_production_date`,
  `clean_production_values`, `deduplicate`, `cast_categoricals`, and `fill_date_gaps`,
  assert the `meta=` argument matches the function output in column names, column
  order, and dtypes by calling the function on `ddf._meta.copy()`.

**Test cases (TR-25 — transform integration test, integration):**
- Run `transform()` on the interim Parquet output of TR-24 using a config dict with
  all output paths pointing to pytest's `tmp_path`.
- Assert processed Parquet is readable.
- Assert output satisfies all upstream guarantees in `boundary-transform-features.md`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
