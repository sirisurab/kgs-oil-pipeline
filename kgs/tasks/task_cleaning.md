# Task Specification: Data Cleaning Component

**Module:** `kgs/src/cleaning.py`
**Component purpose:** Accept the unified raw Dask DataFrame from the ingestion stage
and produce a cleaned Dask DataFrame with: standardised snake_case column names,
correct data types, deduplicated rows, invalid records removed, outliers flagged,
zero-production records preserved (not coerced to null), and all domain physical
constraints enforced. Output is a lazy Dask DataFrame ready for the processing stage.

---

## Overview

Raw KGS data presents several well-known data quality issues:
- Column names are UPPER_CASE with hyphens; need snake_case conversion.
- `PRODUCTION` values are mixed with null, empty string, and non-numeric text.
- `MONTH_YEAR` is a combined string such as `"1/2020"` or `"0/2020"` (Month=0
  means a yearly total row; Month=-1 means a starting cumulative row).
- `PRODUCT` can be `"O"` (oil, BBL) or `"G"` (gas, MCF).
- Negative `PRODUCTION` values are physically impossible and indicate sensor
  errors or data entry errors.
- Duplicate rows can arise from multi-year file overlap.
- Oil production above 50,000 BBL/month per lease is an extreme outlier
  almost certainly indicating a unit error.
- Zero production is a valid measured state and must NOT be converted to null.

**Canonical snake_case column mapping from ingestion UPPER_CASE output:**

| Ingestion column  | Cleaned column      |
|-------------------|---------------------|
| LEASE_KID         | lease_kid           |
| LEASE             | lease_name          |
| DOR_CODE          | dor_code            |
| API_NUMBER        | api_number          |
| FIELD             | field               |
| PRODUCING_ZONE    | producing_zone      |
| OPERATOR          | operator            |
| COUNTY            | county              |
| TOWNSHIP          | township            |
| TWN_DIR           | twn_dir             |
| RANGE             | range_num           |
| RANGE_DIR         | range_dir           |
| SECTION           | section             |
| SPOT              | spot                |
| LATITUDE          | latitude            |
| LONGITUDE         | longitude           |
| MONTH_YEAR        | month_year          |
| PRODUCT           | product             |
| WELLS             | wells               |
| PRODUCTION        | production          |
| URL               | url                 |
| source_year       | source_year         |

---

## Task 01: Define cleaning configuration and column name standardiser

**Module:** `kgs/src/cleaning.py`
**Function/Class:** `CleaningConfig` (dataclass), `standardise_column_names(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Define a frozen dataclass `CleaningConfig` and implement the column-renaming step.

**CleaningConfig fields:**
- `oil_production_upper_bound: float` — max plausible oil BBL/month per lease,
  default `50_000.0`
- `gas_production_upper_bound: float` — max plausible gas MCF/month per lease,
  default `500_000.0`
- `flag_outliers: bool` — if True, add boolean flag columns instead of dropping
  outlier rows, default `True`
- `drop_yearly_summary_rows: bool` — if True, drop rows where month parsed from
  `month_year` equals 0 (yearly totals) or -1 (starting cumulative), default `True`
- `dedup_subset: list[str]` — columns to use for duplicate detection, default
  `["lease_kid", "month_year", "product"]`

**standardise_column_names behaviour:**
- Apply the canonical mapping table (defined as a module-level constant dict).
- Any column not in the mapping is passed through unchanged.
- Return a Dask DataFrame with renamed columns.
- Must not call `.compute()`.

**Error handling:**
- Raise `CleaningError` (custom exception) if none of the expected canonical
  columns are found after renaming (indicates completely unexpected schema).

**Dependencies:** dask, dataclasses

**Test cases:**
- `@pytest.mark.unit` Create a Dask DataFrame with UPPER_CASE columns matching
  the ingestion output; assert `standardise_column_names` returns a Dask DataFrame
  with the correct snake_case names.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame` (not
  pandas), confirming no `.compute()` was called.
- `@pytest.mark.unit` Given a DataFrame with no recognisable canonical columns,
  assert `CleaningError` is raised.
- `@pytest.mark.unit` Assert extra columns not in the mapping are still present
  in the output (pass-through behaviour).

**Definition of done:** Dataclass, function, and `CleaningError` are implemented,
all test cases pass, ruff and mypy report no errors, requirements.txt updated with
all third-party packages imported in this task.

---

## Task 02: Implement data type coercion

**Module:** `kgs/src/cleaning.py`
**Function:** `coerce_dtypes(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Convert all columns from string dtype (as ingested) to their correct types.

**Type coercion rules:**
- `production`: coerce to `float64` using `pandas.to_numeric(errors="coerce")`.
  Non-parseable strings become `NaN` (not zero — zero is a valid string `"0"`
  which must parse as `0.0`).
- `wells`: coerce to `Int64` (nullable integer) using `pandas.to_numeric(errors="coerce")`.
- `latitude`, `longitude`: coerce to `float64` using `pandas.to_numeric(errors="coerce")`.
- `township`, `section`: coerce to `Int64` (nullable integer).
- `source_year`: coerce to `int64`.
- `lease_kid`, `dor_code`, `api_number`, `field`, `producing_zone`, `operator`,
  `county`, `twn_dir`, `range_dir`, `spot`, `product`, `url`, `lease_name`,
  `range_num`, `month_year`: retain as `string` (pandas StringDtype).
- `month_year` is left as string at this stage; parsed into a proper date in
  Task 03.

**Behaviour:**
- Apply coercions using `dask.dataframe.map_partitions` with a pandas function
  so coercion is deferred (lazy).
- Log a DEBUG message with the resulting dtypes after coercion plan is set up.
- Must not call `.compute()`.

**Error handling:**
- Do not raise on coercion failures; `pandas.to_numeric(errors="coerce")` will
  produce NaN for un-parseable values. The null-handling step (Task 04) handles
  these downstream.

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide a partition with `production` containing `"100.5"`,
  `"0"`, `""`, `"N/A"`, and `None`; assert coerced column has values `100.5`,
  `0.0`, `NaN`, `NaN`, `NaN`.
- `@pytest.mark.unit` Assert that string `"0"` for `production` becomes `0.0`
  (not NaN), confirming zero preservation.
- `@pytest.mark.unit` Assert `wells` column becomes `Int64` dtype.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert `latitude` and `longitude` become `float64`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 03: Implement month_year parser and date column builder

**Module:** `kgs/src/cleaning.py`
**Function:** `parse_month_year(df: dask.dataframe.DataFrame, drop_summary_rows: bool = True) -> dask.dataframe.DataFrame`

**Description:**
Parse the `month_year` string field into usable date components.

**KGS month_year format:**
The raw value is `"{month}/{year}"` (e.g. `"1/2020"`, `"12/2024"`).
Special values:
- Month = `0` → yearly summary row (not a monthly record).
- Month = `-1` → starting cumulative row.

**Behaviour:**
- Split `month_year` on `"/"` to extract integer `month` and integer `year`.
- Add column `prod_month: Int64` — the month integer (1–12 for valid monthly rows).
- Add column `prod_year: Int64` — the year integer.
- Add column `prod_date: datetime64[ns]` — constructed as the first day of the
  month: `datetime(year, month, 1)`. Set to `NaT` for rows where month ≤ 0.
- If `drop_summary_rows=True`, drop rows where the parsed month ≤ 0 (yearly
  and cumulative summary rows are not monthly production records).
- Retain the original `month_year` string column.
- Must not call `.compute()`.

**Error handling:**
- Rows where `month_year` cannot be parsed (malformed string) should produce
  `NaT` for `prod_date` and `NA` for `prod_month`/`prod_year` rather than raising.
- Log a WARNING with the count of rows that failed to parse.

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide `month_year = "3/2022"`; assert `prod_month=3`,
  `prod_year=2022`, `prod_date=datetime(2022, 3, 1)`.
- `@pytest.mark.unit` Provide `month_year = "0/2022"` with `drop_summary_rows=True`;
  assert the row is dropped.
- `@pytest.mark.unit` Provide `month_year = "0/2022"` with `drop_summary_rows=False`;
  assert the row is retained and `prod_date=NaT`.
- `@pytest.mark.unit` Provide a malformed `month_year = "bad/data"`; assert no
  exception is raised and `prod_date=NaT`.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 04: Implement null and missing value handler

**Module:** `kgs/src/cleaning.py`
**Function:** `handle_nulls(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Standardise how missing values are represented, without imputing them or
converting valid zero-production rows to null.

**Behaviour:**
- For string columns: replace empty strings `""` and strings containing only
  whitespace with `pandas.NA`.
- For string columns: replace any of the known KGS null-like tokens
  (`"N/A"`, `"NA"`, `"None"`, `"none"`, `"--"`) with `pandas.NA`.
- For numeric columns (`production`, `wells`, `latitude`, `longitude`):
  do NOT replace `0` or `0.0` with `NA` — zero is a valid measurement.
  Only `NaN` (from failed coercion) represents missingness.
- `prod_date` rows with `NaT` are already flagged; do not drop them here
  (that is handled by Task 03 for summary rows; remaining `NaT` may indicate
  parse failures and should be retained for audit).
- Log a DEBUG summary of null counts per column after this step.
- Must not call `.compute()`.

**Error handling:**
- Must not raise; handle all replacements gracefully.

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide `operator = ""`; assert it becomes `pandas.NA`.
- `@pytest.mark.unit` Provide `operator = "N/A"`; assert it becomes `pandas.NA`.
- `@pytest.mark.unit` Provide `production = 0.0`; assert it remains `0.0` (not NA).
- `@pytest.mark.unit` Provide `production = NaN`; assert it remains `NaN`.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 05: Implement domain physical-bounds validator and filter

**Module:** `kgs/src/cleaning.py`
**Function:** `validate_physical_bounds(df: dask.dataframe.DataFrame, config: CleaningConfig) -> dask.dataframe.DataFrame`

**Description:**
Enforce domain physical constraints on production data. These rules derive from
physical laws of oil and gas production and any violation represents either a
sensor error or a pipeline bug.

**Physical constraint rules:**
1. `production` must be ≥ 0.0. Negative values are physically impossible.
2. Oil production (`product == "O"`) above `config.oil_production_upper_bound`
   BBL/month per lease is treated as an outlier (likely a unit error).
3. Gas production (`product == "G"`) above `config.gas_production_upper_bound`
   MCF/month per lease is treated as an outlier.
4. `prod_month` must be in the range 1–12 for monthly records.
5. `prod_year` must be in the range 2000–2030.
6. `product` must be one of `{"O", "G"}`. Invalid product codes should be
   flagged/removed.

**Behaviour:**
- For rules 1, 4, 5, 6: add a boolean column `is_invalid` that is `True`
  for rows violating any of these constraints. Then drop rows where
  `is_invalid=True`.
- For rules 2 and 3 (outliers): if `config.flag_outliers=True`, add a boolean
  column `is_outlier` set to `True` for rows exceeding the bounds, but do NOT
  drop them. If `config.flag_outliers=False`, drop outlier rows.
- Drop the `is_invalid` working column before returning.
- Retain the `is_outlier` column in the output if `config.flag_outliers=True`.
- Log a WARNING with the count of invalid rows removed and outlier rows flagged.
- Must not call `.compute()`.

**Error handling:**
- If `product` column is entirely null, log a WARNING; do not raise.

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide a row with `production = -5.0`; assert it is removed.
- `@pytest.mark.unit` Provide an oil row with `production = 100_000.0` (above bound)
  and `flag_outliers=True`; assert row is retained with `is_outlier=True`.
- `@pytest.mark.unit` Provide an oil row with `production = 100_000.0` and
  `flag_outliers=False`; assert row is dropped.
- `@pytest.mark.unit` Provide a row with `production = 0.0`; assert it is retained
  (zero is valid).
- `@pytest.mark.unit` Provide a row with `product = "X"` (invalid code); assert
  it is removed.
- `@pytest.mark.unit` Provide a row with `prod_month = 13`; assert it is removed.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 06: Implement deduplication

**Module:** `kgs/src/cleaning.py`
**Function:** `remove_duplicates(df: dask.dataframe.DataFrame, config: CleaningConfig) -> dask.dataframe.DataFrame`

**Description:**
Remove duplicate records that can arise from overlapping annual file contents.

**Behaviour:**
- Use `config.dedup_subset` as the set of columns to identify duplicate rows
  (default: `["lease_kid", "month_year", "product"]`).
- Keep the first occurrence, drop subsequent duplicates.
- Use `dask.dataframe.DataFrame.drop_duplicates(subset=config.dedup_subset,
  split_out=...)` if available, otherwise apply per-partition deduplication
  followed by a cross-partition deduplication via `dask.dataframe.map_partitions`.
- Log an INFO message with the count of rows removed.
- Must not call `.compute()`.
- The operation must be idempotent: running `remove_duplicates` twice on the
  same data must produce the same output as running it once.

**Error handling:**
- If any column in `dedup_subset` is not present in the DataFrame, raise
  `CleaningError`.

**Dependencies:** dask, logging

**Test cases:**
- `@pytest.mark.unit` Provide a DataFrame with 5 rows, 2 of which are duplicates
  on `["lease_kid", "month_year", "product"]`; assert output has 3 rows.
- `@pytest.mark.unit` Run `remove_duplicates` twice on the same input; assert
  the output row count is the same as running it once (idempotency).
- `@pytest.mark.unit` Provide a `dedup_subset` containing a non-existent column;
  assert `CleaningError` is raised.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert the post-deduplication row count is ≤ the input row
  count, never greater.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 07: Implement top-level cleaning orchestrator

**Module:** `kgs/src/cleaning.py`
**Function:** `clean(df: dask.dataframe.DataFrame, config: CleaningConfig) -> dask.dataframe.DataFrame`

**Description:**
Chain all cleaning steps into a single lazy pipeline and return the cleaned
Dask DataFrame.

**Execution order:**
1. `standardise_column_names`
2. `coerce_dtypes`
3. `parse_month_year` (using `config.drop_yearly_summary_rows`)
4. `handle_nulls`
5. `validate_physical_bounds`
6. `remove_duplicates`

**Behaviour:**
- Each step receives the output of the previous step.
- Log the wall-clock time of the cleaning plan construction (not `.compute()`
  time — this function must remain lazy throughout).
- Return the final Dask DataFrame without calling `.compute()`.

**Error handling:**
- Propagate `CleaningError` raised by any step.
- Do not swallow unexpected exceptions.

**Dependencies:** dask, logging, time

**Test cases:**
- `@pytest.mark.unit` Provide a minimal valid raw Dask DataFrame; assert `clean`
  returns a `dask.dataframe.DataFrame` without raising.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame` (not pandas).
- `@pytest.mark.unit` Assert all expected output columns are present in the result
  schema (check `.columns` without computing).
- `@pytest.mark.integration` With actual ingested data from `data/raw`, call
  `clean(ingest(...))` and trigger `.compute()` on a sample partition; assert no
  negative `production` values, no invalid `product` codes, and `prod_date` is
  never NaT for monthly rows.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Design decisions and constraints

- The `clean` function must remain lazy end-to-end — all steps use
  `map_partitions` or Dask's native lazy operations. No `.compute()` inside
  this module.
- Zero production (`production == 0.0`) is a valid domain measurement indicating
  the lease reported zero output that month. It must never be replaced with NaN
  or dropped.
- Yearly summary rows (month = 0) and starting cumulative rows (month = -1) are
  not monthly production records. They are dropped by default in `parse_month_year`
  because all downstream analytics assume monthly granularity.
- Outlier flagging (via `is_outlier` column) is preferred over silent dropping
  so that downstream analysts can choose to include or exclude them.
- The deduplication step must be robust to the case where the same lease/month
  appears in both `oil2020.csv` and `oil2021.csv` (overlap at year boundaries
  is known to occur in KGS data).
- `CleaningError` must be importable from `kgs.src.cleaning` by the pipeline
  orchestrator.
