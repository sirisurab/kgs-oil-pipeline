# Task Specification: Data Processing Component

**Module:** `kgs/src/processing.py`
**Component purpose:** Accept the cleaned Dask DataFrame from the cleaning stage
and produce a fully processed, ML/analytics-ready Parquet dataset. This stage
performs feature engineering (cumulative production, monthly trends, production
ratios), normalisations, categorical encodings, well-level and operator/county
aggregations, and writes the output to partitioned Parquet files under
`data/processed/`.

---

## Overview

Domain context (oil-and-gas-domain-expert):
- KGS records contain one row per lease per month per product type (O or G).
  Before feature engineering, separate oil rows from gas rows and rejoin on
  `(lease_kid, prod_date)` to create a wide per-lease-per-month record.
- Cumulative production (Np for oil, Gp for gas) must be computed per lease
  and must be monotonically non-decreasing over time. Any decrease after
  sorting indicates a pipeline bug.
- Monthly production rate trends (month-over-month change) are a standard
  decline-curve feature used in production analytics.
- GOR (Gas-Oil Ratio) = gas_production_mcf / oil_production_bbl for rows
  where both are present. GOR must be ≥ 0.
- Water cut is not directly available in KGS archive data; do not fabricate it.
- Operator and county aggregations are standard analytics views used by
  reservoir engineers.
- All numeric feature columns output to Parquet must use `float64` dtype for
  ML compatibility.

---

## Task 01: Define processing configuration dataclass

**Module:** `kgs/src/processing.py`
**Function/Class:** `ProcessingConfig` (dataclass)

**Description:**
Define a frozen dataclass `ProcessingConfig` that carries all runtime parameters
for the processing stage.

**ProcessingConfig fields:**
- `processed_dir: str` — output directory for Parquet files, default `"data/processed"`
- `interim_dir: str` — directory for intermediate Parquet checkpoints,
  default `"data/interim"`
- `partition_column: str` — column to use for Parquet partitioning,
  default `"lease_kid"`
- `scale_numeric: bool` — if True, apply min-max normalisation to numeric
  feature columns, default `True`
- `encode_categoricals: bool` — if True, encode categorical columns as integer
  codes, default `True`
- `n_workers: int` — Dask LocalCluster worker count, default `4`
- `compute_aggregations: bool` — if True, compute and write operator/county
  aggregation tables, default `True`

**Error handling:**
- Raise `ValueError` if `processed_dir` or `interim_dir` paths are the same.

**Dependencies:** dataclasses

**Test cases:**
- `@pytest.mark.unit` Assert default field values are correct.
- `@pytest.mark.unit` Assert `ValueError` is raised when `processed_dir` equals
  `interim_dir`.

**Definition of done:** Dataclass is implemented, all test cases pass, ruff and
mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 02: Implement oil/gas row splitter and wide-format pivot

**Module:** `kgs/src/processing.py`
**Function:** `pivot_oil_gas(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
The cleaned DataFrame has one row per (lease_kid, prod_date, product). This
function reshapes it into one row per (lease_kid, prod_date) with separate
columns for oil and gas production.

**Behaviour:**
- Filter rows where `product == "O"` into `df_oil`; rename `production` to
  `oil_bbl` and `wells` to `oil_wells`.
- Filter rows where `product == "G"` into `df_gas`; rename `production` to
  `gas_mcf` and `wells` to `gas_wells`.
- Outer-merge `df_oil` and `df_gas` on `["lease_kid", "prod_date",
  "prod_month", "prod_year", "source_year", "lease_name", "dor_code",
  "api_number", "field", "producing_zone", "operator", "county",
  "township", "twn_dir", "range_num", "range_dir", "section", "spot",
  "latitude", "longitude"]`.
- Where either oil or gas data is missing for a lease-month, the missing
  production column must be `NaN` (not zero — missingness must be preserved).
- Drop the `product` and `month_year` columns from the output (they are
  superseded by `prod_date` and the split columns).
- Drop `is_outlier` propagation columns after carrying them forward
  as `oil_is_outlier` / `gas_is_outlier` if they existed.
- Must not call `.compute()`.

**Error handling:**
- Raise `ProcessingError` (custom exception) if neither `product == "O"` nor
  `product == "G"` rows are present after filtering.
- Log a WARNING if the wide-format DataFrame has significantly fewer rows than
  expected (indicates non-matching join keys).

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide a DataFrame with 4 rows: 2 oil and 2 gas for the
  same lease on different months; assert the pivoted DataFrame has 2 rows with
  both `oil_bbl` and `gas_mcf` columns populated.
- `@pytest.mark.unit` Provide a lease-month that has oil but no gas; assert
  `gas_mcf` is `NaN` for that row (not zero).
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert `product` column is absent from the output.

**Definition of done:** Function and `ProcessingError` are implemented, all test
cases pass, ruff and mypy report no errors, requirements.txt updated with all
third-party packages imported in this task.

---

## Task 03: Implement cumulative production calculator

**Module:** `kgs/src/processing.py`
**Function:** `compute_cumulative_production(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute per-lease cumulative oil and gas production (Np and Gp) sorted by date.

**Behaviour:**
- Sort the DataFrame by `["lease_kid", "prod_date"]` ascending.
- Within each `lease_kid` group, compute:
  - `cum_oil_bbl`: cumulative sum of `oil_bbl`, treating NaN as 0 for
    cumulative purposes (i.e. use `fillna(0)` before cumsum, then restore
    NaN in the original `oil_bbl` column).
  - `cum_gas_mcf`: cumulative sum of `gas_mcf`, same NaN treatment.
- The cumulative columns must be monotonically non-decreasing within each
  lease group over time.
- Use `dask.dataframe.map_partitions` with groupby cumsum applied per partition,
  then a cross-partition cumulative correction using a scan-style approach
  (compute partition-end cumulative totals, broadcast offsets back, add to
  each partition's local cumsum). This avoids a full sort and materialise.
- Must not call `.compute()` in the function body.

**Error handling:**
- Log a WARNING if, after cumulative computation, any partition has a
  `cum_oil_bbl` or `cum_gas_mcf` value less than the previous row within
  the same partition (indicates sort order issue). This check should be
  performed lazily via `map_partitions`.

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide a single-lease DataFrame with 3 months of oil
  production `[100, 200, 150]`; assert `cum_oil_bbl = [100, 300, 450]`.
- `@pytest.mark.unit` Provide a month with `oil_bbl = NaN`; assert
  `cum_oil_bbl` does not decrease (NaN treated as 0 for cumulative).
- `@pytest.mark.unit` Provide two leases interleaved; assert cumulative values
  restart correctly per lease.
- `@pytest.mark.unit` Assert `cum_oil_bbl` values are monotonically non-decreasing
  within each lease group after `.compute()` in the test.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 04: Implement monthly trend and derived feature engineer

**Module:** `kgs/src/processing.py`
**Function:** `engineer_features(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add derived feature columns used in decline-curve analysis and ML workflows.

**Features to compute:**
- `oil_mom_change`: month-over-month change in `oil_bbl` per lease
  (`oil_bbl - oil_bbl.shift(1)` within lease group, sorted by `prod_date`).
- `gas_mom_change`: same for `gas_mcf`.
- `oil_pct_change`: percentage change in oil production month-over-month
  per lease. Set to `NaN` for the first month of each lease (no prior month).
- `gor`: gas-oil ratio = `gas_mcf / oil_bbl`. Set to `NaN` where `oil_bbl`
  is 0 or NaN (avoid division by zero). GOR must never be negative.
- `active_months`: count of months with non-null, non-zero oil or gas production
  up to and including the current month, per lease (rolling count).
- `prod_date_ordinal`: the `prod_date` expressed as a numeric ordinal
  (days since epoch) for ML models that require numeric time features.

**Behaviour:**
- All computations are per-lease (grouped by `lease_kid`), applied via
  `dask.dataframe.map_partitions` where possible, or using Dask's native
  groupby transform.
- Must preserve rows where production is zero (these are valid records
  and should not be excluded from trend computations).
- Must not call `.compute()`.

**Error handling:**
- Division by zero in GOR calculation must produce `NaN`, not raise.
- Shift operations at the start of each lease group boundary produce `NaN`;
  this is expected and must not be replaced.

**Dependencies:** dask, pandas, numpy, logging

**Test cases:**
- `@pytest.mark.unit` Provide 3 months of oil: `[100, 150, 120]`; assert
  `oil_mom_change = [NaN, 50, -30]`.
- `@pytest.mark.unit` Provide `oil_bbl = 0.0` and `gas_mcf = 100.0`; assert
  `gor = NaN` (no division by zero error).
- `@pytest.mark.unit` Provide `oil_bbl = 200.0` and `gas_mcf = 400.0`; assert
  `gor = 2.0`.
- `@pytest.mark.unit` Assert `gor` is never negative.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert `prod_date_ordinal` column is of type `float64`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 05: Implement numeric normaliser and categorical encoder

**Module:** `kgs/src/processing.py`
**Function:** `normalise_and_encode(df: dask.dataframe.DataFrame, config: ProcessingConfig) -> dask.dataframe.DataFrame`

**Description:**
Apply min-max normalisation to continuous numeric feature columns and integer
label encoding to categorical columns, controlled by `ProcessingConfig` flags.

**Columns to normalise (if `config.scale_numeric=True`):**
`oil_bbl`, `gas_mcf`, `cum_oil_bbl`, `cum_gas_mcf`, `gor`, `latitude`,
`longitude`. Each gets a corresponding `_scaled` column (e.g. `oil_bbl_scaled`).
The original columns are retained.
Min/max values must be computed once using a Dask `describe()` call (which
triggers one `.compute()`) and then reused as constants in a `map_partitions`
operation — this is the only permitted `.compute()` in this function.

**Columns to encode (if `config.encode_categoricals=True`):**
`operator`, `county`, `field`, `producing_zone`. Each gets a corresponding
`_code` integer column (e.g. `operator_code`). Use Dask's `categorize()` to
build the category mapping and then `cat.codes` to assign integer codes.

**Behaviour:**
- Normalised columns use float64 dtype.
- Encoded columns use `Int32` (nullable integer) dtype.
- Original columns are always retained alongside new derived columns.
- Must not call `.compute()` except for the single min/max stats collection
  call described above.

**Error handling:**
- If a column to normalise has zero variance (min == max), set `_scaled`
  column to `0.0` for all rows; log a WARNING.
- Log an INFO message listing all columns normalised and encoded.

**Dependencies:** dask, pandas, numpy, logging

**Test cases:**
- `@pytest.mark.unit` Provide `oil_bbl` values of `[0, 50, 100]`; assert
  `oil_bbl_scaled = [0.0, 0.5, 1.0]`.
- `@pytest.mark.unit` Provide a constant column (all same value); assert
  `_scaled` column is all `0.0` and a WARNING was logged.
- `@pytest.mark.unit` Provide `operator` values `["ACME", "CORP", "ACME"]`;
  assert `operator_code` values are consistent integers and `"ACME"` maps to
  the same code in both rows.
- `@pytest.mark.unit` Assert original `oil_bbl` column is still present in the
  output.
- `@pytest.mark.unit` Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 06: Implement operator and county aggregation builder

**Module:** `kgs/src/processing.py`
**Function:** `build_aggregations(df: dask.dataframe.DataFrame, config: ProcessingConfig) -> dict[str, dask.dataframe.DataFrame]`

**Description:**
Produce summary aggregation DataFrames grouped by operator, county, and field.
These are standard analytics views used by reservoir engineers to compare
performance across portfolios.

**Aggregations to compute:**

Grouping key `["operator", "prod_year", "prod_month"]`:
- `total_oil_bbl`: sum of `oil_bbl`
- `total_gas_mcf`: sum of `gas_mcf`
- `active_leases`: count of distinct `lease_kid` values
- `avg_oil_bbl_per_lease`: mean `oil_bbl` per lease

Grouping key `["county", "prod_year", "prod_month"]`:
- Same four metrics as above.

Grouping key `["field", "prod_year"]` (annual field summary):
- `annual_oil_bbl`: sum of `oil_bbl`
- `annual_gas_mcf`: sum of `gas_mcf`
- `peak_oil_month`: month with highest `total_oil_bbl` for that field-year
  (requires a two-level aggregation).

**Behaviour:**
- Return a dict with keys `"by_operator"`, `"by_county"`, `"by_field"`,
  each mapping to a lazy Dask DataFrame.
- If `config.compute_aggregations=False`, return an empty dict without
  performing any computation.
- Must not call `.compute()`.

**Error handling:**
- If any required grouping column is missing, log a WARNING and skip
  that aggregation (do not raise).

**Dependencies:** dask, pandas, logging

**Test cases:**
- `@pytest.mark.unit` Provide a minimal DataFrame with 4 rows across 2 operators;
  assert `build_aggregations` returns a dict with keys `"by_operator"`,
  `"by_county"`, `"by_field"`.
- `@pytest.mark.unit` Assert all values in the returned dict are
  `dask.dataframe.DataFrame` instances (not pandas).
- `@pytest.mark.unit` With `compute_aggregations=False`, assert an empty dict
  is returned.
- `@pytest.mark.unit` Assert the `"by_operator"` DataFrame has column
  `total_oil_bbl` when `.compute()` is called in the test.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 07: Implement Parquet writer with per-lease partitioning

**Module:** `kgs/src/processing.py`
**Function:** `write_parquet(df: dask.dataframe.DataFrame, config: ProcessingConfig, name: str = "production") -> list[Path]`

**Description:**
Write the processed Dask DataFrame to partitioned Parquet files.

**Behaviour:**
- Sort the DataFrame by `["lease_kid", "prod_date"]` before writing.
- Write to `config.processed_dir / name /` using
  `dask.dataframe.to_parquet` with:
  - `write_index=False`
  - `partition_on=[config.partition_column]`
  - `schema="infer"` (consistent schema across all partitions)
  - `engine="pyarrow"`
- Return a list of `Path` objects for each written Parquet file discovered
  under the output directory after writing.
- If the output directory already exists and contains Parquet files, overwrite
  them (do not append duplicate data).
- Also write each aggregation DataFrame from `build_aggregations` to
  `config.processed_dir / "aggregations" /` as separate named Parquet files
  (`by_operator.parquet`, `by_county.parquet`, `by_field.parquet`).

**Error handling:**
- Create `config.processed_dir` if it does not exist.
- Raise `ProcessingError` if the written Parquet directory is empty after
  the write completes (silent write failure).
- Log the number of Parquet files written and total bytes.

**Dependencies:** dask, pyarrow, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Write a minimal Dask DataFrame to a temp directory;
  assert the returned list of paths is non-empty and all paths exist on disk.
- `@pytest.mark.unit` Read back the written Parquet files with
  `pandas.read_parquet`; assert they contain the expected rows.
- `@pytest.mark.unit` Assert that each Parquet partition file contains data
  for exactly one `lease_kid` value (no cross-lease data in a single partition).
- `@pytest.mark.unit` Assert that running `write_parquet` twice on the same
  data writes the same row count (idempotent, not cumulative).
- `@pytest.mark.integration` Write actual processed data to `data/processed/`;
  assert all Parquet files are readable by a fresh `dask.dataframe.read_parquet`
  call and schema is consistent across partitions.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 08: Implement top-level processing entry point

**Module:** `kgs/src/processing.py`
**Function:** `process(df: dask.dataframe.DataFrame, config: ProcessingConfig) -> list[Path]`

**Description:**
Chain all processing steps and write the final Parquet output.

**Execution order:**
1. `pivot_oil_gas`
2. `compute_cumulative_production`
3. `engineer_features`
4. `normalise_and_encode`
5. `build_aggregations` (result passed to `write_parquet`)
6. `write_parquet` (writes main processed data and aggregations)

**Behaviour:**
- After step 4, write an interim checkpoint to `config.interim_dir` using
  `dask.dataframe.to_parquet` so the feature-engineered data is preserved
  before the write step. This allows reruns from the interim checkpoint
  without re-running the full pipeline.
- Return the list of written Parquet file paths from `write_parquet`.
- Log total elapsed wall-clock time for the entire processing stage.

**Error handling:**
- Propagate `ProcessingError`.
- Log an ERROR if `write_parquet` returns an empty list.

**Dependencies:** dask, pathlib, logging, time

**Test cases:**
- `@pytest.mark.unit` With mocked sub-functions, assert `process` returns a list
  of Paths.
- `@pytest.mark.unit` Assert that the interim checkpoint directory is created
  and non-empty after `process` completes (triggered by `.compute()` in the test).
- `@pytest.mark.integration` Run `process` against cleaned data from `data/interim`;
  assert Parquet files in `data/processed/` are readable, schema is consistent
  across partitions, and `cum_oil_bbl` is monotonically non-decreasing per lease.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Design decisions and constraints

- The only permitted `.compute()` inside `processing.py` is in `normalise_and_encode`
  for computing min/max statistics (one call). All other operations must remain lazy.
- Per-lease partitioning in Parquet is the canonical output format. Downstream
  ML and analytics tools will read individual partitions by `lease_kid`.
- Cumulative production is computed from the cleaned monthly data, not from the
  KGS raw cumulative rows (which were dropped in the cleaning stage). This ensures
  internal consistency.
- GOR is computed as `gas_mcf / oil_bbl` and set to NaN when `oil_bbl` is 0
  or NaN. Negative GOR is physically impossible and any negative value indicates
  a pipeline bug.
- The `_scaled` and `_code` columns are additive — original columns are always
  preserved for traceability.
- `ProcessingError` must be importable from `kgs.src.processing` by the pipeline
  orchestrator.
- Parquet output must use `pyarrow` engine for schema enforcement and compatibility
  with downstream Spark/BigQuery workflows.
