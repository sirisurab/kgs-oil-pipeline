# Features Stage — Task Specifications

**Module:** `kgs_pipeline/features.py`
**Stage manifest:** `agent_docs/stage-manifest-features.md`
**Boundary contract:** `agent_docs/boundary-transform-features.md` (input)
**ADRs in force:** ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006, ADR-007, ADR-008

---

## Task FEA-01: Implement cumulative production calculator

**Module:** `kgs_pipeline/features.py`
**Function:** `add_cumulative_production(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Compute cumulative production columns for a single partition's worth of data,
grouped by `(LEASE_KID, PRODUCT)`. The function must:

1. Sort by `production_date` within each `(LEASE_KID, PRODUCT)` group — the boundary
   contract guarantees sort order at the partition level, but this sort ensures
   correctness within the function regardless of how data lands in a partition.
2. Compute `cum_production` as the cumulative sum of `PRODUCTION` within each
   `(LEASE_KID, PRODUCT)` group using a vectorized grouped transform (ADR-002 —
   no per-row iteration).
3. Return the DataFrame with the `cum_production` column added.

Cumulative production must be monotonically non-decreasing over time per
`(LEASE_KID, PRODUCT)` group (TR-03). Zero production months must hold the
cumulative value flat (TR-08) — the cumulative sum correctly handles this provided
`PRODUCTION = 0.0` is preserved (not null) in those rows.

**Error handling:**
- If `PRODUCTION` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** pandas

**Test cases (unit — TR-03, TR-08):**
- Given a synthetic group with production values `[100, 150, 0, 200]` across four
  consecutive months, assert `cum_production` is `[100, 250, 250, 450]`
  (zero month holds the cumulative flat — TR-08a).
- Given a group with zero production in the first month (`[0, 100, 200]`), assert
  `cum_production` is `[0, 100, 300]` (TR-08b — shut-in at start).
- Given a group with zero months mid-sequence followed by resumed production, assert
  cumulative resumes correctly from the flat value (TR-08c).
- Assert that `cum_production` is monotonically non-decreasing for every group (TR-03).
- Given a synthetic sequence of known values, assert cumulative values match
  hand-computed results (TR-09a).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-02: Implement GOR and water cut calculator

**Module:** `kgs_pipeline/features.py`
**Function:** `add_ratios(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Compute Gas-Oil Ratio (GOR) and water cut for each row. Because KGS data is
structured as one row per `(LEASE_KID, PRODUCT, production_date)` — not one row per
lease per date with both oil and gas — this function must first pivot the data to
produce a wide-format representation with separate `oil_bbl` and `gas_mcf` columns
before computing ratios. The function must:

1. Pivot the `(LEASE_KID, production_date)` group to wide format: one column for
   oil production (`oil_bbl`, where `PRODUCT = "O"`) and one for gas production
   (`gas_mcf`, where `PRODUCT = "G"`). Leases with only one product type produce
   null in the missing column.
2. If a `WATER` product column exists (production type `W`), pivot it to `water_bbl`;
   otherwise add `water_bbl` as an all-zero column.
3. Compute GOR = `gas_mcf / oil_bbl`:
   - `oil_bbl > 0 and gas_mcf >= 0` → `gas_mcf / oil_bbl` (normal case)
   - `oil_bbl == 0 and gas_mcf > 0` → `NaN` (mathematically undefined — TR-06a)
   - `oil_bbl == 0 and gas_mcf == 0` → `0.0` or `NaN` (shut-in — TR-06b, define
     as `NaN`)
   - `oil_bbl > 0 and gas_mcf == 0` → `0.0` (TR-06c)
   Do not use division that raises `ZeroDivisionError` — use vectorized operations
   with explicit masking.
4. Compute water cut = `water_bbl / (oil_bbl + water_bbl)`:
   - Both zero → `NaN` (shut-in, total liquid = 0)
   - `water_bbl = 0, oil_bbl > 0` → `0.0` (TR-10a)
   - `oil_bbl = 0, water_bbl > 0` → `1.0` (TR-10b — valid end-of-life state)
   - Values outside `[0, 1]` are a computation error and must raise `ValueError`.
5. Add `gor` and `water_cut` columns to the DataFrame.
6. Return the wide-format DataFrame.

**Error handling:**
- If `gor` or `water_cut` values outside expected ranges are produced, raise
  `ValueError` rather than silently propagating.

**Dependencies:** pandas

**Test cases (unit — TR-01, TR-06, TR-09, TR-10):**
- Given `oil_bbl=0, gas_mcf=100`, assert `gor` is `NaN`, not an exception (TR-06a).
- Given `oil_bbl=0, gas_mcf=0`, assert `gor` is `NaN`, not an exception (TR-06b).
- Given `oil_bbl=200, gas_mcf=0`, assert `gor` is `0.0` (TR-06c).
- Given `oil_bbl=200, gas_mcf=400`, assert `gor` is `2.0` (TR-09d — correct
  formula: gas/oil, not oil/gas).
- Given `water_bbl=0, oil_bbl=150`, assert `water_cut` is `0.0` and the row is
  retained (TR-10a).
- Given `oil_bbl=0, water_bbl=300`, assert `water_cut` is `1.0` and the row is
  retained (TR-10b).
- Given `water_bbl=100, oil_bbl=300`, assert `water_cut` is `0.25` — formula uses
  total liquid as denominator, not just oil (TR-09e).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-03: Implement decline rate calculator

**Module:** `kgs_pipeline/features.py`
**Function:** `add_decline_rate(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Compute the period-over-period production decline rate per `(LEASE_KID, PRODUCT)`
group and add it as the `decline_rate` column. The function must:

1. Within each `(LEASE_KID, PRODUCT)` group, compute the month-over-month change
   in `PRODUCTION` using a vectorized shift operation.
2. Define decline rate as `(production_t - production_{t-1}) / production_{t-1}`
   for `production_{t-1} > 0`.
3. For rows where `production_{t-1} == 0` (shut-in prior month), the raw
   computation would produce an undefined or extreme value. Handle this case
   explicitly before the clip is applied — define the value as `NaN` when
   `production_{t-1} == 0` (TR-07d).
4. Clip the resulting values to `[-1.0, 10.0]` (TR-07):
   - Values below `-1.0` clip to exactly `-1.0` (TR-07a).
   - Values above `10.0` clip to exactly `10.0` (TR-07b).
   - Values within `[-1.0, 10.0]` pass through unchanged (TR-07c).
5. Return the DataFrame with `decline_rate` added.

**Error handling:**
- If `PRODUCTION` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** pandas

**Test cases (unit — TR-07):**
- Given a known month-over-month sequence `[100, 80, 60]`, assert `decline_rate`
  values match hand-computed results for months 2 and 3.
- Given a computed decline rate of `-1.5`, assert it is clipped to `-1.0` (TR-07a).
- Given a computed decline rate of `15.0`, assert it is clipped to `10.0` (TR-07b).
- Given a computed decline rate of `0.5`, assert it passes through unchanged (TR-07c).
- Given a prior month production of `0` (shut-in), assert `decline_rate` is `NaN`
  before clipping, not an unclipped extreme (TR-07d).
- Given a well with two consecutive zero-production months, assert no unclipped
  extreme value is produced.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-04: Implement rolling statistics calculator

**Module:** `kgs_pipeline/features.py`
**Function:** `add_rolling_features(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Compute 3-month and 6-month rolling averages of `PRODUCTION` per
`(LEASE_KID, PRODUCT)` group. The function must:

1. Within each `(LEASE_KID, PRODUCT)` group, compute:
   - `rolling_3m_production`: rolling mean of `PRODUCTION` with window=3, min_periods=1.
   - `rolling_6m_production`: rolling mean of `PRODUCTION` with window=6, min_periods=1.
2. Add both columns to the DataFrame.
3. Return the modified DataFrame.

When the rolling window is larger than available history (e.g. first month of a
well's life), the result uses all available rows (partial window via `min_periods=1`)
— not zero, not NaN (TR-09b clarifies: the result for windows larger than history
must be specified as partial window or NaN per spec; use partial window with
`min_periods=1` as the defined behavior).

All computations must use vectorized grouped transforms — no per-row or per-group
iteration (ADR-002).

**Dependencies:** pandas

**Test cases (unit — TR-09):**
- Given a known sequence of production values `[100, 200, 300, 400, 500, 600]` for
  one lease, assert `rolling_3m_production` values match hand-computed 3-month
  averages for each position (TR-09a).
- Given a known sequence of 6 values, assert `rolling_6m_production` for position 6
  equals the mean of all 6 values.
- Given only 2 months of data, assert `rolling_6m_production` for month 2 equals
  the mean of the 2 available values (partial window — TR-09b).
- Assert `rolling_3m_production` and `rolling_6m_production` are present in the
  returned DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-05: Implement lag feature calculator

**Module:** `kgs_pipeline/features.py`
**Function:** `add_lag_features(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Compute lag features for production per `(LEASE_KID, PRODUCT)` group. The function
must:

1. Within each `(LEASE_KID, PRODUCT)` group (sorted by `production_date`), compute:
   - `lag_1m_production`: `PRODUCTION` shifted by 1 period (production of the
     previous month).
   - `lag_3m_production`: `PRODUCTION` shifted by 3 periods.
   - `lag_6m_production`: `PRODUCTION` shifted by 6 periods.
2. The first N rows of each group (where N equals the lag) will be `NaN` — this is
   correct and expected.
3. Return the DataFrame with all three lag columns added.

The lag-1 feature for month N must equal the raw `PRODUCTION` value for month N-1
exactly (TR-09c).

**Dependencies:** pandas

**Test cases (unit — TR-09):**
- Given a sequence of production months `[100, 150, 200]`, assert `lag_1m_production`
  for month 3 is `150` (TR-09c).
- Assert `lag_1m_production` for the first month of a lease is `NaN`.
- Assert `lag_3m_production` for the first 3 months of a lease is `NaN`, and equals
  the first month's production starting from month 4.
- Assert all three lag columns are present in the returned DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-06: Implement categorical encoder

**Module:** `kgs_pipeline/features.py`
**Function:** `encode_categoricals(df: pandas.DataFrame) -> pandas.DataFrame`

**Description:**
Encode categorical columns as integer codes for ML readiness. The function must:

1. For `PRODUCT` (categories: `"O"`, `"G"`): encode as integer codes using the
   declared category order from the data dictionary — `"G"` → 0, `"O"` → 1 (or the
   natural alphabetical order of the category set). Store in `product_code`.
2. For `TWN_DIR` (categories: `"S"`, `"N"`): encode as integer codes. Store in
   `twn_dir_code`. Null values map to `-1`.
3. For `RANGE_DIR` (categories: `"E"`, `"W"`): encode as integer codes. Store in
   `range_dir_code`. Null values map to `-1`.
4. Preserve the original categorical columns unchanged — add the encoded columns
   alongside them.
5. Return the DataFrame with encoded columns added.

Encoding must use the `.cat.codes` accessor on already-cast categorical columns —
do not re-derive categories from data (ADR-003).

**Dependencies:** pandas

**Test cases (unit):**
- Given a row with `PRODUCT = "O"`, assert `product_code` is the correct integer
  code for `"O"` per the declared category set.
- Given a row with `TWN_DIR = None`, assert `twn_dir_code` is `-1`.
- Assert the original `PRODUCT`, `TWN_DIR`, and `RANGE_DIR` columns are still
  present in the returned DataFrame.
- Assert `product_code`, `twn_dir_code`, and `range_dir_code` are present in the
  returned DataFrame with integer dtype.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-07: Implement features map_partitions orchestrator

**Module:** `kgs_pipeline/features.py`
**Function:** `apply_feature_transforms(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Chain all partition-level feature engineering functions into a sequence of
`map_partitions` calls, maintaining a lazy Dask task graph. The function must:

1. Apply `add_cumulative_production` via `map_partitions`.
2. Apply `add_ratios` via `map_partitions`.
3. Apply `add_decline_rate` via `map_partitions`.
4. Apply `add_rolling_features` via `map_partitions`.
5. Apply `add_lag_features` via `map_partitions`.
6. Apply `encode_categoricals` via `map_partitions`.
7. Return the resulting Dask DataFrame without calling `.compute()` (ADR-005, TR-17).

For each `map_partitions` call, the `meta` argument must be derived by calling the
actual partition function on `ddf._meta.copy()` — not constructed separately
(ADR-003, TR-23).

The temporal sort established by the transform stage must be preserved at the point
each time-dependent feature is computed (stage-manifest-features.md H1). Do not
re-sort inside this function — rely on the boundary contract from
boundary-transform-features.md.

**Dependencies:** dask, pandas

**Test cases (unit — TR-17, TR-23):**
- Assert the return type is `dask.dataframe.DataFrame` (TR-17).
- For each `map_partitions` call, assert that calling the wrapped function on
  `ddf._meta.copy()` produces output whose column names and dtypes match the `meta`
  passed to `map_partitions` (TR-23).
- Given a small synthetic Dask DataFrame with known values, assert that the column
  list of the output includes all expected derived columns: `cum_production`, `gor`,
  `water_cut`, `decline_rate`, `rolling_3m_production`, `rolling_6m_production`,
  `lag_1m_production`, `lag_3m_production`, `lag_6m_production`, `product_code`,
  `twn_dir_code`, `range_dir_code` (TR-19).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-08: Implement output schema validator

**Module:** `kgs_pipeline/features.py`
**Function:** `validate_features_schema(ddf: dask.dataframe.DataFrame) -> None`

**Description:**
Validate that the features Dask DataFrame contains all required output columns
before writing. The required columns are all columns from the transform output plus
the derived feature columns. The complete required set is:

`LEASE_KID, LEASE, DOR_CODE, API_NUMBER, FIELD, PRODUCING_ZONE, OPERATOR, COUNTY,
TOWNSHIP, TWN_DIR, RANGE, RANGE_DIR, SECTION, SPOT, LATITUDE, LONGITUDE, PRODUCT,
WELLS, PRODUCTION, production_date, source_file, cum_production, gor, water_cut,
decline_rate, rolling_3m_production, rolling_6m_production, lag_1m_production,
lag_3m_production, lag_6m_production, product_code, twn_dir_code, range_dir_code`

If any column in this set is missing from `ddf.columns`, raise `ValueError` naming
the missing column(s). This implements TR-19.

**Dependencies:** dask

**Test cases (unit — TR-19):**
- Given a synthetic Dask DataFrame containing all required columns, assert the
  function returns without raising.
- Given a DataFrame missing `gor`, assert `ValueError` is raised naming `gor`.
- Given a DataFrame missing `cum_production` and `water_cut`, assert `ValueError`
  is raised naming both missing columns.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-09: Implement schema stability checker

**Module:** `kgs_pipeline/features.py`
**Function:** `check_schema_stability(processed_dir: Path) -> None`

**Description:**
Read the schema (column names and dtypes) from two sampled Parquet partition files
in `processed_dir` and assert they are identical. This implements TR-14 for the
features stage output. The function must:

1. List all `.parquet` files in `processed_dir`.
2. Sample 2 files (first and last, or random if more than 2 are available).
3. Read only the schema (not the full data) from each file using `pyarrow.parquet`.
4. Assert that column names and dtypes are identical across both files.
5. If schemas differ, raise `ValueError` with details of the mismatch.

**Dependencies:** pyarrow, pathlib

**Test cases (unit — TR-14):**
- Given two synthetic Parquet files with identical schemas, assert the function
  returns without raising.
- Given two Parquet files where one has an extra column, assert `ValueError` is
  raised describing the mismatch.
- Given two Parquet files where a shared column has different dtypes, assert
  `ValueError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-10: Implement features Parquet writer

**Module:** `kgs_pipeline/features.py`
**Function:** `write_features(ddf: dask.dataframe.DataFrame, config: dict) -> None`

**Description:**
Write the features Dask DataFrame to ML-ready partitioned Parquet files. The
function must:

1. Resolve the output directory from `config["features"]["processed_dir"]`; create
   it if absent.
2. Validate the schema using `validate_features_schema(ddf)` before writing.
3. Apply repartition to `max(10, min(n, 50))` where `n` is `ddf.npartitions`.
   Repartition must be the last operation before writing (ADR-004).
4. Write using `ddf.to_parquet(output_dir, write_index=True, overwrite=True)`.
5. Call `check_schema_stability(output_dir)` after writing to detect schema drift
   across partitions.
6. Log the output path and partition count at INFO level.

**Dependencies:** dask, pathlib, logging

**Test cases (integration — TR-18):**
- Given a small synthetic Dask DataFrame with all required feature columns, assert
  written Parquet files are readable by `pandas.read_parquet` (TR-18).
- Assert that the schema of a read-back partition includes all feature columns
  from `validate_features_schema`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-11: Implement feature correctness validator

**Module:** `kgs_pipeline/features.py`
**Function:** `validate_feature_correctness(df: pandas.DataFrame) -> list[str]`

**Description:**
Run domain correctness checks on a partition-level DataFrame and return a list of
error message strings (empty list if all checks pass). The function must verify:

1. `PRODUCTION >= 0` for all rows — production volumes cannot be negative (TR-01).
2. `gor >= 0` for all rows where `gor` is not `NaN` — GOR must be non-negative
   (TR-01).
3. `water_cut` is in `[0.0, 1.0]` for all rows where `water_cut` is not `NaN`
   (TR-01, TR-10).
4. `cum_production` is monotonically non-decreasing within each
   `(LEASE_KID, PRODUCT)` group (TR-03).
5. `decline_rate` is in `[-1.0, 10.0]` for all non-NaN rows — values outside this
   range indicate the clip was not applied (TR-07).

Each failed check contributes one descriptive entry to the returned list.

**Dependencies:** pandas

**Test cases (unit — TR-01, TR-03, TR-07, TR-10):**
- Given a valid synthetic DataFrame, assert the function returns an empty list.
- Given a row with `PRODUCTION = -1`, assert the returned list contains one error
  entry.
- Given a decreasing `cum_production` sequence (e.g. `[100, 90]` for the same
  lease), assert the returned list contains one error entry (TR-03).
- Given a `decline_rate` value of `15.0` (above clip bound), assert the returned
  list contains one error entry (TR-07).
- Given a `water_cut` of `1.5`, assert the returned list contains one error entry.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task FEA-12: Wire features stage entry point

**Module:** `kgs_pipeline/features.py`
**Function:** `features(config: dict, client) -> None`

**Description:**
Top-level entry point for the features stage, called by the pipeline orchestrator.
The function must:

1. Load the transformed Parquet using `dask.dataframe.read_parquet` from
   `config["features"]["transform_dir"]`.
2. Call `apply_feature_transforms(ddf)` to chain all feature engineering steps.
3. Call `write_features(ddf, config)` to write ML-ready Parquet output.
4. Run `validate_feature_correctness` on a sample partition (via `map_partitions`
   returning error lists, then batch-computed) and log any errors at WARNING level.
5. Log a stage-complete summary at INFO level.

The task graph must remain lazy until `write_features` triggers computation
(ADR-005). No intermediate `.compute()` calls are permitted between steps 1–3.

**Dependencies:** dask, logging

**Test cases (unit — all mocked):**
- Given mocked reads and writes, assert `features` completes without error and
  logs a stage-complete message.
- Assert that no `.compute()` call occurs before `write_features` is invoked (TR-17).
- Given mocked `validate_feature_correctness` returning one error string, assert it
  is logged at WARNING level.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.
