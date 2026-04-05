# Features Component Tasks

**Module:** `kgs_pipeline/features.py`
**Test file:** `tests/test_features.py`

## Overview

The features component reads processed Parquet files from `data/processed/`, computes
derived ML-ready features for each lease (cumulative production, decline rates, rolling
averages, lag features, GOR, water cut, months since first production), and writes
feature-ready Parquet files to `data/features/`. It also writes a metadata manifest
JSON file describing the output schema, record counts, feature column list, and
processing timestamps.

The processed schema (output of transform) includes:
`lease_kid`, `api_number`, `lease_name`, `operator`, `county`, `field`,
`producing_zone`, `production_date`, `oil_bbl`, `gas_mcf`, `well_count`, `latitude`,
`longitude`, `is_outlier`, `has_negative_production`, `source_file`.

Note: the KGS dataset does not contain a separate water production column. The `water_bbl`
column is introduced in the features stage with a default value of `0.0` to satisfy the
output schema contract required by downstream ML workflows (TR-19). If water production
data becomes available in future KGS dataset versions, this column will carry real values.

## Design Decisions

- All feature functions accept and return `dask.dataframe.DataFrame` — never call
  `.compute()` internally (TR-17). Apply feature computations inside `map_partitions`
  operating on per-lease groups where sequential ordering is required.
- String column meta uses `pd.StringDtype()` — never `"object"`.
- After reading processed Parquet, immediately repartition to `min(npartitions, 50)`.
- Output: write to `data/features/` as Parquet, targeting 20-50 files. Never partition
  on `lease_kid` directly (high cardinality).
- All derived numeric features use `float64`.
- Decline rate is clipped to `[-1.0, 10.0]` (TR-07).
- GOR is `gas_mcf / oil_bbl`; when `oil_bbl == 0`, result is `NaN` (not exception,
  not infinity) (TR-06).
- Water cut is `water_bbl / (oil_bbl + water_bbl)`; when both are zero, result is
  `NaN` (TR-09e, TR-10).
- Cumulative production values must be monotonically non-decreasing per lease (TR-03,
  TR-08).
- Rolling windows: 3-month and 6-month for oil, gas, and water. Partial window results
  for the first N-1 months are `NaN` (min_periods=1 is NOT used — use min_periods equal
  to the window size to produce NaN for insufficient history) (TR-09b).
- Lag features: lag-1 (previous month) for oil_bbl, gas_mcf, water_bbl (TR-09c).
- Months since first production: integer count of months from the lease's first
  non-zero production date to each row's production_date.
- Production per day: `oil_bbl / days_in_month`, `gas_mcf / days_in_month` where
  `days_in_month` is derived from `production_date`.

---

## Task 01: Read processed Parquet and repartition

**Module:** `kgs_pipeline/features.py`
**Function:** `read_processed(processed_dir: str) -> dd.DataFrame`

**Description:**
Read all Parquet files from `processed_dir` using `dd.read_parquet(processed_dir)`.
Immediately repartition to `min(ddf.npartitions, 50)`. Raise `FileNotFoundError` if
`processed_dir` does not exist or contains no Parquet files. Log the partition count.

**Dependencies:** `dask.dataframe`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a temporary directory with synthetic processed Parquet
  files, assert `read_processed` returns a `dask.dataframe.DataFrame` with the
  correct columns.
- `@pytest.mark.unit` — Assert `npartitions <= 50` after the call.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Given a non-existent path, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given `data/processed/` populated by transform, assert
  a non-empty Dask DataFrame is returned.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Add water_bbl column and compute GOR and water cut

**Module:** `kgs_pipeline/features.py`
**Functions:**
- `add_water_column(ddf: dd.DataFrame) -> dd.DataFrame`
- `compute_gor(ddf: dd.DataFrame) -> dd.DataFrame`
- `compute_water_cut(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**

`add_water_column`:
- Add `water_bbl` column with default value `0.0` (float64) to every row.
- This column represents water production; defaulted to zero because the KGS dataset
  does not report water production. Future data versions may populate this column.

`compute_gor`:
- Compute `gor = gas_mcf / oil_bbl` element-wise inside `map_partitions`.
- When `oil_bbl == 0` and `gas_mcf > 0`: result is `NaN` (TR-06a).
- When `oil_bbl == 0` and `gas_mcf == 0`: result is `NaN` (TR-06b).
- When `oil_bbl > 0` and `gas_mcf == 0`: result is `0.0` (TR-06c).
- Use `numpy.where` or pandas division with `replace(inf, NaN)` to avoid
  ZeroDivisionError or infinite values.
- Add `gor` column with dtype `float64` to the DataFrame.

`compute_water_cut`:
- Compute `water_cut = water_bbl / (oil_bbl + water_bbl)` element-wise inside
  `map_partitions`.
- When both `oil_bbl == 0` and `water_bbl == 0`: result is `NaN` (TR-10).
- When `water_bbl == 0` and `oil_bbl > 0`: result is `0.0` — valid, no water (TR-10a).
- When `oil_bbl == 0` and `water_bbl > 0`: result is `1.0` — valid late-life well (TR-10b).
- Values outside `[0.0, 1.0]` by construction are impossible if inputs are non-negative.
- Add `water_cut` column with dtype `float64`.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — `add_water_column`: assert every row has `water_bbl=0.0` and
  dtype `float64`.
- `@pytest.mark.unit` TR-06a — `compute_gor`: `oil_bbl=0`, `gas_mcf=500` → `gor=NaN`.
- `@pytest.mark.unit` TR-06b — `compute_gor`: `oil_bbl=0`, `gas_mcf=0` → `gor=NaN`.
- `@pytest.mark.unit` TR-06c — `compute_gor`: `oil_bbl=100`, `gas_mcf=0` → `gor=0.0`.
- `@pytest.mark.unit` — `compute_gor`: `oil_bbl=100`, `gas_mcf=500` → `gor=5.0`.
- `@pytest.mark.unit` TR-10a — `compute_water_cut`: `water_bbl=0`, `oil_bbl=100` →
  `water_cut=0.0`.
- `@pytest.mark.unit` TR-10b — `compute_water_cut`: `oil_bbl=0`, `water_bbl=50` →
  `water_cut=1.0`.
- `@pytest.mark.unit` TR-10 — `compute_water_cut`: `oil_bbl=0`, `water_bbl=0` →
  `water_cut=NaN`.
- `@pytest.mark.unit` — `compute_water_cut`: `oil_bbl=75`, `water_bbl=25` →
  `water_cut=0.25`.
- `@pytest.mark.unit` — Assert all three functions return `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** All three functions implemented, all tests pass, `ruff` and
`mypy` report no errors. `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 03: Cumulative production per lease

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_cumulative(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Compute cumulative oil, gas, and water production per lease (`lease_kid`), sorted by
`production_date` ascending. Add columns: `cum_oil` (`float64`), `cum_gas` (`float64`),
`cum_water` (`float64`).

Implementation approach using `map_partitions` is insufficient for cumulative production
across partition boundaries. Use Dask's `groupby` + `transform` with a custom function,
or materialise per-lease groups using `ddf.groupby("lease_kid").apply(...)` with
`meta` specified. Document the chosen approach in the function docstring.

Recommended approach:
1. Call `ddf.compute()` inside the function only if absolutely required for the
   cumulative computation. If so, document that this is an intentional exception to the
   lazy evaluation rule (cumulative across partitions requires full materialisation or a
   sort-based approach).
   **Preferred**: use `ddf.assign(cum_oil=ddf.groupby("lease_kid")["oil_bbl"].cumsum(),
   cum_gas=ddf.groupby("lease_kid")["gas_mcf"].cumsum(), ...)` which Dask supports lazily.
   Verify the result remains a `dask.dataframe.DataFrame` before returning.
2. Sort by `["lease_kid", "production_date"]` before computing cumulative (Dask cumsum
   within groupby requires sorted data for correctness).

**Domain correctness (TR-03, TR-08):**
- `cum_oil`, `cum_gas`, `cum_water` must be monotonically non-decreasing per lease.
- When a lease has zero production in a month (shut-in), the cumulative value must
  remain flat (equal to the prior month) — not decrease (TR-08a).
- Zero-production months at the start of the record must have cumulative = 0 (TR-08b).
- Cumulative must resume correctly from the flat value after shut-in (TR-08c).

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` TR-03 — Given a synthetic single-lease sequence with production
  values `[100, 200, 150, 0, 300]` over 5 months, assert `cum_oil` equals
  `[100, 300, 450, 450, 750]` (monotonically non-decreasing, flat during shut-in month).
- `@pytest.mark.unit` TR-08a — Given a zero-production month mid-sequence, assert
  `cum_oil` does not decrease and equals the prior month's cumulative.
- `@pytest.mark.unit` TR-08b — Given a lease where the first 2 months have
  `oil_bbl=0.0`, assert `cum_oil=0.0` for those months.
- `@pytest.mark.unit` TR-08c — Given production resumes after 2 shut-in months (value
  say 400), assert `cum_oil` for that month equals the flat cumulative + 400.
- `@pytest.mark.unit` — Given two leases in the same DataFrame, assert cumulative is
  computed independently per lease (lease B's cumulative is not affected by lease A).
- `@pytest.mark.unit` — Assert columns `cum_oil`, `cum_gas`, `cum_water` are present
  in the output.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Decline rate computation

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_decline_rate(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Compute the period-over-period oil production decline rate per lease and add it as
column `decline_rate` (`float64`).

Formula: `decline_rate = (oil_bbl[t] - oil_bbl[t-1]) / oil_bbl[t-1]`

Edge cases (inside `map_partitions`):
- When `oil_bbl[t-1] == 0` and `oil_bbl[t] > 0`: raw result is undefined (division by
  zero); set to `NaN` before clipping (TR-07d).
- When `oil_bbl[t-1] == 0` and `oil_bbl[t] == 0`: result is `NaN` (both shut-in).
- After computing the raw decline rate, clip to `[-1.0, 10.0]` (TR-07).
- For the first row of each lease (no prior month), `decline_rate = NaN`.

Implementation: use `map_partitions` with a custom `_compute_decline_partition` function
that groups by `lease_kid` within the partition, sorts by `production_date`, and applies
`pct_change()` on `oil_bbl`. For cross-partition correctness, document that the data
must be sorted and that within-partition computation may produce NaN for the first row
of a lease within a partition (not the overall first row). This is an accepted limitation;
document it.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` TR-07a — Given a sequence with computed decline rate of `-1.5`
  (before clipping), assert output `decline_rate = -1.0` (clipped to lower bound).
- `@pytest.mark.unit` TR-07b — Given a computed decline rate of `15.0`, assert output
  `decline_rate = 10.0` (clipped to upper bound).
- `@pytest.mark.unit` TR-07c — Given a computed decline rate of `0.5` (within bounds),
  assert output `decline_rate = 0.5` (unchanged).
- `@pytest.mark.unit` TR-07d — Given `oil_bbl[t-1]=0` and `oil_bbl[t]=100`, assert
  `decline_rate = NaN` (handled before clipping, not an extreme unclipped value).
- `@pytest.mark.unit` — Given `oil_bbl[t-1]=0` and `oil_bbl[t]=0` (both shut-in),
  assert `decline_rate = NaN`.
- `@pytest.mark.unit` — Given a first row for a lease (no prior data), assert
  `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Rolling averages and lag features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_rolling_and_lag_features(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Compute rolling averages and lag-1 features per lease, sorted by `production_date`.

Rolling averages (window sizes 3 and 6 months, `min_periods` equals the window size
to produce NaN when history is insufficient):
- `oil_bbl_roll3`, `oil_bbl_roll6` — rolling mean of `oil_bbl`
- `gas_mcf_roll3`, `gas_mcf_roll6` — rolling mean of `gas_mcf`
- `water_bbl_roll3`, `water_bbl_roll6` — rolling mean of `water_bbl`

Lag-1 features (previous month's value, within each lease group):
- `oil_bbl_lag1` — `oil_bbl` shifted by 1 within each lease group
- `gas_mcf_lag1` — `gas_mcf` shifted by 1
- `water_bbl_lag1` — `water_bbl` shifted by 1

All new columns have dtype `float64`. NaN is the correct value for months where
insufficient history exists (first 2 months for 3-month rolling, first 5 months for
6-month rolling, first month for lag-1).

Implementation: inside `map_partitions`, group by `lease_kid`, sort by
`production_date`, and apply rolling and shift operations per group. Return the
reassembled DataFrame with new columns appended.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` TR-09a — Given a known 6-month oil sequence `[100, 200, 150,
  300, 250, 400]`, assert `oil_bbl_roll3` for month 3 (index 2) equals `150.0` (mean
  of 100, 200, 150) and month 4 equals `216.667` (mean of 200, 150, 300) within
  floating-point tolerance.
- `@pytest.mark.unit` TR-09b — Given only 2 months of history, assert `oil_bbl_roll3`
  is `NaN` for both months (min_periods = window size = 3, not met).
- `@pytest.mark.unit` TR-09b — Given only 4 months of history, assert `oil_bbl_roll6`
  is `NaN` for all 4 months (window=6 not met).
- `@pytest.mark.unit` TR-09c — Given a 4-month oil sequence `[10, 20, 30, 40]`, assert
  `oil_bbl_lag1` equals `[NaN, 10.0, 20.0, 30.0]`.
- `@pytest.mark.unit` — Assert all 9 new columns (`oil_bbl_roll3`, `oil_bbl_roll6`,
  `gas_mcf_roll3`, `gas_mcf_roll6`, `water_bbl_roll3`, `water_bbl_roll6`,
  `oil_bbl_lag1`, `gas_mcf_lag1`, `water_bbl_lag1`) are present in output.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Derived production-rate features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_rate_features(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**
Compute production-per-day and months-since-first-production features inside
`map_partitions`.

Features to add:
- `oil_bbl_per_day` (`float64`): `oil_bbl / days_in_month` where `days_in_month` is
  derived from `production_date` using `production_date.dt.days_in_month`.
- `gas_mcf_per_day` (`float64`): `gas_mcf / days_in_month`.
- `months_since_first_prod` (`float64`): for each lease, the number of months elapsed
  since the lease's first month with `oil_bbl > 0`. Computed as the difference in
  months between `production_date` and the minimum `production_date` where
  `oil_bbl > 0`, per `lease_kid`. If a lease has no months with `oil_bbl > 0`,
  set `months_since_first_prod = NaN` for all its rows.

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given `oil_bbl=310.0` and `production_date=2024-01-01` (January,
  31 days), assert `oil_bbl_per_day = 10.0`.
- `@pytest.mark.unit` — Given `production_date=2024-02-01` (February 2024, 29 days)
  and `oil_bbl=290.0`, assert `oil_bbl_per_day = 10.0`.
- `@pytest.mark.unit` — Given a lease with first production in Jan 2024 and a row in
  Apr 2024, assert `months_since_first_prod = 3.0`.
- `@pytest.mark.unit` — Given a lease with all `oil_bbl=0.0`, assert
  `months_since_first_prod = NaN` for all rows.
- `@pytest.mark.unit` — Assert columns `oil_bbl_per_day`, `gas_mcf_per_day`,
  `months_since_first_prod` are present in output.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Write features Parquet output

**Module:** `kgs_pipeline/features.py`
**Function:** `write_features_parquet(ddf: dd.DataFrame, output_dir: str) -> int`

**Description:**
Write the feature-enriched Dask DataFrame to `data/features/` as Parquet files.

1. Repartition to `max(1, ddf.npartitions // 5)` (target 20-50 output files).
2. Write: `ddf.repartition(npartitions=N).to_parquet(output_dir, write_index=False,
   overwrite=True)`.
3. Return the count of `.parquet` files written.
4. Log the file count, output directory, and total feature columns.

**Dependencies:** `dask`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a small synthetic feature DataFrame, assert
  `write_features_parquet` returns >= 1 and writes Parquet files to the output dir.
- `@pytest.mark.unit` — Assert written files can be read back with `pd.read_parquet`
  without error (TR-18).
- `@pytest.mark.unit` — Assert return value equals actual file count on disk.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Metadata manifest writer

**Module:** `kgs_pipeline/features.py`
**Function:** `write_manifest(ddf: dd.DataFrame, output_dir: str, processing_start: datetime) -> Path`

**Description:**
Write a JSON metadata manifest to `{output_dir}/manifest.json` describing the feature
output. The manifest must include:
- `schema`: dict of `{column_name: dtype_str}` for all columns in `ddf`.
- `feature_columns`: list of derived feature column names (all columns beyond the base
  processed schema).
- `record_count`: total row count (call `len(ddf)` — this triggers a compute; document
  this as an intentional materialisation for manifest purposes).
- `partition_count`: `ddf.npartitions`.
- `processing_start`: ISO-8601 string from the `processing_start` argument.
- `processing_end`: ISO-8601 string of the current UTC time at manifest write time.
- `min_production_date`: ISO-8601 string of the minimum `production_date` in the dataset.
- `max_production_date`: ISO-8601 string of the maximum `production_date` in the dataset.

Return the `Path` to the written manifest file.

**Dependencies:** `json`, `datetime`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a small synthetic Dask DataFrame, assert the manifest
  JSON file is created, is valid JSON, and contains keys `schema`, `feature_columns`,
  `record_count`, `partition_count`, `processing_start`, `processing_end`,
  `min_production_date`, `max_production_date`.
- `@pytest.mark.unit` — Assert `record_count` in the manifest equals the actual row
  count of the DataFrame.
- `@pytest.mark.unit` — Assert the returned `Path` points to an existing file.
- `@pytest.mark.unit` — Assert `processing_end` >= `processing_start` (chronological
  order).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Features orchestrator and CLI

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features(processed_dir: str, output_dir: str) -> Path`
**Entry point:** `main()` and `if __name__ == "__main__"` block

**Description:**
Chain all feature functions:
1. Record `processing_start = datetime.utcnow()`.
2. `read_processed(processed_dir)` → base Dask DataFrame.
3. `add_water_column(ddf)` → with water_bbl.
4. `compute_gor(ddf)` → with gor.
5. `compute_water_cut(ddf)` → with water_cut.
6. `compute_cumulative(ddf)` → with cum_oil, cum_gas, cum_water.
7. `compute_decline_rate(ddf)` → with decline_rate.
8. `compute_rolling_and_lag_features(ddf)` → with rolling and lag columns.
9. `compute_rate_features(ddf)` → with per-day and months_since_first_prod.
10. `write_features_parquet(ddf, output_dir)` → writes Parquet files.
11. Re-read the written Parquet as a fresh Dask DataFrame for manifest computation.
12. `write_manifest(ddf, output_dir, processing_start)` → writes manifest JSON.
13. Return the manifest `Path`.

`main()` uses `argparse`:
- `--processed-dir`: default from `config.PROCESSED_DATA_DIR`
- `--output-dir`: default from `config.FEATURES_DATA_DIR`

Register console script: `kgs-features = "kgs_pipeline.features:main"`

**Test cases:**

- `@pytest.mark.unit` — Mock all sub-functions; assert `run_features` calls them in
  correct order and returns a `Path`.
- `@pytest.mark.integration` — Given real `data/processed/` input, assert `run_features`
  writes Parquet files to `data/features/` and a `manifest.json`, and returns a `Path`
  pointing to the manifest.

**Definition of done:** Orchestrator and CLI implemented, all tests pass, `ruff` and
`mypy` report no errors, console script registered. `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 10: Features domain and technical correctness tests (TR-03, TR-06 to TR-10, TR-14, TR-17 to TR-19)

**Module:** `tests/test_features.py`

**Description:**
Implement all domain and technical correctness tests for the features stage.

**Test cases:**

- `@pytest.mark.unit` TR-01 — Physical bounds: given a row with `oil_bbl=-1.0` passed
  through the full features pipeline, assert no exception is raised and `gor` is `NaN`
  (since `oil_bbl` is negative, treat like zero for GOR purposes, or document the
  expected behaviour explicitly in the test).
- `@pytest.mark.unit` TR-03 — Decline curve monotonicity: given a synthetic lease with
  production `[100, 200, 150, 300]`, assert `cum_oil = [100, 300, 450, 750]` and that
  all consecutive differences of `cum_oil` are >= 0.
- `@pytest.mark.unit` TR-06a — GOR zero denominator: `oil_bbl=0`, `gas_mcf=500` →
  `gor=NaN`, no exception.
- `@pytest.mark.unit` TR-06b — GOR zero-zero: `oil_bbl=0`, `gas_mcf=0` → `gor=NaN`.
- `@pytest.mark.unit` TR-06c — GOR gas-zero: `oil_bbl=100`, `gas_mcf=0` → `gor=0.0`.
- `@pytest.mark.unit` TR-07a — Decline rate clipped at -1.0.
- `@pytest.mark.unit` TR-07b — Decline rate clipped at 10.0.
- `@pytest.mark.unit` TR-07c — Decline rate within bounds passes through unchanged.
- `@pytest.mark.unit` TR-07d — Shut-in followed by production: `decline_rate=NaN`,
  not an extreme pre-clip value.
- `@pytest.mark.unit` TR-08a — Cumulative flat during shut-in month.
- `@pytest.mark.unit` TR-08b — Cumulative = 0 for leading zero-production months.
- `@pytest.mark.unit` TR-08c — Cumulative resumes correctly after shut-in.
- `@pytest.mark.unit` TR-09a — Rolling 3-month average matches hand-computed value for
  known sequence.
- `@pytest.mark.unit` TR-09b — Rolling window larger than history returns NaN.
- `@pytest.mark.unit` TR-09c — Lag-1 for month N equals raw value for month N-1,
  verified for 3 consecutive months.
- `@pytest.mark.unit` TR-09d — GOR formula is `gas_mcf / oil_bbl` (not unit-swapped).
- `@pytest.mark.unit` TR-09e — Water cut formula is `water_bbl / (oil_bbl + water_bbl)`.
- `@pytest.mark.unit` TR-10a — Water cut = 0.0 when `water_bbl=0`, `oil_bbl>0`; row
  retained.
- `@pytest.mark.unit` TR-10b — Water cut = 1.0 when `oil_bbl=0`, `water_bbl>0`; row
  retained.
- `@pytest.mark.unit` TR-10c — Only values outside `[0, 1]` are treated as invalid
  (assert that 0.0 and 1.0 are not flagged).
- `@pytest.mark.integration` TR-14 — Schema stability: read at least 3 feature Parquet
  partition files from `data/features/` and assert all have identical column names and
  dtypes.
- `@pytest.mark.integration` TR-17 — Lazy evaluation: assert each features stage
  function (`add_water_column`, `compute_gor`, `compute_water_cut`, `compute_cumulative`,
  `compute_decline_rate`, `compute_rolling_and_lag_features`, `compute_rate_features`)
  returns `dask.dataframe.DataFrame`, not `pd.DataFrame`.
- `@pytest.mark.integration` TR-18 — Parquet readability: read every `.parquet` file
  in `data/features/` with `pd.read_parquet` and assert no exception.
- `@pytest.mark.unit` TR-19 — Feature column presence: given a synthetic single-lease
  input through the full features pipeline, assert the output DataFrame contains all of:
  `lease_kid`, `production_date`, `oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`,
  `cum_gas`, `cum_water`, `gor`, `water_cut`, `decline_rate`, `oil_bbl_roll3`,
  `oil_bbl_roll6`, `gas_mcf_roll3`, `gas_mcf_roll6`, `water_bbl_roll3`,
  `water_bbl_roll6`, `oil_bbl_lag1`, `gas_mcf_lag1`, `water_bbl_lag1`,
  `oil_bbl_per_day`, `gas_mcf_per_day`, `months_since_first_prod`.

**Definition of done:** All test cases implemented in `tests/test_features.py`, all unit
tests pass, `ruff` and `mypy` report no errors. `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 11: Pipeline orchestrator and end-to-end runner

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `run_pipeline(index_path: str, raw_dir: str, interim_dir: str, processed_dir: str, features_dir: str, min_year: int = 2024, workers: int = 5) -> dict`
**Entry point:** `main()` CLI in `kgs_pipeline/pipeline.py`

**Description:**
Implement the top-level pipeline runner that chains all four stages in order:
1. `run_acquire(index_path, raw_dir, min_year, workers)` → list of downloaded Paths.
2. `run_ingest(raw_dir, interim_dir, min_year)` → interim file count.
3. `run_transform(interim_dir, processed_dir)` → processed file count.
4. `run_features(processed_dir, features_dir)` → manifest Path.
5. Return a summary dict: `{"acquired": len(paths), "interim_files": N, "processed_files": M, "manifest": str(manifest_path)}`.

`main()` uses `argparse`:
- `--index-path`: default from config
- `--raw-dir`: default from config
- `--interim-dir`: default from config
- `--processed-dir`: default from config
- `--features-dir`: default from config
- `--min-year`: integer, default 2024
- `--workers`: integer, default 5
- `--start-year`: alias for `--min-year`
- `--end-year`: integer, ignored in this version (reserved for future filtering; log
  a deprecation notice if provided)

Log structured JSON progress messages at the start and end of each stage.
Exit code 0 on success, 1 on unhandled error.

Register console script: `kgs-pipeline = "kgs_pipeline.pipeline:main"`

**Test cases:**

- `@pytest.mark.unit` — Mock all four stage `run_*` functions; assert `run_pipeline`
  calls them in correct order and the returned dict has all four expected keys.
- `@pytest.mark.unit` — Assert `main()` with `--workers 3 --min-year 2024` calls
  `run_pipeline` with `workers=3` and `min_year=2024`.
- `@pytest.mark.unit` — Assert that if `run_acquire` raises an exception, `main()`
  catches it and raises `SystemExit` with code 1.

**Definition of done:** `run_pipeline` and `main` implemented, all tests pass, console
script registered, `ruff` and `mypy` report no errors. `requirements.txt` updated with
all third-party packages imported in this task.
