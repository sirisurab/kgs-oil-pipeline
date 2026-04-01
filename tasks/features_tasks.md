# Features Component — Task Specifications

## Overview

The features component loads the cleaned Parquet files from `data/processed/clean/`, applies
domain-specific feature engineering (cumulative production, decline rates, ratios, rolling
averages, lag features, well age, formation and county aggregates), encodes categorical
variables, and writes the final ML-ready dataset to `data/processed/features/` as Parquet.
The stage uses Dask with `map_partitions` for partition-level parallelism. All computed
features must satisfy the physical correctness constraints defined in test-requirements.xml.

**Module:** `kgs_pipeline/features.py`
**Entry-point CLI:** invoked by `pipeline.py --features`
**Test file:** `tests/test_features.py`

---

## Output feature schema

The features Parquet output must contain all of the following columns. `[TR-19]`

| Column name             | Type     | Description                                                         |
|-------------------------|----------|---------------------------------------------------------------------|
| LEASE_KID               | str      | Lease identifier                                                    |
| PRODUCT                 | str      | "O" or "G"                                                          |
| production_date         | datetime | First day of production month                                       |
| oil_bbl                 | float    | Monthly oil production (BBL); 0.0 for gas-only leases               |
| gas_mcf                 | float    | Monthly gas production (MCF); 0.0 for oil-only leases               |
| water_bbl               | float    | Monthly water production (BBL); may be NaN if not reported          |
| cum_oil                 | float    | Cumulative oil BBL per lease, monotonically non-decreasing          |
| cum_gas                 | float    | Cumulative gas MCF per lease, monotonically non-decreasing          |
| cum_water               | float    | Cumulative water BBL per lease                                      |
| gor                     | float    | Gas-oil ratio (gas_mcf / oil_bbl); NaN if oil_bbl=0 & gas_mcf > 0  |
| water_cut               | float    | water_bbl / (oil_bbl + water_bbl); range [0, 1]                     |
| decline_rate            | float    | Period-over-period oil decline rate, clipped to [-1.0, 10.0]        |
| well_age_months         | int      | Months since first production date for the lease                    |
| oil_roll3               | float    | 3-month rolling average of oil_bbl                                  |
| oil_roll6               | float    | 6-month rolling average of oil_bbl                                  |
| gas_roll3               | float    | 3-month rolling average of gas_mcf                                  |
| gas_roll6               | float    | 6-month rolling average of gas_mcf                                  |
| water_roll3             | float    | 3-month rolling average of water_bbl                                |
| water_roll6             | float    | 6-month rolling average of water_bbl                                |
| oil_lag1                | float    | oil_bbl lagged by 1 month                                           |
| oil_lag3                | float    | oil_bbl lagged by 3 months                                          |
| gas_lag1                | float    | gas_mcf lagged by 1 month                                           |
| gas_lag3                | float    | gas_mcf lagged by 3 months                                          |
| county_mean_oil         | float    | Mean monthly oil production for the county                          |
| county_std_oil          | float    | Std of monthly oil production for the county                        |
| formation_mean_oil      | float    | Mean monthly oil production for the producing zone/formation        |
| formation_std_oil       | float    | Std of monthly oil production for the producing zone/formation      |
| COUNTY_enc              | int      | LabelEncoded county                                                 |
| PRODUCING_ZONE_enc      | int      | LabelEncoded producing zone                                         |
| OPERATOR_enc            | int      | LabelEncoded operator                                               |
| PRODUCT_enc             | int      | LabelEncoded product ("O"/"G")                                      |

---

## Design decisions and constraints

- All Parquet reads and writes use Dask. No `.compute()` inside module functions except where
  explicitly permitted (e.g., global aggregate lookups, label encoding fit). `[TR-17]`
- After reading cleaned Parquet, immediately repartition to `min(npartitions, 50)`.
- Output file count: repartition to `max(1, total_rows // 500_000)` before writing. Never more
  than 200 output files.
- Per-well time-series operations (cumulative, rolling, lag, decline rate) must be computed
  within each `LEASE_KID` group. Use `ddf.groupby("LEASE_KID").apply(fn, meta=...)` or
  implement via `map_partitions` with sorting guarantees.
- Before any per-well sequential operation, sort within each partition by `(LEASE_KID,
  production_date)` inside a `map_partitions` call.
- Water column: the cleaned data may not have an explicit water column since raw KGS data
  reports oil and gas separately under the PRODUCT column. In this case, `water_bbl` is NaN
  for all rows and cumulative/rolling/lag water features will also be NaN. The pipeline must
  handle this gracefully without raising.
- Physical bounds must be enforced on computed features: GOR >= 0 (or NaN), water_cut ∈ [0,1],
  decline_rate clipped to [-1.0, 10.0], cumulative values non-decreasing. `[TR-01, TR-03]`
- GOR zero-denominator: if oil_bbl == 0 and gas_mcf > 0, GOR = NaN. If both == 0, GOR = 0.0
  (or NaN — document choice). If oil_bbl > 0, GOR = gas_mcf / oil_bbl. `[TR-06]`
- Water cut: water_bbl / (oil_bbl + water_bbl). If denominator == 0, water_cut = NaN. `[TR-10]`
- String `meta=` arguments must use `pd.StringDtype()`, never `"object"`.
- `requirements.txt` must be updated with all third-party packages imported in this module.

---

## Task 01: Implement cleaned data loader

**Module:** `kgs_pipeline/features.py`
**Function:** `load_clean(clean_dir: str) -> dask.dataframe.DataFrame`

**Description:**
Read all Parquet files from `clean_dir` into a Dask DataFrame. Immediately repartition to
`min(npartitions, 50)`. Raise `ValueError` if the directory is empty or has no Parquet files.

**Inputs:** `clean_dir: str` — path to `data/processed/clean/`.

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Write a minimal clean Parquet fixture to `tmp_path`; assert return
  type is `dask.dataframe.DataFrame`. `[TR-17]`
- `@pytest.mark.unit` — Assert `npartitions <= 50`.
- `@pytest.mark.unit` — Given an empty directory, assert `ValueError`.
- `@pytest.mark.integration` — Load from actual `data/processed/clean/`; assert readability.
  `[TR-18]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement oil/gas/water column pivot

**Module:** `kgs_pipeline/features.py`
**Function:** `pivot_products(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
The cleaned data has one row per `(LEASE_KID, MONTH-YEAR, PRODUCT)` where PRODUCT is "O" or "G".
Pivot to a wide format with one row per `(LEASE_KID, production_date)` and separate columns
for oil_bbl, gas_mcf:
1. Filter rows where PRODUCT == "O" → oil_bbl = PRODUCTION; fill NaN with 0.0.
2. Filter rows where PRODUCT == "G" → gas_mcf = PRODUCTION; fill NaN with 0.0.
3. Merge the two filtered DataFrames on `(LEASE_KID, production_date)` (outer join).
4. Fill NaN oil_bbl and gas_mcf with 0.0 after the merge.
5. Add `water_bbl` column as NaN (float) — KGS data does not report water separately.
6. Retain other metadata columns from the "O" rows (COUNTY, PRODUCING_ZONE, OPERATOR,
   LEASE, FIELD, source_file).

This function calls `.compute()` to perform the pivot (it is a reshape operation that is
inherently non-lazy). After pivoting, convert back to a Dask DataFrame and repartition to
`min(npartitions, 50)`.

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame` with columns: LEASE_KID, production_date, oil_bbl,
gas_mcf, water_bbl, COUNTY, PRODUCING_ZONE, OPERATOR, LEASE, FIELD, source_file.

**Edge cases:**
- Lease with only gas records (no oil rows) → oil_bbl = 0.0 for all its rows.
- Lease with only oil records → gas_mcf = 0.0 for all its rows.
- Month present in oil but not gas → after outer merge, gas_mcf filled with 0.0.

**Test cases:**
- `@pytest.mark.unit` — Given a fixture with 2 oil rows and 2 gas rows for the same lease
  and months, assert the pivot produces 2 rows (wide format). `[TR-05]`
- `@pytest.mark.unit` — Given a lease with only "O" rows, assert gas_mcf is 0.0 (not NaN)
  in all output rows.
- `@pytest.mark.unit` — Assert `water_bbl` column exists with NaN values.
- `@pytest.mark.unit` — Assert oil_bbl=0.0 rows are retained (not dropped). `[TR-05]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement cumulative production features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_cumulative(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute cumulative production columns for a **single lease's** sorted time-series DataFrame
(operates on a Pandas DataFrame, called via `groupby.apply` or `map_partitions` with
per-group sorting). Steps:
1. Sort by `production_date` ascending.
2. `cum_oil = oil_bbl.cumsum()` — must be monotonically non-decreasing since oil_bbl >= 0.
3. `cum_gas = gas_mcf.cumsum()` — same constraint.
4. `cum_water = water_bbl.cumsum()` — NaN-safe: use `cumsum(skipna=True)` so NaN months
   propagate the previous cumulative value without resetting. If all water_bbl are NaN,
   cum_water is NaN.
5. Assert (within function) that `cum_oil` is non-decreasing: `assert (cum_oil.diff().dropna() >= 0).all()`. `[TR-03]`
6. Return the DataFrame with `cum_oil`, `cum_gas`, `cum_water` columns added.

**Inputs:** `df: pd.DataFrame` — single-lease sorted time-series.

**Outputs:** `pd.DataFrame` with `cum_oil`, `cum_gas`, `cum_water` added.

**Test cases — [TR-03, TR-08]:**
- `@pytest.mark.unit` — Given oil_bbl = [100, 150, 0, 200] (shut-in at month 3), assert
  cum_oil = [100, 250, 250, 450]. Flat during shut-in, correct resumption. `[TR-08]`
- `@pytest.mark.unit` — Assert cum_oil is monotonically non-decreasing for any non-negative
  oil_bbl input. `[TR-03]`
- `@pytest.mark.unit` — Given oil_bbl = [0, 0, 100, 200] (well not online for first 2 months),
  assert cum_oil = [0, 0, 100, 300]. `[TR-08]`
- `@pytest.mark.unit` — Given oil_bbl = [100, 0, 0, 50] with zero months mid-sequence,
  assert cum_oil stays flat at 100 during the zero months then correctly resumes. `[TR-08]`
- `@pytest.mark.unit` — Given water_bbl = [NaN, NaN, NaN], assert cum_water is all NaN
  (not 0.0).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement GOR and water cut features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_ratios(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute GOR and water cut for a single-lease Pandas DataFrame:

**GOR (gas-oil ratio):** `[TR-06, TR-09]`
- If `oil_bbl > 0`: `gor = gas_mcf / oil_bbl`
- If `oil_bbl == 0` and `gas_mcf > 0`: `gor = NaN` (mathematically undefined)
- If `oil_bbl == 0` and `gas_mcf == 0` (shut-in): `gor = 0.0`
- Must not raise ZeroDivisionError in any case. Use `np.where` or `pd.Series.where`.

**Water cut:** `[TR-09, TR-10]`
- `water_cut = water_bbl / (oil_bbl + water_bbl)`
- If `(oil_bbl + water_bbl) == 0`: `water_cut = NaN`
- Result must be in [0.0, 1.0] or NaN. A result of 0.0 (no water) and 1.0 (100% water) are
  both valid and must NOT be treated as errors. `[TR-10]`
- Values outside [0, 1] indicate a data error and should be set to NaN.

**Inputs:** `df: pd.DataFrame` — single-lease DataFrame with `oil_bbl`, `gas_mcf`, `water_bbl`.

**Outputs:** `pd.DataFrame` with `gor` and `water_cut` columns added.

**Test cases — [TR-01, TR-06, TR-09, TR-10]:**
- `@pytest.mark.unit` — oil_bbl=100, gas_mcf=500 → gor=5.0. `[TR-09]`
- `@pytest.mark.unit` — oil_bbl=0, gas_mcf=500 → gor=NaN (not an exception). `[TR-06]`
- `@pytest.mark.unit` — oil_bbl=0, gas_mcf=0 → gor=0.0. `[TR-06]`
- `@pytest.mark.unit` — oil_bbl=100, gas_mcf=0 → gor=0.0. `[TR-06]`
- `@pytest.mark.unit` — water_bbl=0, oil_bbl=100 → water_cut=0.0 (valid, not an outlier).
  `[TR-10]`
- `@pytest.mark.unit` — water_bbl=200, oil_bbl=0 → water_cut=1.0 (valid end-of-life well).
  `[TR-10]`
- `@pytest.mark.unit` — water_bbl=0, oil_bbl=0 → water_cut=NaN. `[TR-10]`
- `@pytest.mark.unit` — Assert no `ZeroDivisionError` is raised for any combination of zeros.
  `[TR-06]`
- `@pytest.mark.unit` — Assert GOR formula is gas_mcf / oil_bbl, not gas_bbl / oil_mcf.
  `[TR-09]`
- `@pytest.mark.unit` — Assert water_cut formula denominator is (oil_bbl + water_bbl), not
  just oil_bbl. `[TR-09]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement decline rate feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_decline_rate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute the period-over-period production decline rate for each month in a single-lease
sorted time-series. `[TR-07]`

Formula: `decline_rate[t] = (oil_bbl[t-1] - oil_bbl[t]) / oil_bbl[t-1]`
- If `oil_bbl[t-1] == 0`: the raw result would be division by zero. Handle as follows:
  - If `oil_bbl[t] == 0` (two consecutive zero months, shut-in): `decline_rate = 0.0`
  - If `oil_bbl[t] > 0` (production resumes after shut-in): `decline_rate = -1.0` (maximum
    negative clip value — represents infinite negative decline, i.e., production came back).
- After raw computation, clip to `[-1.0, 10.0]`. `[TR-07]`
- First row for each lease (no prior period): `decline_rate = NaN`.

**Inputs:** `df: pd.DataFrame` — single-lease sorted DataFrame with `oil_bbl`.

**Outputs:** `pd.DataFrame` with `decline_rate` column added.

**Test cases — [TR-07]:**
- `@pytest.mark.unit` — oil_bbl = [200, 100, 50] → decline_rate = [NaN, 0.5, 0.5]. `[TR-07]`
- `@pytest.mark.unit` — A decline rate computed as 15.0 (above clip bound) is clipped to 10.0.
  `[TR-07]`
- `@pytest.mark.unit` — A decline rate computed as -2.0 (below clip bound) is clipped to -1.0.
  `[TR-07]`
- `@pytest.mark.unit` — Value within [-1.0, 10.0] passes through unchanged. `[TR-07]`
- `@pytest.mark.unit` — oil_bbl[t-1] = 0, oil_bbl[t] = 0 → decline_rate = 0.0 (no exception).
  `[TR-07]`
- `@pytest.mark.unit` — oil_bbl[t-1] = 0, oil_bbl[t] = 100 → decline_rate = -1.0 (clipped).
  `[TR-07]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement well age feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_well_age(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute `well_age_months` as the number of months elapsed since the earliest `production_date`
for the lease:
- `well_age_months[t] = (production_date[t].year - first_date.year) * 12 + (production_date[t].month - first_date.month)`
- First month = 0.
- Return type: `int` (or `Int64` nullable integer).

**Inputs:** `df: pd.DataFrame` — single-lease sorted DataFrame with `production_date`.

**Outputs:** `pd.DataFrame` with `well_age_months` column added.

**Test cases:**
- `@pytest.mark.unit` — Given dates [2024-01-01, 2024-02-01, 2024-04-01], assert
  well_age_months = [0, 1, 3].
- `@pytest.mark.unit` — Single-row DataFrame → well_age_months = 0.
- `@pytest.mark.unit` — Assert dtype is integer (not float).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement rolling average features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_rolling(df: pd.DataFrame, windows: list[int] = [3, 6]) -> pd.DataFrame`

**Description:**
Compute rolling averages for `oil_bbl`, `gas_mcf`, `water_bbl` over the specified windows
within a single-lease sorted time-series. Use `pd.Series.rolling(window=W, min_periods=1)`.
Name output columns `oil_roll3`, `oil_roll6`, `gas_roll3`, `gas_roll6`, `water_roll3`,
`water_roll6` for windows [3, 6].

When the rolling window is larger than available history (e.g., first 2 months of a 3-month
window), `min_periods=1` ensures a partial average is computed, not NaN. This is the
specified behaviour. `[TR-09]`

**Inputs:**
- `df: pd.DataFrame` — single-lease sorted DataFrame.
- `windows: list[int]` — rolling window sizes.

**Outputs:** `pd.DataFrame` with rolling columns added.

**Test cases — [TR-09]:**
- `@pytest.mark.unit` — oil_bbl = [100, 200, 300, 400]; assert oil_roll3 matches
  hand-computed values: [100.0, 150.0, 200.0, 300.0] (with min_periods=1). `[TR-09]`
- `@pytest.mark.unit` — oil_bbl = [100, 200, 300, 400, 500, 600]; assert oil_roll6 matches
  hand-computed values for all 6 rows. `[TR-09]`
- `@pytest.mark.unit` — First row of oil_roll3 equals oil_bbl[0] (window of 1 = identity).
- `@pytest.mark.unit` — Assert water_roll3 is NaN for all rows when water_bbl is all NaN.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement lag features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_lags(df: pd.DataFrame, lags: list[int] = [1, 3]) -> pd.DataFrame`

**Description:**
Compute lag features for `oil_bbl` and `gas_mcf` within a single-lease sorted time-series.
Use `pd.Series.shift(n)`. Lag-N for row T equals the raw production value at row T-N.
Name columns: `oil_lag1`, `oil_lag3`, `gas_lag1`, `gas_lag3`.

The first N rows for each lag will be NaN (no prior period available).

**Inputs:**
- `df: pd.DataFrame` — single-lease sorted DataFrame.
- `lags: list[int]` — lag periods.

**Outputs:** `pd.DataFrame` with lag columns added.

**Test cases — [TR-09]:**
- `@pytest.mark.unit` — oil_bbl = [100, 200, 300, 400]; assert oil_lag1 = [NaN, 100, 200, 300].
  Verify lag-1 at month N equals the raw value at month N-1 for at least 3 consecutive months.
  `[TR-09]`
- `@pytest.mark.unit` — oil_bbl = [100, 200, 300, 400]; assert oil_lag3 = [NaN, NaN, NaN, 100].
- `@pytest.mark.unit` — Assert first row of oil_lag1 is NaN.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement aggregate features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_aggregates(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute county-level and formation-level aggregate features and join them back to the main
DataFrame:

1. **County aggregates:** compute `county_mean_oil` and `county_std_oil` — the mean and
   standard deviation of `oil_bbl` grouped by `COUNTY`. This requires calling `.compute()`
   once to get the global county stats as a Pandas lookup table, then merging back via
   `map_partitions`.
2. **Formation aggregates:** compute `formation_mean_oil` and `formation_std_oil` — mean and
   std of `oil_bbl` grouped by `PRODUCING_ZONE`. Same approach.
3. Merge the aggregate lookup tables back to the Dask DataFrame via `map_partitions` using
   `pd.DataFrame.merge(..., on="COUNTY", how="left")` and
   `pd.DataFrame.merge(..., on="PRODUCING_ZONE", how="left")`.
4. Fill NaN values in aggregate columns with the global mean where the group had insufficient
   data (e.g., a county with only 1 record has undefined std → fill std with 0.0).

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame` with `county_mean_oil`, `county_std_oil`,
`formation_mean_oil`, `formation_std_oil` columns added.

**Test cases:**
- `@pytest.mark.unit` — Given 3 leases: 2 in county "Allen" (oil 100, 200) and 1 in county
  "Barton" (oil 300); assert county_mean_oil for "Allen" rows = 150.0 and for "Barton" = 300.0.
- `@pytest.mark.unit` — Assert county_std_oil for a county with a single record is 0.0 (not NaN).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`. `[TR-17]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement categorical encoding

**Module:** `kgs_pipeline/features.py`
**Function:** `encode_categoricals(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Encode categorical columns using `sklearn.preprocessing.LabelEncoder`. Columns to encode:
`COUNTY`, `PRODUCING_ZONE`, `OPERATOR`, `PRODUCT`. Output columns: `COUNTY_enc`,
`PRODUCING_ZONE_enc`, `OPERATOR_enc`, `PRODUCT_enc` (integer dtype).

Steps:
1. Call `.compute()` once to collect all unique values per categorical column (required to fit
   LabelEncoder globally).
2. Fit one `LabelEncoder` per column on the global unique values.
3. Apply encoding via `map_partitions` using the fitted encoders.
4. Handle unseen values gracefully — if a partition contains a value not seen during fit
   (should not happen in a stable pipeline, but defensive coding), encode as -1.
5. Return the Dask DataFrame with the `_enc` columns appended; original categorical columns
   are retained.

**Inputs:** `ddf: dask.dataframe.DataFrame`

**Outputs:** `dask.dataframe.DataFrame` with `COUNTY_enc`, `PRODUCING_ZONE_enc`,
`OPERATOR_enc`, `PRODUCT_enc` added.

**Test cases:**
- `@pytest.mark.unit` — Given PRODUCT column with values ["O", "G", "O"], assert PRODUCT_enc
  is integer-typed with exactly 2 distinct values.
- `@pytest.mark.unit` — Assert COUNTY_enc dtype is integer.
- `@pytest.mark.unit` — Assert original COUNTY column is still present alongside COUNTY_enc.
- `@pytest.mark.unit` — Given an unseen category value introduced in a test partition,
  assert encoding returns -1 (not a crash).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 11: Implement per-lease feature pipeline dispatcher

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_per_lease_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Apply all per-lease sequential time-series feature functions to a single-lease Pandas
DataFrame. This function is used as the callable inside a `groupby("LEASE_KID").apply` or
dispatched via `map_partitions` after grouping. It chains:
1. Sort by `production_date`.
2. `compute_cumulative(df)`
3. `compute_ratios(df)`
4. `compute_decline_rate(df)`
5. `compute_well_age(df)`
6. `compute_rolling(df)`
7. `compute_lags(df)`
8. Return the fully-featured per-lease DataFrame.

If any per-lease computation raises an exception, log a WARNING with the LEASE_KID and
return the input DataFrame unmodified for that lease (do not propagate — other leases must
continue).

**Inputs:** `df: pd.DataFrame` — rows for one `LEASE_KID`, sorted by `production_date`.

**Outputs:** `pd.DataFrame` — same rows with all feature columns added.

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic 6-month single-lease DataFrame, assert the output
  contains all columns: cum_oil, cum_gas, cum_water, gor, water_cut, decline_rate,
  well_age_months, oil_roll3, oil_roll6, gas_roll3, gas_roll6, water_roll3, water_roll6,
  oil_lag1, oil_lag3, gas_lag1, gas_lag3. `[TR-19]`
- `@pytest.mark.unit` — Assert cum_oil is monotonically non-decreasing. `[TR-03]`
- `@pytest.mark.unit` — Assert all oil_bbl values >= 0.0. `[TR-01]`
- `@pytest.mark.unit` — Assert gor >= 0.0 wherever it is not NaN. `[TR-01]`
- `@pytest.mark.unit` — Assert water_cut is in [0.0, 1.0] wherever it is not NaN. `[TR-01]`
- `@pytest.mark.unit` — Assert decline_rate is in [-1.0, 10.0] for all non-NaN values. `[TR-07]`
- `@pytest.mark.unit` — If the per-lease function raises an exception for a corrupt input,
  assert the function returns the input DataFrame unchanged (no crash).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 12: Implement features orchestrator

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features(clean_dir: str, output_dir: str) -> dask.dataframe.DataFrame`

**Description:**
Main entry point for the features stage. Chains all feature engineering steps and writes
the final ML-ready dataset:
1. `load_clean(clean_dir)` → `ddf`
2. `pivot_products(ddf)` → `ddf`
3. Apply `compute_per_lease_features` via `ddf.groupby("LEASE_KID", group_keys=False).apply(compute_per_lease_features, meta=...)`
4. `compute_aggregates(ddf)` → `ddf`
5. `encode_categoricals(ddf)` → `ddf`
6. Repartition to `max(1, estimated_rows // 500_000)` — estimate without full `.compute()`.
7. Write to `output_dir` via `ddf.to_parquet(output_dir, write_index=False)`.
8. Re-read: `ddf_features = dask.dataframe.read_parquet(output_dir)`.
9. Repartition to `min(npartitions, 50)`.
10. Return `ddf_features`.

The returned Dask DataFrame must not have been computed. `[TR-17]`

**Inputs:**
- `clean_dir: str` — path to `data/processed/clean/`.
- `output_dir: str` — path to `data/processed/features/`.

**Outputs:** `dask.dataframe.DataFrame`

**Test cases:**
- `@pytest.mark.unit` — Run `run_features` on a synthetic 3-lease 6-month fixture; assert
  return type is `dask.dataframe.DataFrame`. `[TR-17]`
- `@pytest.mark.unit` — Assert the output DataFrame contains all columns listed in the output
  feature schema table (see overview). `[TR-19]`
- `@pytest.mark.unit` — Assert no PRODUCTION or oil_bbl value < 0.0 in the output. `[TR-01]`
- `@pytest.mark.unit` — Assert cum_oil is monotonically non-decreasing per lease in the
  output. `[TR-03]`
- `@pytest.mark.unit` — Read back the written Parquet with a fresh `dask.dataframe.read_parquet`
  call; assert success. `[TR-18]`
- `@pytest.mark.unit` — Sample 2 partitions from the output; assert identical column names
  and dtypes (schema stability). `[TR-14]`
- `@pytest.mark.unit` — Assert output file count is between 1 and 200.
- `@pytest.mark.integration` — Run against actual `data/processed/clean/`; assert output
  at `data/processed/features/` is readable and contains all expected feature columns. `[TR-18, TR-19]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 13: Implement feature correctness and completeness tests

**Module:** `tests/test_features.py`
**Test functions (dedicated test functions, separate from per-task tests above):**

**`test_feature_column_presence`** `[TR-19]`:
Using a known synthetic single-well input (6 months, known oil/gas values), call
`run_features` and assert the output contains every column listed in the feature schema table.
This test exists to catch silent column drops due to computation errors.

**`test_cumulative_monotonicity`** `[TR-03]`:
For each unique LEASE_KID in the output of `run_features` on the synthetic fixture, assert
that `cum_oil.diff().dropna()` >= 0 for all values (non-decreasing). Repeat for `cum_gas`.

**`test_gor_formula_correctness`** `[TR-09]`:
Given a row with oil_bbl=200, gas_mcf=1000, assert gor=5.0.
Given a row with oil_bbl=0, gas_mcf=500, assert gor is NaN.
Given a row with oil_bbl=100, gas_mcf=0, assert gor=0.0.
Verify formula direction: gas / oil, not oil / gas.

**`test_water_cut_formula_correctness`** `[TR-09]`:
Given water_bbl=200, oil_bbl=200, assert water_cut=0.5.
Given water_bbl=0, oil_bbl=100, assert water_cut=0.0.
Given water_bbl=100, oil_bbl=0, assert water_cut=1.0.

**`test_decline_rate_bounds`** `[TR-07]`:
Construct a sequence where the raw decline exceeds 10.0; assert clipped to 10.0.
Construct a sequence where the raw decline is below -1.0; assert clipped to -1.0.
Construct a shut-in sequence (two consecutive zeros); assert no unclipped extreme value.

**`test_rolling_correctness`** `[TR-09]`:
Given oil_bbl = [100, 200, 300, 400, 500, 600] for a 6-month lease, hand-compute expected
rolling 3-month and 6-month averages and assert they match `compute_rolling` output exactly.

**`test_lag_correctness`** `[TR-09]`:
Given oil_bbl = [100, 200, 300, 400], assert oil_lag1 at index 1 = 100, at index 2 = 200,
at index 3 = 300 (three consecutive months verified).

**`test_schema_stability_across_partitions`** `[TR-14]`:
Read 2+ partition files from `data/processed/features/` output (or tmp_path fixture); assert
all have identical column names and dtypes.

**`test_lazy_evaluation`** `[TR-17]`:
Assert that calling `run_features` returns a `dask.dataframe.DataFrame`, not a `pd.DataFrame`.

All tests above:
- `@pytest.mark.unit` for synthetic fixture tests.
- `@pytest.mark.integration` for tests reading from actual `data/processed/` paths.

**Definition of done:** All test functions implemented, all assertions pass, ruff and mypy
report no errors.
