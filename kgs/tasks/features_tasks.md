# Features (Process) Component — Task Specifications

## Overview

The features component loads the cleaned, per-well processed Parquet store written by the
clean component and engineers ML-ready features for each well. Features include time-based
metrics, rolling statistics, cumulative production, decline rate, GOR/WOR placeholders,
operator/county/field aggregations, and categorical label encoding. Output is written as a
Parquet store to `data/processed/features/` partitioned by `well_id`.

All functions except the writer return lazy Dask DataFrames. The only `.compute()` call is
inside `write_features_parquet()`.

**Source module:** `kgs_pipeline/process.py`
**Test file:** `tests/test_process.py`
**Config:** `kgs_pipeline/config.py`

---

## Output Feature Schema

| Column                        | Type          | Notes                                                          |
|-------------------------------|---------------|----------------------------------------------------------------|
| well_id                       | str           | Unique well identifier (API number or synthetic LEASE_ ID)    |
| lease_kid                     | str           | Source lease ID                                               |
| production_date               | datetime64[ns]| First day of production month                                 |
| product                       | str           | "O" or "G"                                                    |
| county                        | str           | County name                                                   |
| operator                      | str           | Operator name                                                 |
| field_name                    | str           | Field name                                                    |
| producing_zone                | str           | Producing formation                                           |
| production                    | float64       | Monthly production (BBL or MCF)                               |
| unit                          | str           | "BBL" or "MCF"                                                |
| outlier_flag                  | bool          | True if production flagged as outlier                         |
| production_year               | int16         | Calendar year                                                  |
| production_month              | int8          | Calendar month (1–12)                                         |
| production_quarter            | int8          | Quarter (1–4)                                                 |
| months_since_first_prod       | int16         | Months elapsed since well's first recorded production month   |
| cumulative_production         | float64       | Running cumulative production per well per product (Np or Gp) |
| decline_rate                  | float64       | Month-over-month fractional decline; clipped to [-1.0, 10.0]  |
| rolling_mean_3m               | float64       | 3-month rolling mean of production (per well, per product)    |
| rolling_std_3m                | float64       | 3-month rolling std of production (per well, per product)     |
| rolling_mean_6m               | float64       | 6-month rolling mean of production (per well, per product)    |
| rolling_std_6m                | float64       | 6-month rolling std of production (per well, per product)     |
| gor                           | float64       | Gas-Oil Ratio (MCF/BBL) per well-month; NaN if oil-only well  |
| water_cut                     | float64       | Water cut placeholder (NaN — requires water production data)  |
| wor                           | float64       | Water-Oil Ratio placeholder (NaN — requires water data)       |
| county_monthly_production     | float64       | County-level total monthly production for the same product    |
| operator_monthly_production   | float64       | Operator-level total monthly production for the same product  |
| county_encoded                | int16         | Integer label encoding for county                             |
| operator_encoded              | int16         | Integer label encoding for operator                           |
| field_name_encoded            | int16         | Integer label encoding for field_name                         |
| producing_zone_encoded        | int16         | Integer label encoding for producing_zone                     |
| product_encoded               | int8          | Integer label encoding for product (0=G, 1=O)                |

---

## Design Decisions & Constraints

- All per-well computations (cumulative production, decline rate, rolling statistics,
  months since first production) are computed per `[well_id, product]` group to keep oil
  and gas records separate.
- Rolling windows are computed with `min_periods=1` so early months with fewer than 3/6
  observations still receive a value.
- Cumulative production (`cumulative_production`) is the running total using
  `groupby(...).cumsum()`. This must be monotonically non-decreasing per well after sorting
  by `production_date`.
- Decline rate is defined as `(prod_t-1 - prod_t) / prod_t-1`. If `prod_t-1 == 0`, the
  result is `NaN`. Values are clipped to the range `[-1.0, 10.0]` to handle extreme outliers.
- `months_since_first_prod` is computed as the number of months between `production_date` and
  the earliest `production_date` for that `[well_id, product]`.
- GOR is computed by pivoting oil and gas production for the same `well_id` and
  `production_date`, then calculating `gas_production / oil_production`. If oil production
  is zero or the well has no gas records, `gor = NaN`.
- `water_cut` and `wor` are always `NaN` in this pipeline version (water production data is
  not available in the KGS monthly file).
- County-level and operator-level monthly aggregations are computed as Dask groupby
  aggregations and joined back onto the per-well DataFrame.
- Label encoding maps are computed once over the full dataset (using `.compute()` on the
  unique value series) before being broadcast to all partitions, ensuring consistent integer
  codes across all Dask partitions.
- Output is written to `data/processed/features/` partitioned by `well_id` using
  `engine="pyarrow"`, `compression="snappy"`, explicit `pyarrow.Schema`.
- All functions return `dask.dataframe.DataFrame` except the writer.
- Logging via Python's standard `logging` module at DEBUG/INFO/WARNING/ERROR levels.

---

## Task 23: Implement processed Parquet loader

**Module:** `kgs_pipeline/process.py`
**Function:** `load_processed_parquet(processed_dir: Path) -> dask.dataframe.DataFrame`

**Description:**
- Load the partitioned Parquet dataset from `processed_dir` using `dask.dataframe.read_parquet()`.
- Return a lazy Dask DataFrame.
- Log the number of partitions and detected schema at DEBUG level.

**Error handling:**
- If `processed_dir` does not exist or contains no Parquet files, raise `FileNotFoundError`.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Write a minimal processed-schema Parquet file to `tmp_path`; assert
  the function returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a non-existent path, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given real `data/processed/` data, assert the returned
  DataFrame has non-zero partition count.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 24: Implement time-based feature engineering

**Module:** `kgs_pipeline/process.py`
**Function:** `add_time_features(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Add the following columns derived from `production_date`:
  - `production_year` (`int16`): `.dt.year`
  - `production_month` (`int8`): `.dt.month`
  - `production_quarter` (`int8`): `.dt.quarter`
- Add `months_since_first_prod` (`int16`):
  - For each `[well_id, product]` group, find `min(production_date)`.
  - Compute `months_since_first_prod` as the number of complete months between
    `min(production_date)` and `production_date` for each row.
  - Use `(production_date.dt.year - first_date.dt.year) * 12 + (production_date.dt.month - first_date.dt.month)`.
- Use `map_partitions` with a custom helper for group-level computations.
- Return a lazy Dask DataFrame.

**Error handling:**
- If `production_date` is absent or not `datetime64[ns]`, raise `TypeError`.
- If `well_id` or `product` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given `production_date = "2021-03-01"`, assert `production_year=2021`,
  `production_month=3`, `production_quarter=1`.
- `@pytest.mark.unit` — Given a well with first production `"2020-01-01"` and a row dated
  `"2021-01-01"`, assert `months_since_first_prod = 12`.
- `@pytest.mark.unit` — Given a single-month well, assert `months_since_first_prod = 0`.
- `@pytest.mark.unit` — Assert `production_year` dtype is `int16`.
- `@pytest.mark.unit` — Assert `production_month` dtype is `int8`.
- `@pytest.mark.unit` — Assert `production_quarter` dtype is `int8`.
- `@pytest.mark.unit` — Assert `months_since_first_prod` is non-negative for all rows.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 25: Implement cumulative production feature

**Module:** `kgs_pipeline/process.py`
**Function:** `add_cumulative_production(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Compute `cumulative_production` (`float64`) as the running sum of `production` per
  `[well_id, product]` group, ordered by `production_date` ascending.
- The DataFrame must already be sorted by `[well_id, product, production_date]` ascending
  (guaranteed by the clean component) before this function is called.
- Use `groupby(["well_id", "product"])["production"].cumsum()` applied via `map_partitions`.
- Zero production months contribute `0.0` to the cumulative — they do not reset it.
- NaN production months contribute `NaN` to the cumulative from that point forward within the
  group (standard pandas `cumsum` behaviour with `skipna=False`).
- Return a lazy Dask DataFrame.

**Error handling:**
- If `well_id`, `product`, `production`, or `production_date` are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with monthly production `[100, 200, 150]`, assert
  cumulative is `[100, 300, 450]`.
- `@pytest.mark.unit` — Given a zero production month (`production=0.0`), assert cumulative
  is unchanged from the previous month (adds 0).
- `@pytest.mark.unit` — Assert `cumulative_production` is monotonically non-decreasing per
  well per product (domain law: a well cannot un-produce oil).
- `@pytest.mark.unit` — Assert `cumulative_production` dtype is `float64`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — For a sample of 20 wells in the features output, assert
  `cumulative_production` is monotonically non-decreasing for every well.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 26: Implement decline rate feature

**Module:** `kgs_pipeline/process.py`
**Function:** `add_decline_rate(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Compute `decline_rate` (`float64`) as the month-over-month fractional production decline:
  `decline_rate = (production_t-1 - production_t) / production_t-1`
  where `production_t-1` is the previous month's production for the same `[well_id, product]`
  group.
- A positive `decline_rate` means production declined; negative means production increased.
- If `production_t-1 == 0.0` or `NaN`, set `decline_rate = NaN` for that month.
- Clip the result to `[-1.0, 10.0]` to limit extreme outlier impact on ML models.
- The first record for each well (no prior month) gets `decline_rate = NaN`.
- Use `groupby(...).shift(1)` for the lagged value, applied via `map_partitions`.
- Return a lazy Dask DataFrame.

**Error handling:**
- If `well_id`, `product`, `production`, or `production_date` are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given production `[100, 80]`, assert `decline_rate` for the second
  row is `(100 - 80) / 100 = 0.2`.
- `@pytest.mark.unit` — Given production `[100, 120]`, assert `decline_rate` for the second
  row is `(100 - 120) / 100 = -0.2`.
- `@pytest.mark.unit` — Given `production_t-1 = 0.0`, assert `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert the first record per well has `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert `decline_rate` values are clipped to the range `[-1.0, 10.0]`.
- `@pytest.mark.unit` — Assert `decline_rate` dtype is `float64`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 27: Implement rolling statistics features

**Module:** `kgs_pipeline/process.py`
**Function:** `add_rolling_statistics(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- For each `[well_id, product]` group, ordered by `production_date`, compute:
  - `rolling_mean_3m` (`float64`): 3-month rolling mean of `production`, `min_periods=1`.
  - `rolling_std_3m` (`float64`): 3-month rolling standard deviation of `production`,
    `min_periods=1`. A single-value window produces `NaN` (std undefined with 1 observation).
  - `rolling_mean_6m` (`float64`): 6-month rolling mean of `production`, `min_periods=1`.
  - `rolling_std_6m` (`float64`): 6-month rolling standard deviation of `production`,
    `min_periods=1`.
- Apply rolling computations using `pandas.DataFrame.groupby(...).rolling(...)` inside
  `map_partitions`. The data must already be sorted by `[well_id, product, production_date]`.
- Include `min_periods=1` so wells with fewer than 3 or 6 months of data still receive values.
- Return a lazy Dask DataFrame.

**Error handling:**
- If any required column is absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given production `[100, 200, 300]` for a single well, assert
  `rolling_mean_3m` for the third row is `(100+200+300)/3 = 200.0`.
- `@pytest.mark.unit` — Given only 1 production row for a well, assert `rolling_mean_3m = 100.0`
  (min_periods=1 allows a single-point mean).
- `@pytest.mark.unit` — Given only 1 production row for a well, assert `rolling_std_3m = NaN`
  (std is undefined for a single observation).
- `@pytest.mark.unit` — Assert `rolling_mean_3m` dtype is `float64`.
- `@pytest.mark.unit` — Assert `rolling_std_3m` dtype is `float64`.
- `@pytest.mark.unit` — Assert `rolling_mean_6m` dtype is `float64`.
- `@pytest.mark.unit` — Assert that rolling statistics do not bleed across wells (two different
  well IDs in the same DataFrame produce independent rolling windows).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 28: Implement GOR and WOR placeholder features

**Module:** `kgs_pipeline/process.py`
**Function:** `add_gor_wor_features(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Compute `gor` (Gas-Oil Ratio, MCF/BBL) per `[well_id, production_date]`:
  - Pivot the DataFrame on `product` to get oil and gas production side by side for the same
    well-month. This requires `map_partitions` with a pandas pivot on `product`.
  - `gor = gas_production / oil_production`. If `oil_production == 0` or `NaN`, `gor = NaN`.
  - Join the computed GOR back onto the original rows. Oil rows and gas rows for the same
    well-month will both carry the same `gor` value.
  - Wells that produce only oil or only gas will have `gor = NaN`.
- Add `water_cut` as a `float64` column of all `NaN` values (water data not available).
- Add `wor` as a `float64` column of all `NaN` values (water data not available).
- Return a lazy Dask DataFrame.

**Error handling:**
- If `well_id`, `product`, `production`, or `production_date` are absent, raise `KeyError`.
- Division by zero for GOR is handled by returning `NaN`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with oil production `100.0 BBL` and gas production
  `500.0 MCF` in the same month, assert `gor = 5.0` for both the oil and gas rows.
- `@pytest.mark.unit` — Given a well with oil production `0.0 BBL` in a month, assert
  `gor = NaN` for that month.
- `@pytest.mark.unit` — Given an oil-only well, assert `gor = NaN` for all its rows.
- `@pytest.mark.unit` — Assert `water_cut` column contains only `NaN` values.
- `@pytest.mark.unit` — Assert `wor` column contains only `NaN` values.
- `@pytest.mark.unit` — Assert `gor`, `water_cut`, `wor` dtypes are `float64`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 29: Implement operator-level and county-level aggregation features

**Module:** `kgs_pipeline/process.py`
**Function:**
`add_aggregation_features(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Compute the following aggregation features and join them back onto the per-row DataFrame:
  - `county_monthly_production` (`float64`): Total production by `[county, product,
    production_date]`. Sum over all wells in the same county, product type, and month.
  - `operator_monthly_production` (`float64`): Total production by `[operator, product,
    production_date]`. Sum over all wells managed by the same operator, product type, and month.
- Compute aggregations using `dask.dataframe.groupby(...).agg({"production": "sum"})` — these
  are cross-partition aggregations and will trigger a Dask shuffle.
- Rename the aggregated production column to avoid collision before joining.
- Left-join the aggregation results back onto the original DataFrame using
  `df.merge(county_agg, on=[...], how="left")`.
- Return a lazy Dask DataFrame.

**Error handling:**
- If `county`, `operator`, `product`, `production`, or `production_date` are absent, raise
  `KeyError`.
- Null `county` or `operator` values are grouped under the label `"UNKNOWN"` before
  aggregating; log a WARNING if any such substitution is made.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given 2 oil wells in the same county and month with productions of
  100 and 200, assert `county_monthly_production = 300.0` for both rows.
- `@pytest.mark.unit` — Given 2 oil wells operated by the same operator in the same month
  with productions of 50 and 75, assert `operator_monthly_production = 125.0` for both rows.
- `@pytest.mark.unit` — Assert county aggregations do not bleed across products (oil county
  total is not inflated by gas records).
- `@pytest.mark.unit` — Assert `county_monthly_production` and `operator_monthly_production`
  dtypes are `float64`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — For a sample of real county-month data, assert that the sum of
  individual well productions matches the `county_monthly_production` value.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 30: Implement categorical label encoding

**Module:** `kgs_pipeline/process.py`
**Function:**
`encode_categorical_features(df: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
- Encode the following categorical columns as integer labels:
  - `county` → `county_encoded` (`int16`)
  - `operator` → `operator_encoded` (`int16`)
  - `field_name` → `field_name_encoded` (`int16`)
  - `producing_zone` → `producing_zone_encoded` (`int16`)
  - `product` → `product_encoded` (`int8`) — fixed encoding: `"G"=0`, `"O"=1`
- Build a global encoding map for each categorical column by calling
  `.compute()` on the unique-value series once, sorting the values alphabetically, and
  assigning integer codes starting from 0.
- Broadcast the encoding maps as Python dictionaries into `map_partitions` for partition-level
  `.map()` replacement — do not recompute the map per partition.
- Unknown values (values not in the encoding map) receive code `-1`.
- Retain the original string columns alongside the encoded columns.
- Return a lazy Dask DataFrame.

**Error handling:**
- If any categorical column is absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given `product="O"`, assert `product_encoded = 1`.
- `@pytest.mark.unit` — Given `product="G"`, assert `product_encoded = 0`.
- `@pytest.mark.unit` — Given a column with 3 unique counties `["Anderson", "Butler",
  "Chase"]`, assert the encoding is `{"Anderson": 0, "Butler": 1, "Chase": 2}` (alphabetical).
- `@pytest.mark.unit` — Given an unknown county value not in the training set, assert
  `county_encoded = -1`.
- `@pytest.mark.unit` — Assert encoding maps are consistent across all Dask partitions (same
  string maps to the same integer in every partition).
- `@pytest.mark.unit` — Assert original string columns (`county`, `operator`, `field_name`,
  `producing_zone`, `product`) are still present in the output alongside encoded columns.
- `@pytest.mark.unit` — Assert `county_encoded` dtype is `int16`.
- `@pytest.mark.unit` — Assert `product_encoded` dtype is `int8`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 31: Implement features Parquet writer

**Module:** `kgs_pipeline/process.py`
**Function:** `write_features_parquet(df: dask.dataframe.DataFrame, output_dir: Path) -> None`

**Description:**
- Write the full feature-engineered Dask DataFrame to `output_dir` as partitioned Parquet.
- Partition by `well_id` using `df.to_parquet(output_dir, partition_on=["well_id"], ...)`.
- Use `write_index=False`, `engine="pyarrow"`, `compression="snappy"`.
- Pass an explicit `pyarrow.Schema` matching the output feature schema defined in this document.
- Create `output_dir` if it does not exist.
- This is the only `.compute()` trigger in the process module.
- Log start and completion times and total row count at INFO level.

**Error handling:**
- If `output_dir` cannot be created, let `OSError` propagate.
- If the DataFrame is empty, log a WARNING and return without writing.

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small features Dask DataFrame, assert
  `write_features_parquet` creates at least 1 Parquet file in `tmp_path`.
- `@pytest.mark.unit` — Assert each written file is readable by `pandas.read_parquet()`.
- `@pytest.mark.unit` — Assert partition directories are named by `well_id`.
- `@pytest.mark.unit` — Assert each partition file contains rows for exactly one `well_id`
  value (partition correctness).
- `@pytest.mark.unit` — Sample two partition files and assert they share identical column
  names and dtypes (schema stability).
- `@pytest.mark.unit` — Given an empty DataFrame, assert no files are written and a WARNING
  is logged.
- `@pytest.mark.integration` — Run the full features pipeline on real processed data; assert
  `data/processed/features/` is non-empty and all Parquet files are readable.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 32: Implement features pipeline entry-point

**Module:** `kgs_pipeline/process.py`
**Function:**
`run_features_pipeline(processed_dir: Path | None = None, output_dir: Path | None = None) -> None`

**Description:**
- Synchronous entry-point that orchestrates the feature engineering workflow in order:
  1. `load_processed_parquet(processed_dir)` → `df`
  2. `add_time_features(df)` → `df`
  3. `add_cumulative_production(df)` → `df`
  4. `add_decline_rate(df)` → `df`
  5. `add_rolling_statistics(df)` → `df`
  6. `add_gor_wor_features(df)` → `df`
  7. `add_aggregation_features(df)` → `df`
  8. `encode_categorical_features(df)` → `df`
  9. `write_features_parquet(df, output_dir)`
- Resolves defaults from `config.py` when arguments are `None`.
- `output_dir` defaults to `config.FEATURES_DIR`.
- Logs total elapsed time at INFO level.

**Error handling:**
- Re-raises `FileNotFoundError` from `load_processed_parquet` with context.
- Any unhandled exception is logged at CRITICAL level before re-raising.

**Dependencies:** `pathlib`, `logging`, `kgs_pipeline.config`, `kgs_pipeline.process`

**Test cases:**
- `@pytest.mark.unit` — Patch each sub-function; assert they are called in the correct order
  with correct arguments.
- `@pytest.mark.unit` — Patch `load_processed_parquet` to raise `FileNotFoundError`; assert
  `run_features_pipeline` re-raises `FileNotFoundError`.
- `@pytest.mark.unit` — Assert `None` arguments resolve to config defaults without raising.
- `@pytest.mark.integration` — Run `run_features_pipeline()` end-to-end on real processed data;
  assert `data/processed/features/` is non-empty and all Parquet files are readable.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 33: Implement main pipeline orchestration script

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `run_pipeline(stages: list[str] | None = None) -> None`

**Description:**
- Top-level orchestrator that runs all pipeline stages in sequence:
  1. `run_acquire_pipeline()` (from `kgs_pipeline.acquire`)
  2. `run_ingest_pipeline()` (from `kgs_pipeline.ingest`)
  3. `run_clean_pipeline()` (from `kgs_pipeline.clean`)
  4. `run_features_pipeline()` (from `kgs_pipeline.process`)
- The optional `stages` argument accepts a list of stage names to run selectively, e.g.,
  `["acquire", "ingest"]`. If `None`, all stages are run.
- Valid stage names are `"acquire"`, `"ingest"`, `"clean"`, `"features"`.
- Log start/end of each stage and overall wall-clock time at INFO level.
- If `__name__ == "__main__"`, parse a `--stages` CLI argument using `argparse` (comma-separated
  stage names) and call `run_pipeline(stages)`.

**Error handling:**
- Invalid stage names in `stages` raise `ValueError` listing the unknown names.
- Any stage failure is logged at CRITICAL level; the pipeline halts and re-raises.

**Dependencies:** `argparse`, `logging`, `kgs_pipeline.acquire`, `kgs_pipeline.ingest`,
  `kgs_pipeline.clean`, `kgs_pipeline.process`, `kgs_pipeline.config`

**Test cases:**
- `@pytest.mark.unit` — Patch all four stage entry-points; assert `run_pipeline()` calls all
  four in order when `stages=None`.
- `@pytest.mark.unit` — Patch all four stage entry-points; assert `run_pipeline(["acquire",
  "ingest"])` calls only `run_acquire_pipeline` and `run_ingest_pipeline`.
- `@pytest.mark.unit` — Assert `run_pipeline(["invalid_stage"])` raises `ValueError`.
- `@pytest.mark.unit` — Patch `run_acquire_pipeline` to raise `RuntimeError`; assert
  `run_pipeline()` re-raises `RuntimeError` and logs at CRITICAL.
- `@pytest.mark.integration` — Run `run_pipeline()` end-to-end; assert all four output
  directories contain data.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Domain-Specific Test Cases for `tests/test_process.py`

The following domain-specific tests must be implemented in `tests/test_process.py`.

### Decline curve monotonicity
- `@pytest.mark.unit` — For a synthetic 12-month well with known production values, assert
  `cumulative_production` is monotonically non-decreasing (i.e., each value ≥ the previous).
- `@pytest.mark.integration` — For a sample of 20 real wells, assert `cumulative_production`
  never decreases over time.

### Well completeness
- `@pytest.mark.integration` — For a sample of 10 wells, assert the number of feature rows
  equals the expected monthly span from `min(production_date)` to `max(production_date)`.

### Unit consistency
- `@pytest.mark.unit` — Assert `gor` is always non-negative (GOR cannot be negative by physics)
  or `NaN`.
- `@pytest.mark.unit` — Assert `rolling_mean_3m` and `rolling_mean_6m` are non-negative for
  oil wells (production volumes cannot be negative).

### Lazy evaluation
- `@pytest.mark.unit` — Assert every transformation function in `process.py`
  (`add_time_features`, `add_cumulative_production`, `add_decline_rate`,
  `add_rolling_statistics`, `add_gor_wor_features`, `add_aggregation_features`,
  `encode_categorical_features`) returns a `dask.dataframe.DataFrame` and not a
  `pandas.DataFrame`.

### Partition correctness
- `@pytest.mark.unit` — Assert each features Parquet partition file contains rows for exactly
  one unique `well_id` value.

### Schema stability
- `@pytest.mark.integration` — Sample 5 randomly chosen well partition files from
  `data/processed/features/`; assert all 5 schemas are identical in column names and dtypes.

### Parquet readability
- `@pytest.mark.integration` — Assert every Parquet file in `data/processed/features/` is
  readable by `pandas.read_parquet()` without raising an exception.

### Label encoding consistency
- `@pytest.mark.unit` — Assert that the same county name maps to the same `county_encoded`
  integer across two independently sampled partitions.

### Row count reconciliation
- `@pytest.mark.unit` — Assert the number of rows in the features output equals the number of
  rows in the processed input (feature engineering adds columns, not rows — GOR computation
  must not duplicate rows).
