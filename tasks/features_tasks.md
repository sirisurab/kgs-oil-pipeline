# Features Component — Task Specifications

## Overview
The features component is responsible for:
1. Reading the processed well-level Parquet data from `kgs/data/processed/` (produced by transform).
2. Engineering domain-specific production features per well, including cumulative production, decline rates, production ratios, and rolling statistics.
3. Encoding categorical metadata features for ML readiness.
4. Assembling and writing the final ML-ready feature dataset to `kgs/data/processed/features/` as Parquet files partitioned by `well_id`.
5. Keeping all operations lazy (no `.compute()` inside feature functions) — computation is triggered only in `write_features_parquet()`.

**Source module:** `kgs_pipeline/features.py`  
**Test file:** `tests/test_features.py`

---

## Design Decisions & Constraints
- Python 3.11+, Dask, Pandas, NumPy, PyArrow.
- All feature functions receive and return lazy Dask DataFrames — `.compute()` must **not** be called inside any feature function.
- Features are computed separately for oil (`product == "O"`) and gas (`product == "G"`) records, then reassembled.
- Cumulative production (Np for oil, Gp for gas) is computed per `well_id` in chronological order of `production_date`. Because Dask requires records for a single `well_id` to be within the same partition (established by `sort_by_well_and_date()` in transform), cumulative sums are computed via `map_partitions`.
- Rolling statistics use a 3-month and 6-month window per `well_id` — computed within partitions using `pandas.DataFrame.groupby(...).rolling(...)`.
- Decline rate is defined as the month-over-month percentage change in production: `(production[t] - production[t-1]) / production[t-1]`. Computed per `well_id` via `pct_change()` on the sorted time series.
- GOR (Gas-Oil Ratio) = gas production / oil production — requires pivoting oil and gas records onto the same row per `well_id` and `production_date`. Computed as a wide-format feature after a pivot step.
- Water cut and WOR are not directly computable from KGS data (no water production column) — these columns are added as `NaN` placeholders with a column comment in the schema for future use.
- Time since first production (`months_since_first_prod`) is an integer column: the number of months elapsed from the well's first `production_date` to the current record's `production_date`.
- Categorical metadata columns (`county`, `operator`, `producing_zone`, `field_name`) are label-encoded using a consistent mapping derived from the full dataset (not per-partition), to avoid inconsistent encoding across partitions.
- The final feature DataFrame must include all columns necessary for ML: production features, metadata features, and well identity (`well_id`, `lease_kid`, `production_date`).
- Output Parquet is written to `kgs/data/processed/features/`, partitioned by `well_id`.
- All configuration paths come from `kgs_pipeline/config.py`. Add `FEATURES_DIR: Path = PROCESSED_DATA_DIR / "features"` to `config.py` as part of this component.

---

## Task 22: Add `FEATURES_DIR` to `config.py`

**Module:** `kgs_pipeline/config.py`  
**Modification:** Add one constant

**Description:**  
Add the following constant to `kgs_pipeline/config.py`:
- `FEATURES_DIR: Path` — `PROCESSED_DATA_DIR / "features"`

This path points to the directory where the final ML-ready feature Parquet files will be written.

**Test cases:**
- `@pytest.mark.unit` — Import `FEATURES_DIR` from `kgs_pipeline.config` and assert it is a `pathlib.Path` instance.
- `@pytest.mark.unit` — Assert `FEATURES_DIR` is a subdirectory of `PROCESSED_DATA_DIR`.

**Definition of done:** Constant added to `config.py`, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 23: Implement `load_processed_data()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def load_processed_data(processed_dir: Path = PROCESSED_DATA_DIR) -> dask.dataframe.DataFrame`

**Description:**  
Read the processed Parquet store into a lazy Dask DataFrame.

Steps:
1. Use `dask.dataframe.read_parquet(str(processed_dir), engine="pyarrow")`.
2. If `processed_dir` does not exist or contains no Parquet files, raise `FileNotFoundError` with a descriptive message.
3. Verify that mandatory columns are present: `well_id`, `production_date`, `production`, `product`, `unit`. If any are missing, raise `KeyError` listing the missing columns.
4. Return the lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `processed_dir` does not exist, raise `FileNotFoundError`.
- If any mandatory column is missing, raise `KeyError`.

**Test cases:**
- `@pytest.mark.unit` — Write a small Parquet file with required columns to a temp directory; assert the function returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a non-existent directory, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a Parquet file missing `well_id`, assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — Given the real `kgs/data/processed/` (requires transform to have run), assert the loaded Dask DataFrame has the expected columns and at least one partition.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 24: Implement `compute_cumulative_production()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def compute_cumulative_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute cumulative production per well (`Np` for oil, `Gp` for gas) as a new column `cumulative_production`.

**Domain context:** Cumulative production (Np/Gp) is a fundamental decline curve analysis quantity. It must be monotonically non-decreasing per well over time — a well cannot un-produce oil. This is also used to verify sort correctness.

Steps:
1. Use `ddf.map_partitions()` to apply a per-partition function that:
   a. Groups by `["well_id", "product"]`.
   b. Within each group, sorts by `production_date`.
   c. Computes the cumulative sum of `production` (treating `NaN` as 0 for cumulative purposes using `fillna(0)` before cumsum, then restoring `NaN` where original was `NaN`).
   d. Assigns the result to a new column `cumulative_production`.
2. This works correctly because `sort_by_well_and_date()` in transform has already placed all records for a given `well_id` in the same Dask partition.
3. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Domain-specific test cases:**
- `@pytest.mark.unit` `@pytest.mark.domain` — **Decline curve monotonicity**: Given a well with 3 monthly production values `[100, 80, 60]`, assert `cumulative_production` is `[100, 180, 240]` (non-decreasing) after `.compute()`.
- `@pytest.mark.unit` `@pytest.mark.domain` — Given a row with `production = NaN`, assert `cumulative_production` is not decremented (NaN does not reduce the cumulative sum) and the cumulative value is carried forward from the previous row.
- `@pytest.mark.unit` `@pytest.mark.domain` — Given a well with a `production = 0.0` record, assert `cumulative_production` does not decrease (zero is not negative production).
- `@pytest.mark.unit` — Assert the `cumulative_production` column is present in the output DataFrame.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` `@pytest.mark.domain` — For a sample of 10 wells in the real processed data, assert that `cumulative_production` is monotonically non-decreasing across all monthly records sorted by `production_date`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 25: Implement `compute_decline_rate()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def compute_decline_rate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute the month-over-month production decline rate per well as a new column `decline_rate`.

**Domain context:** Decline rate is one of the most important features in production forecasting. Exponential decline is the standard model for conventional oil wells in the Mid-Continent (Kansas included). A positive decline rate means production increased that month; a negative rate means it decreased.

**Formula:** `decline_rate[t] = (production[t] - production[t-1]) / production[t-1]`  
This is equivalent to `pandas.Series.pct_change()`.

Steps:
1. Use `ddf.map_partitions()` to apply a per-partition function that:
   a. Groups by `["well_id", "product"]`.
   b. Within each group (already sorted by `production_date`), calls `pct_change()` on `production`.
   c. Assigns the result to `decline_rate`.
   d. Clips `decline_rate` to the range `[-1.0, 10.0]` to avoid extreme values from near-zero denominators (a well going from 0.001 BBL to 1 BBL is a 99,900% increase — not meaningful for ML).
2. The first record for each well will have `decline_rate = NaN` (no prior period) — this is correct and expected.
3. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `production` column is not present, raise `KeyError("production column not found")`.
- If `well_id` or `production_date` columns are not present, raise `KeyError` with the missing column name.

**Test cases:**
- `@pytest.mark.unit` — Given a well with production `[100.0, 80.0, 40.0]`, assert `decline_rate` is `[NaN, -0.2, -0.5]` after `.compute()`.
- `@pytest.mark.unit` — Assert the first record per well always has `decline_rate == NaN`.
- `@pytest.mark.unit` — Given `production = [0.0, 1.0]`, assert `decline_rate` for the second row is clipped to `10.0` (not infinity).
- `@pytest.mark.unit` — Assert the `decline_rate` column is present in the output DataFrame.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `production`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 26: Implement `compute_rolling_statistics()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def compute_rolling_statistics(ddf: dask.dataframe.DataFrame, windows: list[int] = [3, 6]) -> dask.dataframe.DataFrame`

**Description:**  
Compute rolling mean and rolling standard deviation of `production` per well over specified month windows. These serve as smoothed production trend features for ML.

Steps:
1. Use `ddf.map_partitions()` to apply a per-partition function that:
   a. Groups by `["well_id", "product"]`.
   b. For each window size `w` in `windows`:
      - Computes `rolling_mean_{w}m = production.rolling(window=w, min_periods=1).mean()` per group.
      - Computes `rolling_std_{w}m = production.rolling(window=w, min_periods=1).std()` per group.
   c. Assigns all rolling columns to the DataFrame.
2. Column names produced: `rolling_mean_3m`, `rolling_std_3m`, `rolling_mean_6m`, `rolling_std_6m` (for default `windows=[3, 6]`).
3. `min_periods=1` ensures records at the start of a well's life (fewer than `w` prior records) still receive a value rather than `NaN`.
4. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `production` column is not present, raise `KeyError("production column not found")`.
- If any value in `windows` is less than 1, raise `ValueError("Window size must be >= 1")`.

**Test cases:**
- `@pytest.mark.unit` — Given a well with 6 monthly production values, assert `rolling_mean_3m` for month 3 equals the mean of months 1, 2, 3.
- `@pytest.mark.unit` — Assert `rolling_mean_3m` for the first record of a well equals the production value itself (min_periods=1).
- `@pytest.mark.unit` — Assert that `rolling_mean_3m`, `rolling_std_3m`, `rolling_mean_6m`, `rolling_std_6m` columns are all present in the output DataFrame.
- `@pytest.mark.unit` — Given `windows=[3]`, assert only `rolling_mean_3m` and `rolling_std_3m` columns are added (not 6m columns).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given `windows=[0]`, assert `ValueError` is raised.
- `@pytest.mark.unit` — Given a DataFrame missing `production`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 27: Implement `compute_time_features()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def compute_time_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Add time-based features to each well record that provide temporal context for ML models:

**Features to add:**
- `months_since_first_prod`: integer — months elapsed from the well's first `production_date` to the current record's `production_date`. Formula: `(year_current - year_first) * 12 + (month_current - month_first)`.
- `production_year`: integer — calendar year of `production_date`.
- `production_month`: integer — calendar month of `production_date` (1–12).

Steps:
1. Use `ddf.map_partitions()` to apply a per-partition function that:
   a. Groups by `["well_id", "product"]`.
   b. Computes `first_prod_date` as the minimum `production_date` per group.
   c. Maps `first_prod_date` back to each row and computes `months_since_first_prod`.
   d. Extracts `production_year` and `production_month` from `production_date`.
2. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `production_date` is not a `datetime64[ns]` column, raise `TypeError("production_date must be datetime64[ns]")`.
- If `well_id` or `production_date` columns are absent, raise `KeyError`.

**Test cases:**
- `@pytest.mark.unit` — Given a well with first production in January 2020 and a record in March 2020, assert `months_since_first_prod == 2`.
- `@pytest.mark.unit` — Assert the first record for a well has `months_since_first_prod == 0`.
- `@pytest.mark.unit` — Given `production_date = pandas.Timestamp("2022-07-01")`, assert `production_year == 2022` and `production_month == 7`.
- `@pytest.mark.unit` — Assert `months_since_first_prod`, `production_year`, and `production_month` columns are present in the output.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a `production_date` column with dtype `object` (string), assert `TypeError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 28: Implement `compute_gor_placeholder()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def compute_gor_placeholder(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Attempt to compute the Gas-Oil Ratio (GOR = gas production / oil production) per well per month by pivoting the oil and gas records onto the same row. Where both oil and gas records exist for the same `well_id` and `production_date`, compute GOR. Where only one product type exists, set GOR to `NaN`.

**Domain context:** GOR is a key diagnostic for reservoir drive mechanism and well health. Rising GOR can indicate gas cap expansion or solution gas drive. KGS data may have both oil and gas records per well, or only one type — this function handles both cases.

Steps:
1. Using `ddf.map_partitions()`, apply a per-partition function that:
   a. Pivots the partition on `product` with `production` as values and `["well_id", "production_date"]` as index, using `pandas.DataFrame.pivot_table(index=[...], columns="product", values="production", aggfunc="first")`.
   b. Renames pivoted columns: `"O"` → `oil_production_bbl`, `"G"` → `gas_production_mcf`. If a column is absent (well only has one product type), fill with `NaN`.
   c. Computes `gor = gas_production_mcf / oil_production_bbl` where `oil_production_bbl > 0`, else `NaN`.
   d. Adds a `water_cut` column set to `NaN` (placeholder — KGS data has no water production).
   e. Adds a `wor` column set to `NaN` (placeholder — KGS data has no water production).
   f. Merges these back onto the original partition rows via `well_id` and `production_date`.
2. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `well_id`, `production_date`, `product`, or `production` columns are absent, raise `KeyError` listing the missing column.

**Test cases:**
- `@pytest.mark.unit` — Given a partition with an oil record (100 BBL) and gas record (500 MCF) for the same `well_id` and `production_date`, assert `gor == 5.0`.
- `@pytest.mark.unit` — Given a partition with only an oil record (no gas), assert `gor == NaN`.
- `@pytest.mark.unit` — Given `oil_production_bbl = 0.0`, assert `gor == NaN` (no division by zero).
- `@pytest.mark.unit` — Assert `water_cut` and `wor` columns are present and all `NaN`.
- `@pytest.mark.unit` — Assert `gor`, `oil_production_bbl`, and `gas_production_mcf` columns are present in the output.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 29: Implement `encode_categorical_features()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def encode_categorical_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Label-encode categorical metadata columns to produce integer-coded features suitable for ML models. The encoding must be globally consistent across all partitions (not per-partition), so a global category mapping must be computed first.

**Columns to encode:**
- `county` → `county_encoded`
- `operator` → `operator_encoded`
- `producing_zone` → `producing_zone_encoded`
- `field_name` → `field_name_encoded`
- `product` → `product_encoded` (`"O"` → `0`, `"G"` → `1`)

Steps:
1. Compute the unique values for each categorical column globally by calling `.compute()` on a minimal projected Dask DataFrame (select only the four categorical columns, drop duplicates). This is the **only** place `.compute()` is permitted in the features module, and it is explicitly scoped to building the encoding dictionary.
2. For each column, build a `dict[str, int]` mapping sorted unique values to integers (0-indexed, sorted alphabetically for determinism).
3. Apply the encoding via `ddf.map_partitions()` using `pandas.Series.map(encoding_dict)` for each column. Unknown values (not seen during encoding) map to `-1`.
4. Add `_encoded` suffix columns alongside the original string columns (do not drop originals — both human-readable and encoded versions are needed for ML).
5. Return the updated lazy Dask DataFrame — do **not** call `.compute()` again after the initial encoding computation.

**Error handling:**
- If any of the four categorical columns is absent, raise `KeyError` listing the missing column.

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with `county` values `["Allen", "Butler", "Allen"]`, assert `county_encoded` values are `[0, 1, 0]` (alphabetical encoding).
- `@pytest.mark.unit` — Given an unseen `county` value in a partition not present during encoding, assert `county_encoded == -1`.
- `@pytest.mark.unit` — Assert `product_encoded` is `0` for `"O"` and `1` for `"G"`.
- `@pytest.mark.unit` — Assert original string columns (`county`, `operator`, etc.) are still present in the output.
- `@pytest.mark.unit` — Assert that encoding is deterministic: running `encode_categorical_features()` twice on the same Dask DataFrame produces identical `_encoded` values.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `county`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 30: Implement `write_features_parquet()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def write_features_parquet(ddf: dask.dataframe.DataFrame, features_dir: Path = FEATURES_DIR) -> None`

**Description:**  
Persist the ML-ready feature DataFrame to the features Parquet store, partitioned by `well_id`.

Steps:
1. Create `features_dir` if it does not exist (`features_dir.mkdir(parents=True, exist_ok=True)`).
2. Call `ddf.to_parquet(str(features_dir), partition_on=["well_id"], write_index=False, overwrite=True, engine="pyarrow")`.
3. Log the target directory and confirm completion.

**Error handling:**
- If `ddf` is not a `dask.dataframe.DataFrame`, raise `TypeError("Expected a dask DataFrame")`.
- If `well_id` is not in `ddf.columns`, raise `KeyError("well_id column required for partitioning")`.
- Wrap `to_parquet` in try/except for `OSError`; re-raise with a descriptive message.

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with `well_id`, assert the function creates `well_id=<value>/` subdirectories in the features directory.
- `@pytest.mark.unit` — Given a pandas DataFrame (not Dask), assert `TypeError` is raised.
- `@pytest.mark.unit` — Assert written Parquet files are readable by `pandas.read_parquet()` (Parquet readability check).
- `@pytest.mark.unit` — Assert each written partition file contains rows for exactly one unique `well_id` (partition correctness).
- `@pytest.mark.integration` — Given the real feature Dask DataFrame, assert files are written to `kgs/data/processed/features/` and schema is consistent across all partitions (schema stability check: sample two partition schemas and assert equality).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 31: Implement `run_features_pipeline()`

**Module:** `kgs_pipeline/features.py`  
**Function:** `def run_features_pipeline(processed_dir: Path = PROCESSED_DATA_DIR, features_dir: Path = FEATURES_DIR) -> None`

**Description:**  
Synchronous orchestrator that chains all feature engineering steps into a single callable pipeline entry point.

Steps (in order):
1. `load_processed_data(processed_dir)` → `ddf`
2. `compute_cumulative_production(ddf)` → `ddf`
3. `compute_decline_rate(ddf)` → `ddf`
4. `compute_rolling_statistics(ddf, windows=[3, 6])` → `ddf`
5. `compute_time_features(ddf)` → `ddf`
6. `compute_gor_placeholder(ddf)` → `ddf`
7. `encode_categorical_features(ddf)` → `ddf` ← only `.compute()` call (for encoding map)
8. `write_features_parquet(ddf, features_dir)` ← triggers full Dask graph computation

Log each step with timing using `time.perf_counter()`.

**Domain-specific test cases:**
- `@pytest.mark.integration` `@pytest.mark.domain` — **Decline curve monotonicity**: For a sample of 20 wells in the feature output, assert `cumulative_production` is monotonically non-decreasing over `production_date`.
- `@pytest.mark.integration` `@pytest.mark.domain` — **Well completeness**: For a sample of wells, assert the `months_since_first_prod` for the last record equals the expected span between first and last `production_date`.
- `@pytest.mark.integration` `@pytest.mark.domain` — **GOR non-negative**: For all rows in the feature output where `gor` is not `NaN`, assert `gor >= 0` (GOR cannot be negative by physical law).
- `@pytest.mark.integration` `@pytest.mark.domain` — **Unit consistency in features**: Assert that `oil_production_bbl` column values (from GOR pivot) match the `production` values for `product == "O"` rows for the same `well_id` and `production_date`.
- `@pytest.mark.integration` — **Partition correctness**: For each Parquet file in `kgs/data/processed/features/`, assert `well_id` has exactly one unique value.
- `@pytest.mark.integration` — **Parquet readability**: For every Parquet file in `kgs/data/processed/features/`, assert `pandas.read_parquet(file)` succeeds without error.
- `@pytest.mark.integration` — **Schema stability**: Load two feature Parquet partitions for different wells and assert PyArrow schemas are identical.
- `@pytest.mark.integration` — **Lazy Dask evaluation check**: Assert that all feature functions (except `encode_categorical_features` for the encoding step, and `write_features_parquet`) return `dask.dataframe.DataFrame` and not `pandas.DataFrame`.

**Orchestrator test cases:**
- `@pytest.mark.unit` — Mock all sub-functions; assert each is called exactly once in the correct order.
- `@pytest.mark.unit` — If `load_processed_data` raises `FileNotFoundError`, assert it propagates from `run_features_pipeline`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.
