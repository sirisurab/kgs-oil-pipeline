# Features Component — Task Specifications

## Overview

The features component is responsible for:
1. Loading the processed, well-level Parquet store from `data/processed/`.
2. Computing per-well cumulative production (Np for oil, Gp for gas) as a running total sorted chronologically.
3. Computing month-over-month production decline rate per well.
4. Computing rolling mean and standard deviation at 3-month and 6-month windows per well.
5. Deriving time-based features: `months_since_first_prod`, `production_year`, `production_month`.
6. Computing gas-oil ratio (GOR) by pivoting oil and gas production for the same well-month.
7. Adding placeholder columns for `water_cut` and `wor` (water-oil ratio) as `NaN`, reserved for future integration when water production data becomes available.
8. Label-encoding categorical columns (`county`, `operator`, `producing_zone`, `field_name`, `product`) with globally consistent integer codes across all Dask partitions.
9. Writing the ML-ready feature set to `data/processed/features/` as Parquet files partitioned by `well_id`.

**Source module:** `kgs_pipeline/features.py`
**Test file:** `tests/test_features.py`

---

## Design Decisions & Constraints

- All functions in `features.py` must return `dask.dataframe.DataFrame` (lazy). The only place `.compute()` is called is inside `write_features_parquet()`, triggered by Dask's internal `to_parquet()` machinery.
- Per-well operations (cumulative sum, rolling windows, decline rate, time features) require all records for a well to reside in the same Dask partition. The processed data from the transform step is already partitioned and indexed by `well_id` — this must be verified at the start of `run_features_pipeline()`.
- Cumulative production: computed using `pandas.DataFrame.groupby("well_id")["production"].cumsum()` inside `map_partitions`. Since each partition contains exactly one well's records (after sort-and-repartition), this is correct and does not require a cross-partition merge.
- Decline rate definition: `(production[t] - production[t-1]) / production[t-1]`. Clipped to the range `[-1.0, 10.0]`. When `production[t-1]` is `0` or `NaN`, decline rate is `NaN`.
- Rolling windows: use `pandas.Series.rolling(window=N, min_periods=1).mean()` and `.std()` within `map_partitions`. `min_periods=1` ensures wells with fewer than N records still get a value (no unnecessary NaN introduction).
- Time features: `months_since_first_prod` is an integer computed as the number of full months between `production_date` and the earliest `production_date` for that well. Use `(production_date.dt.year - first_prod_year) * 12 + (production_date.dt.month - first_prod_month)`.
- GOR (gas-oil ratio): For each `(well_id, production_date)` pair, pivot on `product` to get oil and gas volumes side by side, then compute `GOR = gas_production / oil_production`. Where oil production is `0` or `NaN`, GOR is `NaN`. Rows for gas-only wells have `GOR = NaN`.
- Label encoding: each categorical column is encoded with a globally consistent mapping. The mapping is computed by calling `.compute()` once per column to collect all unique values, building a `{value: integer_code}` dict, then applying it across all partitions via `map_partitions`. This single pre-computation is the only permitted non-write `.compute()` call in this component.
- Column naming convention for features: `cumulative_production`, `decline_rate`, `rolling_mean_3m`, `rolling_std_3m`, `rolling_mean_6m`, `rolling_std_6m`, `months_since_first_prod`, `production_year`, `production_month`, `gor`, `water_cut`, `wor`, and `<col>_encoded` for each label-encoded categorical.
- Output Parquet in `data/processed/features/` is partitioned by `well_id`. Schema enforcement via `pyarrow.schema()` at write time is mandatory.
- All logging must use the Python `logging` module. No `print()` statements.
- Configuration values must come from `kgs_pipeline/config.py`. Add `FEATURES_DATA_DIR` and `DECLINE_RATE_CLIP_MIN / DECLINE_RATE_CLIP_MAX` to config.

---

## Task 22: Extend configuration for features stage

**Module:** `kgs_pipeline/config.py`
**Function:** N/A — add constants

**Description:**
Add the following constants to `kgs_pipeline/config.py` for use by the features component:

- `FEATURES_DATA_DIR` — `Path` pointing to `data/processed/features/` (may already exist from Task 01; ensure it is present).
- `DECLINE_RATE_CLIP_MIN` — float `-1.0`
- `DECLINE_RATE_CLIP_MAX` — float `10.0`
- `ROLLING_WINDOW_SHORT` — integer `3` (months)
- `ROLLING_WINDOW_LONG` — integer `6` (months)
- `CATEGORICAL_COLUMNS` — list of strings `["county", "operator", "producing_zone", "field_name", "product"]`

**Error handling:** N/A.

**Dependencies:** None beyond what already exists in `config.py`.

**Test cases:**

- `@pytest.mark.unit` — Assert `DECLINE_RATE_CLIP_MIN` is `-1.0` and `DECLINE_RATE_CLIP_MAX` is `10.0`.
- `@pytest.mark.unit` — Assert `ROLLING_WINDOW_SHORT` is `3` and `ROLLING_WINDOW_LONG` is `6`.
- `@pytest.mark.unit` — Assert `CATEGORICAL_COLUMNS` is a list containing `"county"` and `"operator"`.
- `@pytest.mark.unit` — Assert `FEATURES_DATA_DIR` is a `pathlib.Path`.

**Definition of done:** Constants are added, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 23: Implement cumulative production feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_cumulative_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add a `cumulative_production` column representing the running total of `production` for each well over time (Np for oil, Gp for gas). This is the fundamental production depletion metric used in decline curve analysis.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Sorts rows within the partition by `["well_id", "production_date"]` in ascending order (records are already sorted, but enforce it for safety).
   - Fills `NaN` production values with `0.0` temporarily for the cumsum only (so the running total does not propagate NaN across months), then restores NaN in the original `production` column.
   - Computes `cumulative_production = production.fillna(0.0).cumsum()` within the partition (since each partition is exactly one well's data).
2. Assigns `cumulative_production` as a new `float64` column.
3. Returns the Dask DataFrame with the new column appended.

The function must not call `.compute()`.

**Error handling:**
- If `production` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a single well with monthly production `[100.0, 200.0, 150.0]`. Assert `cumulative_production` is `[100.0, 300.0, 450.0]`.
- `@pytest.mark.unit` — Provide a well with a `NaN` month: `[100.0, NaN, 150.0]`. Assert `cumulative_production` is `[100.0, 100.0, 250.0]` (NaN treated as zero in cumsum, original `production` column retains `NaN`).
- `@pytest.mark.unit` — Provide a well with all zeros. Assert `cumulative_production` is `[0.0, 0.0, 0.0]`.
- `@pytest.mark.unit` — Assert the `cumulative_production` column exists and is `float64` dtype.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Decline curve monotonicity test (domain rule):**
- `@pytest.mark.integration` — Load processed Parquet from `data/processed/` (or features Parquet after this step). For a sample of 50 wells, assert `cumulative_production` is monotonically non-decreasing over time. A well cannot un-produce oil — any decrease is a pipeline error.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 24: Implement production decline rate feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_decline_rate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add a `decline_rate` column representing the month-over-month proportional change in production for each well. Decline rate is the primary indicator of reservoir depletion behaviour and is a key input for decline curve analysis ML models.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Sorts rows by `production_date` (ascending).
   - Computes `decline_rate = (production - production.shift(1)) / production.shift(1)`.
   - Where `production.shift(1)` is `0.0` or `NaN`, sets `decline_rate = NaN`.
   - Clips `decline_rate` to `[DECLINE_RATE_CLIP_MIN, DECLINE_RATE_CLIP_MAX]` using `pandas.Series.clip()`.
2. Assigns `decline_rate` as a new `float64` column.
3. The first record for each well always has `decline_rate = NaN` (no previous month).

The function must not call `.compute()`.

**Error handling:**
- If `production` or `production_date` columns are absent, raise `KeyError`.
- `DECLINE_RATE_CLIP_MIN` and `DECLINE_RATE_CLIP_MAX` must come from `kgs_pipeline/config.py`.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide production `[100.0, 80.0, 60.0]`. Assert `decline_rate` is `[NaN, -0.2, -0.25]`.
- `@pytest.mark.unit` — Provide production `[0.0, 50.0]`. Assert `decline_rate[1]` is `NaN` (division by zero → NaN).
- `@pytest.mark.unit` — Provide production `[100.0, NaN, 80.0]`. Assert `decline_rate[2]` is `NaN` (prior month is NaN → NaN).
- `@pytest.mark.unit` — Provide a case where the raw decline rate would be `15.0` (greater than clip max). Assert `decline_rate` is clipped to `10.0`.
- `@pytest.mark.unit` — Provide a case where the raw decline rate would be `-5.0` (less than clip min). Assert `decline_rate` is clipped to `-1.0`.
- `@pytest.mark.unit` — Assert the first row for each well has `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 25: Implement rolling statistics features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_rolling_statistics(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add rolling mean and rolling standard deviation features at two window sizes (3-month and 6-month) to capture short-term and medium-term production trends. These are standard smoothing features for time-series ML models.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Sorts rows by `production_date` ascending.
   - Computes `rolling_mean_3m = production.rolling(window=3, min_periods=1).mean()`.
   - Computes `rolling_std_3m = production.rolling(window=3, min_periods=1).std()`.
   - Computes `rolling_mean_6m = production.rolling(window=6, min_periods=1).mean()`.
   - Computes `rolling_std_6m = production.rolling(window=6, min_periods=1).std()`.
2. Assigns all four columns as `float64`.
3. Returns the Dask DataFrame with the four new columns appended.

Use `ROLLING_WINDOW_SHORT` and `ROLLING_WINDOW_LONG` from `kgs_pipeline/config.py` for window sizes.

The function must not call `.compute()`.

**Error handling:**
- If `production` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide production `[10.0, 20.0, 30.0, 40.0, 50.0, 60.0]`. Assert `rolling_mean_3m[2]` equals `20.0` (mean of 10, 20, 30).
- `@pytest.mark.unit` — Provide production `[10.0, 20.0, 30.0, 40.0, 50.0, 60.0]`. Assert `rolling_mean_6m[5]` equals `35.0` (mean of all 6 values).
- `@pytest.mark.unit` — Provide a well with only 2 records. Assert `rolling_mean_3m` is not `NaN` for either record (due to `min_periods=1`).
- `@pytest.mark.unit` — Provide a well with a `NaN` production month. Assert rolling calculations treat it as a gap and do not propagate error.
- `@pytest.mark.unit` — Assert all four rolling columns exist and are `float64` dtype.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 26: Implement time-based features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_time_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Derive three time-based features that encode well age and temporal position for ML models:

- `months_since_first_prod` (integer): number of full calendar months from the first production date for the well to the current row's production date. A well's first production month has `months_since_first_prod = 0`.
- `production_year` (integer): calendar year extracted from `production_date` (e.g. `2021`).
- `production_month` (integer): calendar month extracted from `production_date` (1–12).

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Determines `first_prod_date = production_date.min()` within the partition (valid because each partition is exactly one well).
   - Computes `months_since_first_prod = (production_date.dt.year - first_prod_date.year) * 12 + (production_date.dt.month - first_prod_date.month)`.
   - Extracts `production_year = production_date.dt.year`.
   - Extracts `production_month = production_date.dt.month`.
2. Assigns all three as integer (`int32`) columns.

The function must not call `.compute()`.

**Error handling:**
- If `production_date` column is absent, raise `KeyError`.
- Rows with `NaT` `production_date` get `months_since_first_prod = NaN`, `production_year = NaN`, `production_month = NaN`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide 3 monthly records starting from `2021-01-01`. Assert `months_since_first_prod` is `[0, 1, 2]`.
- `@pytest.mark.unit` — Provide records from `2021-11-01` and `2022-02-01`. Assert `months_since_first_prod` is `[0, 3]`.
- `@pytest.mark.unit` — Assert `production_year` and `production_month` match the year and month of `production_date` for a known row.
- `@pytest.mark.unit` — Provide a row with `NaT` `production_date`. Assert `months_since_first_prod` is `NaN`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 27: Implement gas-oil ratio (GOR) feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_gor(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the gas-oil ratio (GOR) for each `(well_id, production_date)` pair. GOR = gas production (MCF) / oil production (BBL) for the same well and month. This ratio is a critical reservoir characterisation metric used in production engineering and ML feature sets.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Pivots the partition on `product` to produce `oil_production` and `gas_production` side-by-side columns for each `(well_id, production_date)` pair, using `pandas.DataFrame.pivot_table(index=["well_id", "production_date"], columns="product", values="production", aggfunc="first")`.
   - Renames the pivoted columns to `oil_production` (from `"O"`) and `gas_production` (from `"G"`). If either column is absent (e.g. oil-only well), fill it with `NaN`.
   - Computes `gor = gas_production / oil_production`. Where `oil_production == 0.0` or is `NaN`, set `gor = NaN`.
   - Merges the `gor` column back into the original partition on `(well_id, production_date)`.
2. Returns the Dask DataFrame with the `gor` column appended (`float64`, nullable).

Wells with only oil records have `gor = NaN`. Wells with only gas records have `gor = NaN`.

The function must not call `.compute()`.

**Error handling:**
- If `product`, `production`, `well_id`, or `production_date` columns are absent, raise `KeyError`.
- Division-by-zero (`oil_production == 0`) must be handled explicitly: result is `NaN`, not `inf`.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a well with `production_date = 2021-03-01`, oil `= 1000.0 BBL`, gas `= 500.0 MCF`. Assert `gor = 0.5`.
- `@pytest.mark.unit` — Provide a well-month with `oil_production = 0.0`. Assert `gor = NaN` (not `inf`).
- `@pytest.mark.unit` — Provide a gas-only well. Assert `gor = NaN` for all rows.
- `@pytest.mark.unit` — Provide an oil-only well. Assert `gor = NaN` for all rows (no gas data to divide).
- `@pytest.mark.unit` — Assert `gor` column is `float64` dtype.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 28: Implement water cut and WOR placeholder columns

**Module:** `kgs_pipeline/features.py`
**Function:** `add_water_placeholders(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add `water_cut` and `wor` (water-oil ratio) columns as `NaN` float64 placeholders. The KGS dataset does not include water production volumes; these columns are reserved for future integration when water production data becomes available from additional data sources. Including them now ensures a stable, forward-compatible schema.

Processing steps:
1. Add column `water_cut` of dtype `float64`, all values `NaN`, with the docstring note: "Reserved: water production data not available in KGS monthly records."
2. Add column `wor` of dtype `float64`, all values `NaN`, with the same note.
3. Return the Dask DataFrame with both columns appended.

The function must not call `.compute()`.

**Error handling:** None — this is a schema extension only.

**Dependencies:** `dask.dataframe`, `numpy`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Assert `water_cut` and `wor` columns exist in the output DataFrame.
- `@pytest.mark.unit` — Assert all values in `water_cut` and `wor` are `NaN` (not zero).
- `@pytest.mark.unit` — Assert `water_cut` and `wor` are `float64` dtype.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Domain test:**
- `@pytest.mark.unit` — Assert `water_cut` values, when non-null, would need to satisfy `0 <= water_cut <= 1`. Since all values are currently `NaN`, assert no value violates this bound (vacuously true, but the test documents the constraint for future implementation).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 29: Implement categorical label encoding

**Module:** `kgs_pipeline/features.py`
**Function:** `encode_categorical_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Label-encode the categorical columns `["county", "operator", "producing_zone", "field_name", "product"]` with globally consistent integer codes. This is required for ML models that cannot consume string categories. The encoding must be consistent across all Dask partitions — the same string value must always receive the same integer code.

Processing steps:
1. For each column in `CATEGORICAL_COLUMNS` (from `config.py`):
   a. Compute the global set of unique values by calling `ddf[col].unique().compute()` — this is the single permitted pre-computation `.compute()` call per column.
   b. Sort the unique values alphabetically to ensure deterministic ordering.
   c. Build an encoding dict: `{value: index for index, value in enumerate(sorted_unique_values)}`. Missing/null values map to `-1`.
   d. Apply the encoding dict to the column using `dask.dataframe.map_partitions` with a pandas `.map()` call, creating a new column named `<col>_encoded` (e.g. `county_encoded`).
2. Return the Dask DataFrame with all `_encoded` columns appended. The original string columns are retained.

Log the encoding map for each column at `DEBUG` level for reproducibility auditing.

The function must not call `.compute()` except in the one permitted pre-computation per column.

**Error handling:**
- If any column in `CATEGORICAL_COLUMNS` is absent from the DataFrame, log a `WARNING` and skip that column (do not raise).

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a DataFrame with `county` values `["Allen", "Butler", "Allen", "Clark"]`. Assert `county_encoded` values map consistently: the same county name always gets the same integer, codes are 0-indexed, and `"Allen"` < `"Butler"` < `"Clark"` alphabetically.
- `@pytest.mark.unit` — Provide a row with `county = NaN`. Assert `county_encoded = -1`.
- `@pytest.mark.unit` — Assert the original `county` column is still present in the output.
- `@pytest.mark.unit` — Assert all `_encoded` columns are integer dtype.
- `@pytest.mark.unit` — Assert the encoding is deterministic: run `encode_categorical_features` twice on the same input and assert the encoded values are identical.
- `@pytest.mark.unit` — Provide a DataFrame missing the `product` column. Assert the function logs a `WARNING` for that column and returns a DataFrame without `product_encoded`, but with all other `_encoded` columns.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 30: Implement features Parquet writer with schema enforcement

**Module:** `kgs_pipeline/features.py`
**Function:** `write_features_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`

**Description:**
Write the fully engineered ML-ready feature Dask DataFrame to Parquet format, partitioned by `well_id`, with schema enforcement via PyArrow. This mirrors the pattern of `write_processed_parquet()` in the transform component but extends the schema to include all feature columns.

Processing steps:
1. Ensure `output_dir` exists.
2. Define a `pyarrow.schema()` object that explicitly types every expected column. In addition to the base processed columns, include:
   - `cumulative_production`: `pa.float64()`
   - `decline_rate`: `pa.float64()`
   - `rolling_mean_3m`: `pa.float64()`
   - `rolling_std_3m`: `pa.float64()`
   - `rolling_mean_6m`: `pa.float64()`
   - `rolling_std_6m`: `pa.float64()`
   - `months_since_first_prod`: `pa.int32()`
   - `production_year`: `pa.int32()`
   - `production_month`: `pa.int32()`
   - `gor`: `pa.float64()`
   - `water_cut`: `pa.float64()`
   - `wor`: `pa.float64()`
   - `<col>_encoded` for each categorical column: `pa.int32()`
3. Call `ddf.to_parquet(str(output_dir), engine="pyarrow", schema=enforced_schema, write_index=True, overwrite=True)`.
4. Log the output directory and number of files written at `INFO` level.
5. Return `output_dir` as a `Path`.

**Error handling:**
- If `output_dir` cannot be created, let `OSError` propagate.
- Schema mismatch errors from `to_parquet()` must be logged at `ERROR` level and re-raised.

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a minimal valid Dask DataFrame with all expected feature columns. Call `write_features_parquet(ddf, tmp_path)`. Assert the directory contains at least one `.parquet` file.
- `@pytest.mark.unit` — Assert every `.parquet` file written is readable by `pandas.read_parquet()` without error.
- `@pytest.mark.unit` — Assert the function returns a `Path` equal to `output_dir`.

**Parquet readability test:**
- `@pytest.mark.integration` — For every Parquet file in `data/processed/features/`, attempt to read it with `pandas.read_parquet()`. Assert no file raises an exception on read.

**Schema stability test:**
- `@pytest.mark.integration` — Sample the schema from any two Parquet files in `data/processed/features/` using `pyarrow.parquet.read_schema()`. Assert the two schemas are identical (same column names and dtypes).

**Partition correctness test:**
- `@pytest.mark.integration` — Read each Parquet partition from `data/processed/features/` individually. Assert each file contains exactly one unique `well_id` value.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 31: Implement features pipeline orchestrator and domain integrity tests

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features_pipeline() -> Path`

**Description:**
Synchronous entry-point that wires together the full features engineering workflow end-to-end:
1. Read config from `kgs_pipeline/config.py`.
2. Load the processed Parquet store from `PROCESSED_DATA_DIR` using `dask.dataframe.read_parquet()`.
3. Verify the Dask DataFrame index is `well_id` (each partition corresponds to one well). If not, call `sort_and_repartition()` from `kgs_pipeline/transform.py` to enforce it.
4. Call `compute_cumulative_production(ddf)`.
5. Call `compute_decline_rate(ddf)`.
6. Call `compute_rolling_statistics(ddf)`.
7. Call `compute_time_features(ddf)`.
8. Call `compute_gor(ddf)`.
9. Call `add_water_placeholders(ddf)`.
10. Call `encode_categorical_features(ddf)`.
11. Call `write_features_parquet(ddf, FEATURES_DATA_DIR)`.
12. Return `FEATURES_DATA_DIR`.

Log pipeline start, each step name, and completion with elapsed time at `INFO` level.

**Error handling:**
- If `PROCESSED_DATA_DIR` contains no Parquet files, raise `RuntimeError`.
- Any exception in steps 4–10 must propagate with full traceback.

**Dependencies:** `dask.dataframe`, `pathlib.Path`, `logging`, `time`

**Test cases:**

- `@pytest.mark.unit` — Patch all sub-functions. Assert `run_features_pipeline()` calls each step exactly once in the correct order.
- `@pytest.mark.unit` — Patch `dask.dataframe.read_parquet` to simulate empty processed store. Assert `RuntimeError` is raised.

**Domain-specific integrity tests (against features output in `data/processed/features/`):**

- `@pytest.mark.integration` — **Decline curve monotonicity**: For a sample of 50 wells, load features Parquet and assert `cumulative_production` is monotonically non-decreasing over `production_date`. Any decrease is a pipeline bug.
- `@pytest.mark.integration` — **Decline rate bounds**: Assert no `decline_rate` value in the features dataset falls outside `[DECLINE_RATE_CLIP_MIN, DECLINE_RATE_CLIP_MAX]` (excluding NaN rows).
- `@pytest.mark.integration` — **GOR non-negative**: Assert all non-null `gor` values are `>= 0`. GOR is a ratio of physical volumes and cannot be negative.
- `@pytest.mark.integration` — **Time feature consistency**: For any well, assert `months_since_first_prod` is `0` for the row where `production_date` equals the minimum `production_date` of that well.
- `@pytest.mark.integration` — **Encoding consistency**: Assert `county_encoded` is identical for all rows sharing the same `county` value across the entire features dataset (global code consistency).
- `@pytest.mark.integration` — **Lazy evaluation**: Load `data/processed/` and call each features function in sequence (except `write_features_parquet`). After each function call, assert the return type is `dask.dataframe.DataFrame`, confirming no premature `.compute()` call has occurred.
- `@pytest.mark.integration` — **Data integrity**: For 50 randomly selected `(well_id, production_date, product)` triples, assert that `production` in the features Parquet matches `production` in the processed Parquet for the same triple (features layer must not mutate base production values).

**Definition of done:** Function and all integration tests are implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
