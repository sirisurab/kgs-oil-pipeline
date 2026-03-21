# Features Component — Task Specifications
**Module:** `kgs_pipeline/features.py`  
**Test file:** `tests/test_features.py`  
**Input directory:** `kgs/data/processed/wells/`  
**Output directory:** `kgs/data/processed/features/`

---

## Design Decisions & Constraints

- All feature engineering functions consume and return `dask.dataframe.DataFrame`. `.compute()` must NOT be called inside any feature function — the full Dask graph is built lazily and materialized only at the final `to_parquet` write.
- Features are computed at the **well-month** level. Each row in the output represents one well in one calendar month.
- Rolling window statistics (3-month, 6-month, 12-month) are computed per well, sorted by `production_date`. Because Dask does not natively support `groupby().rolling()` across partition boundaries, all rolling computations use `map_partitions` on a well-partitioned DataFrame (re-partitioned by `well_id` prior to rolling). The re-partitioning ensures all months for a single well reside in one partition.
- For rolling calculations, `NaN` months (gaps filled in the transform step) are treated as missing and excluded from the rolling window using `min_periods=1`.
- Categorical columns (`county`, `field`, `producing_zone`, `operator`) are label-encoded to integer codes. The encoding mapping is saved as a JSON sidecar file alongside the output Parquet so it can be inverted during model inference.
- Output Parquet is written to `kgs/data/processed/features/` partitioned by `well_id`.
- All output column names are lowercase with underscores.
- Paths are sourced from `kgs_pipeline/config.py`.

---

## Output Schema (Feature Parquet) — adds to processed schema

All columns from the processed schema are carried through plus the following engineered features:

| Column                          | Type      | Description                                                                  |
|---------------------------------|-----------|------------------------------------------------------------------------------|
| `months_since_first_prod`       | `float64` | Integer count of months elapsed since the well's first non-null production   |
| `producing_months_count`        | `float64` | Cumulative count of months where `oil_bbl > 0` or `gas_mcf > 0` up to this row |
| `production_phase`              | `str`     | Phase label: `"early"` (≤12 months), `"mid"` (13–60 months), `"late"` (>60 months) based on `months_since_first_prod` |
| `oil_bbl_roll3`                 | `float64` | 3-month rolling mean of `oil_bbl` per well                                  |
| `oil_bbl_roll6`                 | `float64` | 6-month rolling mean of `oil_bbl` per well                                  |
| `oil_bbl_roll12`                | `float64` | 12-month rolling mean of `oil_bbl` per well                                 |
| `gas_mcf_roll3`                 | `float64` | 3-month rolling mean of `gas_mcf` per well                                  |
| `gas_mcf_roll6`                 | `float64` | 6-month rolling mean of `gas_mcf` per well                                  |
| `gas_mcf_roll12`                | `float64` | 12-month rolling mean of `gas_mcf` per well                                 |
| `oil_decline_rate_mom`          | `float64` | Month-on-month decline rate for oil: `(oil_bbl[t] - oil_bbl[t-1]) / oil_bbl[t-1]`. `NaN` if `oil_bbl[t-1]` is 0 or NaN |
| `gas_decline_rate_mom`          | `float64` | Month-on-month decline rate for gas: same formula as oil                     |
| `gor`                           | `float64` | Gas-oil ratio proxy: `gas_mcf / oil_bbl`. `NaN` if `oil_bbl` is 0 or NaN   |
| `county_encoded`                | `int32`   | Label-encoded `county`                                                       |
| `field_encoded`                 | `int32`   | Label-encoded `field`                                                        |
| `producing_zone_encoded`        | `int32`   | Label-encoded `producing_zone`                                               |
| `operator_encoded`              | `int32`   | Label-encoded `operator`                                                     |

---

## Task 20: Implement processed Parquet reader for features

**Module:** `kgs_pipeline/features.py`  
**Function:** `read_processed_parquet(processed_dir: Path) -> dask.dataframe.DataFrame`

**Description:**  
Read the processed well-partitioned Parquet dataset from the transform step as input for feature engineering.

- Use `dask.dataframe.read_parquet` to read from `processed_dir / "wells"`.
- Return the `dask.dataframe.DataFrame` without calling `.compute()`.
- Log the number of Dask partitions at INFO level.

**Error handling:**
- If the path does not exist, raise `FileNotFoundError`.
- If the path exists but contains no Parquet files, raise `ValueError("No Parquet files found in processed wells directory")`.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Parquet written to a temp directory, assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a path that exists but is empty (no `.parquet` files), assert `ValueError` is raised.
- `@pytest.mark.integration` — Given the real processed wells directory, assert the result has column `well_id` after `.compute()`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 21: Implement time-based features

**Module:** `kgs_pipeline/features.py`  
**Function:** `compute_time_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute time-based features that characterise each well's position in its production life.

Features to compute per well (sorted by `production_date`):
1. **`months_since_first_prod`**: For each row, compute the number of whole months elapsed from the well's first `production_date` (the earliest non-null `production_date` for that `well_id`) to the current row's `production_date`. Use `map_partitions` on a well-partitioned DataFrame.
2. **`producing_months_count`**: Running count of months (up to and including the current row) where `oil_bbl > 0.0` OR `gas_mcf > 0.0`. Uses cumulative sum of a boolean mask per well.
3. **`production_phase`**: Categorical label derived from `months_since_first_prod`:
   - `"early"` if `months_since_first_prod <= 12`
   - `"mid"` if `13 <= months_since_first_prod <= 60`
   - `"late"` if `months_since_first_prod > 60`

- Use `map_partitions` for all computations.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- If `production_date` or `well_id` columns are missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with first production in Jan-2020 and a row for Mar-2020, assert `months_since_first_prod = 2`.
- `@pytest.mark.unit` — Given a well with 3 months of production where month 2 is a gap (NaN), assert `producing_months_count` counts only the non-gap, non-zero months.
- `@pytest.mark.unit` — Given `months_since_first_prod = 6`, assert `production_phase = "early"`.
- `@pytest.mark.unit` — Given `months_since_first_prod = 24`, assert `production_phase = "mid"`.
- `@pytest.mark.unit` — Given `months_since_first_prod = 80`, assert `production_phase = "late"`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given two wells in the same partition, assert `months_since_first_prod` resets correctly for each well (not computed across wells).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 22: Implement rolling production statistics

**Module:** `kgs_pipeline/features.py`  
**Function:** `compute_rolling_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute rolling mean production statistics per well for 3-month, 6-month, and 12-month windows.

- Re-partition the input Dask DataFrame by `well_id` before applying rolling windows to ensure all months for a well reside in one partition.
- Within each partition, apply per-well rolling means:
  - `oil_bbl_roll3 = groupby("well_id")["oil_bbl"].transform(lambda x: x.rolling(3, min_periods=1).mean())`
  - `oil_bbl_roll6`, `oil_bbl_roll12` using window sizes 6 and 12 respectively.
  - `gas_mcf_roll3`, `gas_mcf_roll6`, `gas_mcf_roll12` using the same pattern for `gas_mcf`.
- Use `min_periods=1` so that the first few months of a well's history still produce a value rather than `NaN`.
- Sort by `production_date` within each well before applying the rolling window.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- If `oil_bbl`, `gas_mcf`, `well_id`, or `production_date` columns are missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with `oil_bbl = [100, 200, 300, 400]` over 4 months, assert `oil_bbl_roll3` for month 4 equals `(200 + 300 + 400) / 3 = 300.0`.
- `@pytest.mark.unit` — Given a well with only 2 months of data, assert `oil_bbl_roll3` for month 2 uses `min_periods=1` and returns the mean of the 2 available months.
- `@pytest.mark.unit` — Assert `oil_bbl_roll12` is `NaN` only if all 12 prior months are `NaN` (due to `min_periods=1`).
- `@pytest.mark.unit` — Given two wells in the same partition, assert rolling calculations are computed independently per well (not blended).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert all 6 rolling columns (`oil_bbl_roll3`, `oil_bbl_roll6`, `oil_bbl_roll12`, `gas_mcf_roll3`, `gas_mcf_roll6`, `gas_mcf_roll12`) are present in the output.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 23: Implement decline rate and GOR features

**Module:** `kgs_pipeline/features.py`  
**Function:** `compute_decline_and_gor(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute month-on-month production decline rates and the gas-oil ratio (GOR) proxy feature.

Features:
1. **`oil_decline_rate_mom`**: `(oil_bbl[t] - oil_bbl[t-1]) / oil_bbl[t-1]` per well, sorted by `production_date`. Set to `NaN` if `oil_bbl[t-1]` is `0.0` or `NaN`, or if `t` is the first month for the well.
2. **`gas_decline_rate_mom`**: Same calculation for `gas_mcf`.
3. **`gor`**: `gas_mcf / oil_bbl` per row. Set to `NaN` if `oil_bbl` is `0.0` or `NaN` (avoid division by zero).

Domain context:
- A negative decline rate means production increased month-on-month (workover, stimulation, or new well).
- A positive `gor` indicates associated gas production. Rising GOR over time can signal reservoir depletion.
- GOR is only meaningful when both oil and gas are produced; if only one product is reported, `gor` is `NaN`.

- Use `map_partitions` on a well-partitioned DataFrame.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- Division by zero in GOR and decline rate must produce `NaN`, not raise exceptions (use `pandas` safe division).
- If required columns are missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with `oil_bbl = [100.0, 80.0]`, assert `oil_decline_rate_mom[1] = (80 - 100) / 100 = -0.20`.
- `@pytest.mark.unit` — Given `oil_bbl[t-1] = 0.0`, assert `oil_decline_rate_mom[t] = NaN` (no division by zero).
- `@pytest.mark.unit` — Given the first month of a well's production, assert `oil_decline_rate_mom = NaN`.
- `@pytest.mark.unit` — Given `oil_bbl = 100.0` and `gas_mcf = 50.0`, assert `gor = 0.5`.
- `@pytest.mark.unit` — Given `oil_bbl = 0.0`, assert `gor = NaN`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given two wells in the same partition, assert decline rates do not bleed across wells (first month of each well must have `NaN` decline rate).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 24: Implement categorical encoder

**Module:** `kgs_pipeline/features.py`  
**Function:** `encode_categorical_features(ddf: dask.dataframe.DataFrame, encoding_map: dict | None = None) -> tuple[dask.dataframe.DataFrame, dict]`

**Description:**  
Label-encode categorical columns and return both the encoded DataFrame and the encoding map.

Columns to encode: `county`, `field`, `producing_zone`, `operator`.

Encoding approach:
- If `encoding_map` is `None` (training mode): compute the full set of unique values for each column by calling `.compute()` on a small aggregation (this is the **only** permitted `.compute()` call in this module — it is a metadata operation, not a data scan), then assign integer codes sorted alphabetically.
- If `encoding_map` is provided (inference mode): apply the existing mapping. Any value not found in the map is assigned code `-1` (unseen category).
- Add columns `county_encoded`, `field_encoded`, `producing_zone_encoded`, `operator_encoded` as `int32`.
- The original categorical columns (`county`, `field`, `producing_zone`, `operator`) are retained in the output alongside the encoded columns.
- Return a `tuple` of `(encoded_ddf, encoding_map)` where `encoding_map` is a `dict` of `{column_name: {category_string: integer_code}}`.

**Error handling:**
- If any of the 4 categorical columns are missing, raise `KeyError` listing the missing columns.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`, `json`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with `county = ["Allen", "Barton", "Allen"]`, assert `county_encoded` has values `[0, 1, 0]` (alphabetical encoding).
- `@pytest.mark.unit` — Given `encoding_map = {"county": {"Allen": 0, "Barton": 1}}` and a new value `"Crawford"` not in the map, assert `county_encoded = -1` for that row.
- `@pytest.mark.unit` — Assert the returned `encoding_map` is a dict with keys for all 4 categorical columns.
- `@pytest.mark.unit` — Assert the encoded columns have dtype `int32`.
- `@pytest.mark.unit` — Assert original categorical columns are preserved in the output alongside encoded columns.
- `@pytest.mark.unit` — Given a DataFrame missing the `field` column, assert `KeyError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 25: Implement encoding map persister

**Module:** `kgs_pipeline/features.py`  
**Function:** `save_encoding_map(encoding_map: dict, output_dir: Path) -> Path`  
**Function:** `load_encoding_map(output_dir: Path) -> dict`

**Description:**  
Persist and load the categorical encoding map as a JSON sidecar file alongside the feature Parquet output.

`save_encoding_map`:
- Serialize `encoding_map` to JSON and write to `output_dir / "encoding_map.json"`.
- Use `json.dump` with `indent=2` for human readability.
- Return the `Path` to the written file.
- Create `output_dir` if it does not exist.

`load_encoding_map`:
- Read `output_dir / "encoding_map.json"` and return the deserialized `dict`.
- Raise `FileNotFoundError` if the file does not exist.

**Dependencies:** `json`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a simple encoding map dict, assert `save_encoding_map` returns a `Path` ending in `"encoding_map.json"` and the file exists on disk.
- `@pytest.mark.unit` — Assert `load_encoding_map` round-trips the dict correctly (save then load produces the same dict).
- `@pytest.mark.unit` — Given a non-existent directory, assert `save_encoding_map` creates the directory before writing.
- `@pytest.mark.unit` — Given a path without `encoding_map.json`, assert `load_encoding_map` raises `FileNotFoundError`.

**Definition of done:** Functions are implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 26: Implement feature Parquet writer

**Module:** `kgs_pipeline/features.py`  
**Function:** `write_feature_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`

**Description:**  
Write the feature-engineered Dask DataFrame to Parquet, partitioned by `well_id`.

- Write to `output_dir` using `dask.dataframe.to_parquet`.
- Partition by `well_id` with `partition_on=["well_id"]`.
- Use `write_index=False`, `overwrite=True`, `compression="snappy"`.
- Return the `Path` to the output directory.
- Log the output path at INFO level.

**Error handling:**
- Create `output_dir` if it does not exist.
- Re-raise write exceptions after logging.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame, assert the function returns a `Path`.
- `@pytest.mark.integration` — After writing, assert the output directory contains `.parquet` files.
- `@pytest.mark.integration` — **Partition correctness**: Assert each partition file contains rows for exactly one `well_id`.
- `@pytest.mark.integration` — **Schema stability**: Sample schema from 2 different well partitions and assert column names and dtypes match.
- `@pytest.mark.integration` — **Parquet readability**: Assert every `.parquet` file can be read by a fresh `dask.dataframe.read_parquet` call without errors.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 27: Implement features pipeline orchestrator

**Module:** `kgs_pipeline/features.py`  
**Function:** `run_features_pipeline(processed_dir: Path, output_dir: Path) -> Path`

**Description:**  
Top-level entry point for the features component. Chains all feature engineering steps and writes the final feature Parquet.

Execution sequence:
1. `read_processed_parquet(processed_dir)` → processed Dask DataFrame
2. `compute_time_features(ddf)` → time features added
3. `compute_rolling_features(ddf)` → rolling statistics added
4. `compute_decline_and_gor(ddf)` → decline rate and GOR added
5. `encode_categorical_features(ddf)` → encoded columns added, `encoding_map` returned
6. `save_encoding_map(encoding_map, output_dir)` → encoding map persisted to JSON
7. `write_feature_parquet(ddf, output_dir)` → feature Parquet written to disk
8. Return the output `Path`.

- Log start, end, and elapsed time of each step.
- `.compute()` is called only once inside `encode_categorical_features` for the encoding map metadata aggregation, and once implicitly during `to_parquet`. No other `.compute()` calls are permitted.

**Error handling:**
- Propagate exceptions after logging them with the step name.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`, `time`

**Test cases:**
- `@pytest.mark.unit` — Mock all sub-functions. Assert they are called in the correct order (steps 1–8).
- `@pytest.mark.unit` — Assert that no extra `.compute()` calls are made beyond the two permitted ones (in `encode_categorical_features` and `to_parquet`).
- `@pytest.mark.integration` — Given a real processed wells directory, run the full features pipeline and assert the output directory contains `.parquet` files and `encoding_map.json`.
- `@pytest.mark.integration` — Assert all 16 engineered feature columns are present in the output Parquet schema.
- `@pytest.mark.integration` — **Unit consistency check**: Assert that for all rows, `oil_bbl` values in the feature Parquet are within the range `[0, 50000]` or flagged as `is_suspect_rate = True`. Values above 50,000 must have `is_suspect_rate = True`.
- `@pytest.mark.integration` — **Lazy Dask evaluation**: Assert the return type of each intermediate function (steps 2–5) is `dask.dataframe.DataFrame`, not `pandas.DataFrame`.
- `@pytest.mark.integration` — **Parquet readability**: Assert all output Parquet files are readable without errors by a fresh `dask.dataframe.read_parquet` call.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
