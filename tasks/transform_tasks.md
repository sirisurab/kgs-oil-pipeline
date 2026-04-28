# Transform Stage Tasks

**Module:** `kgs_pipeline/transform.py`  
**Test file:** `tests/test_transform.py`  
**Governing docs:** stage-manifest-transform.md, boundary-ingest-transform.md,
boundary-transform-features.md, ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006,
ADR-007, build-env-manifest.md

---

## Task T-01: Parse production date column

**Module:** `kgs_pipeline/transform.py`  
**Function:** `parse_production_date(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition (used via `map_partitions`). Convert the MONTH-YEAR
column into a `production_date` column of dtype `datetime64[ns]`. MONTH-YEAR has format
"M-YYYY" (e.g., "1-2024"). Rows with MONTH-YEAR values that cannot be parsed to a valid
date (e.g., month=0 or month=-1 representing yearly/cumulative records) must be dropped.
Return the partition DataFrame with `production_date` added and MONTH-YEAR retained.

**Input:** `pd.DataFrame` — one partition as delivered by boundary-ingest-transform.md
(canonical schema, data-dictionary dtypes, unsorted).  
**Output:** `pd.DataFrame` — same columns plus `production_date` as `datetime64[ns]`.
Rows with unparseable MONTH-YEAR are dropped.

**Constraints:**
- String operations for year/month extraction must use vectorized str accessor methods —
  see ADR-002.
- Per-row iteration is prohibited — see ADR-002.
- Date parsing must handle the "M-YYYY" format directly; do not rely on pandas inference.
- No retry logic, no caching.

**Test cases (TR-05):**
- Given a partition with MONTH-YEAR "1-2024", assert `production_date` is
  `datetime64("2024-01-01")`.
- Given a partition with MONTH-YEAR "0-2024" (yearly record), assert the row is dropped.
- Given a partition with MONTH-YEAR "-1-2024" (starting cumulative), assert the row is
  dropped.
- Assert the `production_date` column dtype is `datetime64[ns]`.
- Assert zeros in PRODUCTION remain as zeros — not converted to null (TR-05).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-02: Cast categorical columns and replace invalid values

**Module:** `kgs_pipeline/transform.py`  
**Function:** `cast_categoricals(df: pd.DataFrame, data_dict: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. For each column declared `dtype="categorical"` in
the data dictionary (`kgs_monthly_data_dictionary.csv`), replace any value not in the
declared category set with the appropriate null sentinel before casting (see ADR-003 and
stage-manifest-transform.md H2). The declared category values are pipe-delimited in the
`categories` column of the data dictionary. After replacement, cast to
`pd.CategoricalDtype(categories=[...], ordered=False)` using the declared values. Return
the partition with categorical columns cast.

Categorical columns in `kgs_monthly_data_dictionary.csv`: `TWN_DIR` (categories: S, N),
`RANGE_DIR` (categories: E, W), `PRODUCT` (categories: O, G).

**Input:** `pd.DataFrame` — one partition; `data_dict: pd.DataFrame` — data dictionary.  
**Output:** `pd.DataFrame` — same shape, categorical columns cast to declared category
sets. Invalid values are replaced with the appropriate null sentinel before casting
(see ADR-003).

**Constraints:**
- Invalid values must be replaced before casting — see ADR-003 and stage-manifest-transform.md H2.
- The null sentinel for CategoricalDtype columns is `pd.NA` — see ADR-003.
- Categorical dtype must use the declared ordered category list from the data dictionary —
  not inferred from the data.
- No retry logic, no caching.

**Test cases (TR-12, TR-14):**
- Given a partition where PRODUCT contains "X" (invalid), assert it becomes `pd.NA`
  after casting, not "X".
- Given a partition where TWN_DIR contains "S" and "N" only, assert the column dtype is
  `CategoricalDtype` with categories `["S", "N"]`.
- Given a partition where RANGE_DIR contains "E", "W", and an invalid value, assert the
  invalid value is replaced with `pd.NA` and the column dtype is `CategoricalDtype`.
- Assert schema is identical across two partitions with different value distributions
  (TR-14).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-03: Validate physical bounds

**Module:** `kgs_pipeline/transform.py`  
**Function:** `validate_bounds(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. Replace values violating physical constraints with
the appropriate null sentinel — do not drop rows. Constraints (see TR-01, TR-02):

- `PRODUCTION` cannot be negative — replace negative values with `np.nan`.
- Oil production rates above 50,000 BBL/month for a single lease are almost certainly
  unit errors (TR-02) — replace values exceeding 50,000 with `np.nan` for rows where
  `PRODUCT == "O"`.

Return the partition with out-of-bounds values replaced.

**Input:** `pd.DataFrame` — one partition.  
**Output:** `pd.DataFrame` — same shape. Out-of-bounds PRODUCTION values replaced with
`np.nan`.

**Constraints:**
- Float null sentinel is `np.nan`, not `pd.NA` — see ADR-003.
- Operations must be vectorized — see ADR-002.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-01, TR-02):**
- Given a partition with PRODUCTION = -5.0 (oil), assert the value is replaced with
  `np.nan`.
- Given a partition with PRODUCTION = 100000.0 and PRODUCT = "O", assert the value is
  replaced with `np.nan`.
- Given a partition with PRODUCTION = 0.0, assert the value remains 0.0 — zero is a
  valid measurement (TR-05).
- Given a partition with PRODUCTION = 100000.0 and PRODUCT = "G" (gas, units in MCF),
  assert the value is NOT replaced — the 50,000 cap applies only to oil.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-04: Deduplicate per entity and production date

**Module:** `kgs_pipeline/transform.py`  
**Function:** `deduplicate(df: pd.DataFrame, entity_col: str, date_col: str) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition that has already been indexed on `entity_col` (i.e.,
the index is `entity_col`). Drop duplicate rows keyed on `(entity_col, date_col)` —
keeping the first occurrence per (entity, production_date) pair — as defined in
task-writer-kgs.md `<deduplication>`. The subset for `drop_duplicates` must explicitly
name the index level (or reset index before deduplication and restore afterward).

**Input:** `pd.DataFrame` — one partition indexed on `entity_col`.  
**Output:** `pd.DataFrame` — same schema, with duplicate (entity, production_date) rows
removed.

**Constraints:**
- Deduplication must key on `(entity_col, date_col)` — a bare `drop_duplicates()` with
  no subset must not be used — see task-writer-kgs.md and stage-manifest-transform.md H5.
- KGS files have no revision column — no tie-breaking by revision field is needed (see
  task-writer-kgs.md).
- No retry logic, no caching.

**Test cases (TR-15):**
- Given a partition with two rows for the same (LEASE_KID, production_date) but different
  PRODUCTION values, assert only the first row is retained.
- Assert the row count after deduplication is less than or equal to the row count before.
- Assert that running deduplication twice on the same data produces the same result as
  running it once (idempotency — TR-15).
- Assert a bare `drop_duplicates()` call with no subset is not used (inspect function
  source or verify by behaviour on a DataFrame where rows differ only in PRODUCTION).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-05: Fill temporal gaps per well

**Module:** `kgs_pipeline/transform.py`  
**Function:** `fill_date_gaps(df: pd.DataFrame, entity_col: str, date_col: str) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. For each unique entity in the partition, ensure
every month between the entity's first and last `production_date` is present. For missing
months, insert a row with `PRODUCTION = 0.0` and all other columns propagated from the
entity's most recent prior row (forward-fill) or left as the appropriate null sentinel
where no prior value exists. This preserves the distinction between zero production
(reported shut-in) and missing data (TR-04, TR-05).

**Input:** `pd.DataFrame` — one partition indexed on `entity_col`.  
**Output:** `pd.DataFrame` — same schema with any missing monthly records inserted.

**Constraints:**
- Per-entity aggregation must use vectorized grouped transformations — see ADR-002.
- Zero production is a valid measurement, not a null — see TR-05.
- The distinction between zero-production and missing rows must be preserved — see TR-05.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-04, TR-05):**
- Given a well with records for January and March 2024 but not February 2024, assert a
  February row is inserted with PRODUCTION = 0.0.
- Assert that an existing zero-production row is unchanged after gap-filling.
- Assert the inserted rows carry the same entity identifier in the index.
- Assert that after gap-filling, each entity's record count equals its expected span from
  first to last production_date (TR-04).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-06: Apply all partition-level transformations via map_partitions

**Module:** `kgs_pipeline/transform.py`  
**Function:** `apply_transformations(ddf: dd.DataFrame, data_dict: pd.DataFrame) -> dd.DataFrame`

**Description:**  
Apply `parse_production_date`, `cast_categoricals`, `validate_bounds`, and `fill_date_gaps`
to every partition of the input Dask DataFrame using `map_partitions`. Each
`map_partitions` call must supply a `meta=` argument derived by calling the actual
function on a minimal real input sliced to zero rows — no helper functions, no
empty-frame builders (see ADR-003 and TR-23). The Dask DataFrame must remain lazy
throughout; do not call `.compute()` (see ADR-005, TR-17). `set_index` and
`deduplicate` are not called here — they are applied in the orchestrating `transform`
function after this call (see stage-manifest-transform.md H4, H5).

**Input:**
- `ddf: dd.DataFrame` — the interim Parquet dataset as delivered by
  boundary-ingest-transform.md (canonical schema, data-dictionary dtypes, unsorted).
- `data_dict: pd.DataFrame` — data dictionary.

**Output:** `dd.DataFrame` — lazy Dask DataFrame with `production_date` column added,
categoricals cast, bounds validated, date gaps filled. Entity column is not yet the
index.

**Constraints:**
- Meta derivation: call each actual function on a minimal real input sliced to zero rows
  — see ADR-003. No separate meta helpers.
- Float null sentinel is `np.nan` — see ADR-003.
- No `.compute()` calls inside this function — see ADR-005, TR-17.
- No retry logic, no caching.

**Test cases (TR-17, TR-23):**
- Assert the return type is `dd.DataFrame` (not `pd.DataFrame`) — TR-17.
- For each `map_partitions` call, verify that the `meta=` argument matches the actual
  function output in column names, column order, and dtypes — TR-23.
- Assert `.compute()` is not called inside this function.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T-07: Orchestrate transform stage and write processed Parquet

**Module:** `kgs_pipeline/transform.py`  
**Function:** `transform(config: dict) -> None`

**Description:**  
Orchestrate the full transform stage. Read interim Parquet from
`config["transform"]["interim_dir"]` using `dd.read_parquet`. Call `apply_transformations`
to apply all partition-level operations. Call `set_index` on `LEASE_KID` (the entity
column) to establish the entity index — this is the last structural operation before
writing (see stage-manifest-transform.md H4). After `set_index`, call `map_partitions`
to apply `deduplicate`. Repartition to `max(10, min(n, 50))` where `n` is the partition
count of the input (see ADR-004); repartition must be the last operation before writing
(see ADR-004, stage-manifest-transform.md H3). Write the result to
`config["transform"]["processed_dir"]` using `dask.dataframe.to_parquet`. The task graph
must remain lazy until the write operation (see ADR-005). Use the distributed scheduler
per ADR-001.

**Input:** `config: dict` — full config loaded from `config.yaml`.  
**Output:** None. Side effect: Parquet files written to processed directory.

**Constraints:**
- `set_index` must be the last structural operation before writing — see
  stage-manifest-transform.md H4. All column operations must precede it.
- Deduplication applied after `set_index` — see stage-manifest-transform.md H5.
- Temporal sort must be valid at stage exit — see stage-manifest-transform.md H1.
  Sort by `production_date` within each entity group must be applied (via
  `map_partitions` or `sort_values`) at an appropriate point in the chain.
- Repartition is the last operation before write — see ADR-004.
- Task graph stays lazy until write — see ADR-005.
- Scheduler: distributed — see ADR-001.
- Logging: see ADR-006.
- No retry logic, no caching.
- Output satisfies all guarantees in boundary-transform-features.md.

**Test cases (TR-11, TR-12, TR-13, TR-14, TR-15, TR-16, TR-17, TR-18, TR-25):**
- Assert Parquet files are written to the processed directory and are readable (TR-18).
- Assert each Parquet partition contains data for rows with only one `LEASE_KID` value —
  no partition spans multiple wells (TR-13).
- Assert data types in sampled partitions match data-dictionary dtypes (TR-12).
- Assert schema is identical across sampled partitions (TR-14).
- Assert the output row count is less than or equal to the input row count (TR-15).
- Assert sort order: within any partition, `production_date` values are non-decreasing
  (TR-16).
- Assert the return type of the function's internal Dask operations does not materialize
  to pandas before the write (TR-17).
- Run `transform()` on the interim Parquet output of TR-24 using `tmp_path` for all
  output paths; assert processed Parquet satisfies all guarantees in
  boundary-transform-features.md (TR-25).
- Spot-check: for at least one well, verify that PRODUCTION values in the processed
  output match the raw source file values for the same MONTH-YEAR (TR-11).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
