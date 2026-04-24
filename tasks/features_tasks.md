# Features Stage — Task Specifications

All tasks in this file implement the features stage for the `kgs_pipeline`
package. Read the following authoritative documents before starting:
- `/agent_docs/ADRs.md`
- `/agent_docs/build-env-manifest.md`
- `/agent_docs/stage-manifest-features.md`
- `/agent_docs/boundary-transform-features.md` (input contract)
- `/test-requirements.xml` (TR-03, TR-06, TR-07, TR-08, TR-09, TR-10, TR-14,
  TR-17, TR-18, TR-19, TR-23, TR-26, TR-27)
- `references/kgs_monthly_data_dictionary.csv`

All design decisions are governed by those documents. Where those documents
speak, cite them by name — do not restate.

## Context

The features stage reads the entity-indexed, date-sorted Parquet produced by
transform and adds derived columns required for ML workflows. The output is
the final, ML-ready Parquet in `data/processed/` (or the path configured in
`config.yaml`).

**Entity column and production-date column.** Defined in the transform task
spec and in `boundary-transform-features.md` — the input is indexed on
`LEASE_KID` and sorted by `production_date` within each partition.

**Per-entity derivation contract.** All time-dependent features (cumulative
sums, rolling windows, lag features, decline rates) must be computed within
an entity group (grouped by `LEASE_KID`) in a way that preserves the
per-entity temporal ordering guaranteed by
`boundary-transform-features.md` (H1 in `stage-manifest-features.md`).
ADR-002 prohibits per-row iteration and per-entity Python loops — use
vectorized grouped transformations.

**Product-aware aggregation.** KGS production is reported per product
(`PRODUCT` ∈ {`O`, `G`}) with volumes in a single `PRODUCTION` column (BBL
for oil, MCF for gas — see `kgs_monthly_data_dictionary.csv`). The features
stage must derive `oil_bbl`, `gas_mcf`, and, where source data provides it,
`water_bbl` columns by pivoting the product-keyed `PRODUCTION` value into
product-specific columns before computing ratios. Keep a row per
(LEASE_KID, production_date) after this reshape so that downstream time
features operate on a single row per entity-month.

---

## Task F1: Product-wise reshape of production

**Module:** `kgs_pipeline/features.py`

**Function:** a partition-level (pandas) function that takes a cleaned
transform-stage partition and returns one row per (LEASE_KID,
production_date) with product-specific volume columns.

**Output contract (every return path must satisfy this — including empty
partitions, per the Task Decomposition Constraint):**
- Row granularity: one row per (LEASE_KID, production_date) within the
  partition.
- Volume columns: `oil_bbl` (float, nullable-aware per ADR-003) from
  `PRODUCTION` where `PRODUCT == "O"`; `gas_mcf` (float, nullable-aware)
  from `PRODUCTION` where `PRODUCT == "G"`. If the source data provides a
  water volume column, expose it as `water_bbl`; otherwise add `water_bbl`
  as an all-NA column of the correct dtype (per ADR-003 absent-column
  handling).
- Non-volume identifier and attribute columns from the input are preserved
  (LEASE, OPERATOR, COUNTY, and the location attributes listed in
  `kgs_monthly_data_dictionary.csv`). Where an attribute varies across
  products for the same (LEASE_KID, month) due to source inconsistency,
  a deterministic choice must be made (documented in the module docstring);
  do not silently coerce to null.
- Zero-vs-null distinction preserved per TR-05.
- Column dtypes preserved per ADR-003.
- An empty input partition returns an empty DataFrame with the full output
  schema (stage-manifest-ingest H2 applies equally here — empty is valid,
  unschema'd is not).

**Design decisions governed by docs:**
- Vectorization → ADR-002 (no Python loops over entities or rows).
- Dtype policy → ADR-003.
- Zero-vs-null distinction → TR-05.
- Categorical handling for `PRODUCT` during the reshape → ADR-003 +
  `boundary-transform-features.md` (categoricals are already clean at
  input — no re-validation needed).

**Test cases (unit):**
- Given a partition with rows for one LEASE_KID, one month, two PRODUCT
  values (O and G), assert a single output row with both `oil_bbl` and
  `gas_mcf` populated.
- Given a partition with a row where `PRODUCT == "O"` and `PRODUCTION == 0`,
  assert `oil_bbl == 0` (not null) (TR-05).
- Given a partition with only oil rows, assert `gas_mcf` is null for every
  output row.
- Given an empty input partition with full schema, assert an empty output
  with the full feature-stage input schema plus `oil_bbl`, `gas_mcf`,
  `water_bbl`.
- Given a partition with `PRODUCT` category values outside {`O`,`G`},
  assert those rows are dropped or handled per the ADR-003 null sentinel
  rule — no silent propagation.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task F2: Cumulative, ratio, and decline-rate features

**Module:** `kgs_pipeline/features.py`

**Function:** a partition-level function that takes the F1 output and adds
cumulative production, GOR, water cut, and period-over-period decline rate
columns. Cumulative and decline features require per-LEASE_KID grouping
within the partition; the partition is already sorted by `production_date`
per `boundary-transform-features.md`.

**Output contract (every return path):**
- Adds columns: `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
  `decline_rate` (all float, nullable-aware per ADR-003).
- `cum_oil`, `cum_gas`, `cum_water` are per-LEASE_KID cumulative sums of
  `oil_bbl`, `gas_mcf`, `water_bbl` over `production_date`. They must be
  monotonically non-decreasing within each LEASE_KID (TR-03) and must
  remain flat across zero-production months (TR-08).
- `gor = gas_mcf / oil_bbl` with zero-denominator handling per TR-06:
  `oil_bbl == 0` and `gas_mcf > 0` → NaN; `oil_bbl == 0` and `gas_mcf == 0`
  → NaN or 0 (pick one, document in docstring, stay consistent); no
  ZeroDivisionError raised.
- `water_cut = water_bbl / (oil_bbl + water_bbl)` with the denominator
  being total liquid, not just oil (TR-09 (e)). Valid at 0 and 1 — per
  TR-10.
- `decline_rate` is the month-over-month rate on `oil_bbl` per LEASE_KID,
  clipped to `[-1.0, 10.0]` per TR-07. The raw computation must be
  evaluated before clipping (TR-07 (d)); a shut-in-then-resume sequence
  must not produce an unclipped extreme value that survives into output.
- Empty input partition → empty output with the full schema including
  every new column at its declared dtype.

**Design decisions governed by docs:**
- Vectorized grouped transformations → ADR-002 (use groupby-transform /
  groupby-cumsum / groupby-shift; no per-group Python loop).
- Temporal ordering reliance → `boundary-transform-features.md` H1 — the
  transform guarantees within-partition sort; features does not re-sort.
- Null sentinel and dtype policy → ADR-003.
- Decline-rate clip bounds → TR-07.
- GOR zero-denominator contract → TR-06.
- Water-cut formula and boundary validity → TR-09 (e), TR-10.
- Cumulative flat-period contract → TR-08.
- Cumulative monotonicity contract → TR-03.

**Test cases (unit):**
- Given a synthetic single-LEASE_KID 6-month series with known values,
  assert `cum_oil`, `cum_gas`, `cum_water` match hand-computed values
  (TR-09 (a)).
- Given a sequence with a mid-series zero-production month, assert
  `cum_oil` is flat across the zero month and resumes correctly
  (TR-08 (a), (c)).
- Given a sequence with zero-production months at the start (well not yet
  online), assert `cum_oil` stays at 0 through those months (TR-08 (b)).
- Given `oil_bbl = 0, gas_mcf = 100`, assert `gor` is NaN and no exception
  is raised (TR-06 (a)).
- Given `oil_bbl = 0, gas_mcf = 0`, assert `gor` is the declared value (0
  or NaN per docstring) and no exception (TR-06 (b)).
- Given `oil_bbl = 10, gas_mcf = 0`, assert `gor == 0.0` (TR-06 (c)).
- Given `water_bbl = 0, oil_bbl = 100`, assert `water_cut == 0.0` and the
  row is retained (TR-10 (a)).
- Given `oil_bbl = 0, water_bbl = 50`, assert `water_cut == 1.0` and the
  row is retained (TR-10 (b)).
- Given a synthetic oil series producing a raw decline rate < -1.0, assert
  `decline_rate == -1.0` after clip (TR-07 (a)).
- Given a raw decline rate > 10.0, assert clip to 10.0 (TR-07 (b)).
- Given a value within bounds, assert pass-through (TR-07 (c)).
- Given a shut-in-then-resume sequence, assert the clip is applied after
  the raw computation and no unclipped extreme leaks into output
  (TR-07 (d)).
- Given an empty input partition with full F1 schema, assert an empty
  output with all feature columns at the declared dtype.

**Definition of done (F2):** Function is implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all
third-party packages imported in this task.

---

## Task F3: Rolling-average and lag features

**Module:** `kgs_pipeline/features.py`

**Function:** a partition-level function that takes the F2 output and adds
rolling averages (3-month and 6-month) for `oil_bbl`, `gas_mcf`, and
`water_bbl`, plus a 1-month lag on `oil_bbl`, `gas_mcf`, and `water_bbl`.

**Output contract (every return path):**
- Adds columns at minimum: `oil_bbl_rolling_3m`, `oil_bbl_rolling_6m`,
  `gas_mcf_rolling_3m`, `gas_mcf_rolling_6m`, `water_bbl_rolling_3m`,
  `water_bbl_rolling_6m`, `oil_bbl_lag_1m`, `gas_mcf_lag_1m`,
  `water_bbl_lag_1m` (all float, nullable-aware per ADR-003).
- Rolling averages are computed per `LEASE_KID` over the
  production_date-sorted sequence within the partition.
- For months with history shorter than the window, the result must be
  NaN (not silently zero and not a wrong partial value) — TR-09 (b). If
  the coder chooses a partial-window mode instead, that must be documented
  and applied identically across all rolling columns; a silent mix of
  partial-window and full-window results across columns is a defect.
- `*_lag_1m` is the value of the column for month N-1 within the same
  LEASE_KID — verified for at least 3 consecutive months per TR-09 (c).
- Empty input partition → empty output with full schema.

**Design decisions governed by docs:**
- Vectorized grouped transformations → ADR-002 (rolling and shift over
  groupby, not per-group Python loops).
- Input temporal ordering → `boundary-transform-features.md` H1.
- Dtype policy → ADR-003.
- Rolling-window correctness rules → TR-09 (a), (b), (c).

**Test cases (unit):**
- Given a known 12-month synthetic single-LEASE_KID sequence, assert the
  3-month and 6-month rolling averages for oil, gas, water match
  hand-computed values (TR-09 (a)).
- Given a well with only 2 months of history, assert the 3-month rolling
  value for month 2 is NaN (or the documented partial-window value)
  consistently across oil, gas, water (TR-09 (b)).
- Given a 4-month synthetic sequence, assert `oil_bbl_lag_1m` for months
  2, 3, 4 equals the raw `oil_bbl` of months 1, 2, 3 (TR-09 (c)).
- Given an empty input partition, assert an empty output with every
  rolling and lag column present at the declared dtype.

**Definition of done (F3):** Function is implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all
third-party packages imported in this task.

---

## Task F4: meta-derivation for features map_partitions calls

**Module:** `kgs_pipeline/features.py`

**Function:** derives Dask `meta` for every `map_partitions` call in the
features stage (F1, F2, F3) by calling the actual function on a zero-row
copy of the upstream `_meta` — not by constructing meta separately.

**Design decisions governed by docs:**
- Meta-derivation method → ADR-003 ("Meta derivation and function
  execution must share the same code path"). Any construction of meta
  separate from calling the actual function is prohibited — whether from
  the data dictionary, from function logic, or via a helper function.
- Meta consistency requirement → TR-23.

**Test cases (unit, TR-23):**
- For each of F1, F2, F3, call the function on a zero-row DataFrame
  matching the upstream schema and assert the result's columns, column
  order, and dtypes exactly match the meta supplied at the
  `map_partitions` call site.

**Definition of done (F4):** Function is implemented, tests pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task F5: Features stage entry point

**Module:** `kgs_pipeline/features.py`

**Function:** `features(config: dict) -> None`.

**Behavior:**
- Reads the entity-indexed, production_date-sorted Parquet produced by
  transform (input contract: `boundary-transform-features.md` downstream
  reliance — all those guarantees are relied on, not re-established).
- Applies F1 (reshape) → F2 (cumulative / ratios / decline) → F3 (rolling
  and lag) via `map_partitions` with F4-derived meta.
- Repartitions to `max(10, min(n, 50))` (ADR-004) as the last operation
  before write.
- Writes Parquet to the configured features output path in
  `data/processed/`.

**Output guarantees at stage exit (stage-manifest-features):**
- All input columns plus derived feature columns.
- ML-ready Parquet files in `data/processed/`.
- Complete column set per TR-19: well_id (i.e. LEASE_KID), production_date,
  oil_bbl, gas_mcf, water_bbl, cum_oil, cum_gas, cum_water, gor,
  water_cut, decline_rate, and every rolling and lag column from F3.

**Design decisions governed by docs:**
- Scheduler reuse → `build-env-manifest.md` "Dask scheduler
  initialization".
- Laziness → ADR-005 (no `.compute()` before the final write; batch
  independent branches into one compute).
- Partition count formula and final-operation position → ADR-004.
- Input contract reliance (no re-sort, no re-index, no re-cast of
  categoricals) → `boundary-transform-features.md`.
- Vectorized grouped transformations → ADR-002.
- Logging → ADR-006.

**Error handling:**
- Config or path errors propagate as raised exceptions.
- Per-partition errors in F1/F2/F3 must still satisfy the output contract
  on every return path.

**Test cases:**
- Unit (TR-17): intermediate functions in the features pipeline return
  Dask DataFrames (not pandas) — the stage does not call `.compute()` on
  the working graph before the final write.
- Unit (TR-14): sample two feature partitions from the output and assert
  identical column list and dtypes.
- Unit (TR-18): every output Parquet file is readable by a fresh pandas
  or Dask process.
- Unit (TR-19): for a synthetic single-LEASE_KID input, assert the output
  contains the complete column list enumerated in TR-19 — not a subset.
- Integration (TR-26): run `features(config)` on the transform Parquet
  output from TR-25 using a config dict with output paths in `tmp_path`
  (ADR-008). Assert the features Parquet is readable, all derived feature
  columns defined in `stage-manifest-features.md` / TR-19 are present, and
  schema is consistent across a sample of partitions.
- Integration (TR-27): see `pipeline_tasks.md`.

**Definition of done (F5):** Function is implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all
third-party packages imported in this task.

