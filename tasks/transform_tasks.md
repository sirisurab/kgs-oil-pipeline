# Transform Stage — Task Specifications

All tasks in this file implement the transform stage for the `kgs_pipeline`
package. Read the following authoritative documents before starting:
- `/agent_docs/ADRs.md`
- `/agent_docs/build-env-manifest.md`
- `/agent_docs/stage-manifest-transform.md`
- `/agent_docs/boundary-ingest-transform.md` (input contract)
- `/agent_docs/boundary-transform-features.md` (output contract)
- `/task-writer-kgs.md`
- `/test-requirements.xml` (TR-01, TR-02, TR-04, TR-05, TR-11, TR-12,
  TR-13, TR-14, TR-15, TR-16, TR-17, TR-18, TR-23, TR-25, TR-27)
- `references/kgs_monthly_data_dictionary.csv`

All design decisions are governed by those documents. Where those documents
speak, cite them by name — do not restate.

## Context

The transform stage reads the interim Parquet produced by ingest, cleans
and reshapes the data into an analysis-ready form, and writes processed
Parquet to `data/processed/` (path from `config.yaml`).

**Entity column.** The processed output is entity-indexed per
`stage-manifest-transform.md` and `boundary-transform-features.md`. The
entity column for KGS lease data is `LEASE_KID` — the unique, non-nullable
lease ID defined in `kgs_monthly_data_dictionary.csv`. All references to
`{entity_col}` in the stage manifest and boundary contract resolve to
`LEASE_KID`.

**Production date column.** The `MONTH-YEAR` column in the dictionary has
the format `"M-YYYY"` (see the project data dictionary and `<data-filtering>`
in `task-writer-kgs.md`). The transform stage derives a `production_date`
column (nullable datetime) from `MONTH-YEAR`. The `production_date` column
is what `stage-manifest-transform.md` refers to when it says "sorted by
production_date within each partition".

---

## Task T1: Build a partition-level cleaning function

**Module:** `kgs_pipeline/transform.py`

**Function:** a pandas-level function that takes a single partition
(pandas DataFrame) conforming to the ingest output contract and returns
a cleaned pandas DataFrame.

**Inputs (upstream reliance from `boundary-ingest-transform.md`):**
- Canonical schema is already established — no column rename.
- Data-dictionary dtypes are already enforced — no re-casting.
- All nullable columns are present.
- Partition count is within contract — no re-partitioning needed.

**Output contract (every return path must satisfy this — including empty
partitions, per the Task Decomposition Constraint):**
- All input columns preserved.
- New column `production_date` (nullable datetime) derived from
  `MONTH-YEAR`. Rows where `MONTH-YEAR` month component is `0` (yearly
  aggregate) or `-1` (starting cumulative) per the dictionary note must
  be dropped — those are not monthly records and would pollute monthly
  features.
- Duplicate rows removed. Duplication key: `LEASE_KID` + `production_date`
  + `PRODUCT` (two rows for the same lease, month, and product are
  duplicates). TR-15 requires deduplication to be idempotent.
- Invalid physical values replaced with the appropriate null sentinel per
  ADR-003:
  - `PRODUCTION < 0` → null (TR-01 — physical bound).
  - `WELLS < 0` → null.
  - Unit-error outliers per TR-02 (e.g. oil per lease-month above a
    realistic range — threshold configured in `config.yaml`) → null.
- Zero-vs-null distinction preserved per TR-05: a zero in the raw data
  must remain zero, not be converted to null.
- Categorical columns carry only declared category values per ADR-003
  and stage-manifest-transform H2.
- Column dtypes preserved end-to-end per ADR-003 (no upcasting of
  nullable-int columns to float).

**Design decisions governed by docs:**
- Vectorization requirement → ADR-002 (no per-row iteration).
- Null sentinel and categorical handling → ADR-003.
- Zero-vs-null distinction → TR-05.
- Physical bounds → TR-01, TR-02.

**Test cases (unit):**
- Given a partition containing rows with `MONTH-YEAR` = `0-2024` and
  `-1-2023`, assert those rows are dropped.
- Given a partition with two rows identical on (LEASE_KID,
  production_date, PRODUCT), assert one row remains. Run the function
  twice and assert idempotency (TR-15).
- Given a partition with `PRODUCTION` = -5, assert the value is replaced
  with null (TR-01).
- Given a partition with `PRODUCTION` = 0 and `WELLS` = 0, assert both
  remain zero (TR-05).
- Given a partition with a PRODUCTION value above the configured
  unit-error threshold, assert it is replaced with null (TR-02).
- Given an empty input partition with full schema, assert the output is
  an empty DataFrame with the full schema including `production_date`.

**Definition of done:** Function is implemented, test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task T2: meta-derivation helper for `map_partitions`

**Module:** `kgs_pipeline/transform.py`

**Function:** derives the Dask `meta` for the T1 function (and any other
`map_partitions` call in the stage) by calling the actual function on a
zero-row copy of the input's `_meta` — not by constructing meta separately.

**Design decisions governed by docs:**
- Meta derivation method → ADR-003 ("Meta derivation and function
  execution must share the same code path"). Any construction of meta
  separate from calling the actual function is prohibited.
- Meta consistency requirement → TR-23.

**Test cases (unit, TR-23):**
- For each `map_partitions`-applied function in the transform stage
  (including T1 and anything added in T3/T4), call it on a zero-row
  DataFrame matching the ingest output schema and assert the resulting
  column list, column order, and dtypes match the meta used at the
  `map_partitions` call site.

**Definition of done:** Function is implemented, tests pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task T3: Transform stage entry point

**Module:** `kgs_pipeline/transform.py`

**Function:** `transform(config: dict) -> None`.

**Behavior:**
- Reads interim Parquet from the configured path (produced by ingest).
- Applies the T1 cleaning via `map_partitions` with T2-derived meta.
- Casts categoricals to their declared category sets (ADR-003,
  stage-manifest-transform H2).
- Sorts by `production_date` within each partition such that the sort
  is valid at stage exit (stage-manifest-transform H1 — the shuffle from
  `set_index` destroys any prior ordering).
- Sets the index on `LEASE_KID` (`set_index`) as the LAST structural
  operation before writing (stage-manifest-transform H4).
- Repartitions to `max(10, min(n, 50))` (ADR-004) as the last operation
  before write (ADR-004 — the repartition step must be the last op).
- Writes Parquet to the configured processed path.

**Output guarantees at stage exit (boundary-transform-features.md):**
- Entity-indexed on `LEASE_KID`.
- Sorted by `production_date` within each partition.
- Categoricals cast to declared category sets.
- Invalid values replaced with the appropriate null sentinel.
- Partitions: `max(10, min(n, 50))`.

**Design decisions governed by docs:**
- Scheduler reuse → build-env-manifest "Dask scheduler initialization".
- Lazy execution → ADR-005 (no `.compute()` before the final write;
  batch any independent computations into a single call).
- Partition-count formula and final-operation position → ADR-004.
- Sort-before-set_index ordering → stage-manifest-transform H1, H4.
- Vectorized grouped transformations → ADR-002.
- Logging → ADR-006.
- String-operation row filtering must be done inside a partition-level
  function → ADR-004 (Dask string accessor unreliable post-repartition).

**Error handling:**
- Config or path errors propagate as raised exceptions.
- Per-partition failures caught by `map_partitions` must preserve the
  output contract on every return path (T1).

**Test cases:**
- Unit (TR-17): intermediate functions return Dask DataFrames rather than
  pandas DataFrames.
- Unit (TR-13): read the output Parquet and assert each partition file
  contains rows for exactly one `LEASE_KID` — set_index on a
  low-to-moderate cardinality entity combined with repartitioning must
  not mix entities across a partition file.

  Note: ADR-004 prohibits per-entity partitioning (one file per well).
  TR-13 applies only to the natural per-partition result of `set_index`
  on `LEASE_KID`; it verifies that after set_index + repartition, each
  partition file contains data for exactly one `LEASE_KID` value. If
  `set_index` combined with repartition cannot guarantee this property
  at project data volumes without generating one-file-per-lease (ADR-004
  violation), the coder must reconcile the two by choosing the
  partitioning that satisfies ADR-004 and flag TR-13 as inapplicable in
  the test module. Do not violate ADR-004 to satisfy TR-13.
- Unit (TR-14): sample two processed partition files and assert the
  column list and dtypes are identical.
- Unit (TR-15): assert processed row count ≤ interim row count.
  Run the stage twice and assert idempotency.
- Unit (TR-16): for any `LEASE_KID` that spans multiple partition files,
  assert the last `production_date` in one file is ≤ the first
  `production_date` in the next.
- Unit (TR-18): every output Parquet file is readable by a fresh pandas
  or Dask process.
- Unit (TR-11): spot-check PRODUCTION values from raw to processed for
  randomly chosen (LEASE_KID, month) pairs across a meaningful sample.
- Unit (TR-12): assert nulls, duplicates, and dtypes are handled per the
  T1 contract.
- Unit (TR-04): for a LEASE_KID with production from January to December,
  assert 12 monthly records are present — no unexpected gaps.
- Unit (TR-05): assert zero values in raw data remain zero (not null)
  in processed data.
- Integration (TR-25): run `transform(config)` on the interim Parquet
  output of the TR-24 fixture using a config dict with output paths in
  `tmp_path` (ADR-008). Assert the output is readable and satisfies
  every upstream guarantee in `boundary-transform-features.md`.
- Integration (TR-27): see pipeline_tasks.md.

**Definition of done:** Function is implemented, all test cases pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.
