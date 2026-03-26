# Task Index

| # | Task File | Description |
|---|-----------|-------------|
| 1 | `tasks/acquire_tasks.md` | Configuration module (`config.py`), shared utility helpers (`utils.py`), lease URL loader, single-lease Playwright scraper with `asyncio.Semaphore(5)` rate-limiting, and parallel Dask-driven acquire orchestrator; raw `.txt` files downloaded to `data/raw/`. |
| 2 | `tasks/ingest_tasks.md` | Dask-based discovery and reading of raw per-lease `.txt` files, monthly record filtering, lease metadata enrichment join, schema casting, and writing a unified interim Parquet store partitioned by `LEASE_KID` to `data/interim/`. |
| 3 | `tasks/transform_tasks.md` | Production date parsing, `snake_case` column normalization, API number explosion to per-well rows (`well_id`), physical bounds validation (negative → NaN, outlier flagging), deduplication, chronological sort, repartition by `well_id`, cleaned Parquet output to `data/processed/`, and cleaning report JSON. |
| 4 | `tasks/features_tasks.md` | ML-ready feature engineering per well (cumulative production Np/Gp, decline rate clipped to [-1.0, 10.0], rolling stats at 3/6/12 months, lag features, time features, GOR/water-cut/WOR, categorical label encoding), features Parquet output to `data/processed/features/`, CSV export, and the CLI pipeline orchestrator (`pipeline.py`). |

## Pipeline Execution Order

```
run_acquire_pipeline()  →  run_ingest_pipeline()  →  run_transform_pipeline()  →  run_features_pipeline()
    acquire.py                  ingest.py                  transform.py                 features.py
    data/raw/                   data/interim/              data/processed/              data/processed/features/
```

## Component Summary

### Component 1 — Acquire (`kgs_pipeline/acquire.py`) + Config + Utils
- **Tasks 01–05** (plus config and utils sub-tasks)
- `kgs_pipeline/config.py`: Pydantic `BaseSettings` with all pipeline constants and a module-level `CONFIG` singleton.
- `kgs_pipeline/utils.py`: `setup_logging`, `retry` decorator, `timer` decorator, `compute_file_hash`, `ensure_dir`, `is_valid_raw_file`.
- Reads lease URLs from `data/external/oil_leases_2020_present.txt`.
- Scrapes each lease page via Playwright async API; navigates to MonthSave page; downloads `.txt` file.
- Rate-limited to 5 concurrent requests (`asyncio.Semaphore(5)`).
- Downloads raw per-lease monthly `.txt` files to `data/raw/`.
- Custom `ScrapingError` exception class.
- Dask delayed graph with a single `.compute()` call in `run_acquire_pipeline()`.
- Idempotent: skips already-downloaded valid files on re-run.

### Component 2 — Ingest (`kgs_pipeline/ingest.py`)
- **Tasks 06–11**
- Discovers `lp*.txt` files in `data/raw/` via `discover_raw_files()`.
- Reads all files lazily with `dask.dataframe.read_csv()` using `latin-1` encoding; adds `source_file` column.
- Filters out yearly (`Month=0`) and starting cumulative (`Month=-1`) records via `filter_monthly_records()`.
- Left-joins with lease-level metadata from the archive index via `enrich_with_lease_metadata()`.
- Casts and normalizes column dtypes via `apply_interim_schema()`.
- Writes unified interim Parquet to `data/interim/` partitioned by `LEASE_KID` via `write_interim_parquet()`.
- All helper functions return lazy Dask DataFrames; the only `.compute()` call is inside `write_interim_parquet()`.

### Component 3 — Transform (`kgs_pipeline/transform.py`)
- **Tasks 12–21**
- Loads interim Parquet from `data/interim/` via `load_interim_parquet()`.
- Parses `M-YYYY` strings to `datetime64[ns]` `production_date` via `parse_production_date()`.
- Renames all columns to `snake_case` via `normalize_column_names()`.
- Explodes comma-separated `api_number` to one row per well (`well_id`) via `explode_api_numbers()`.
- Validates physical bounds: negative production → NaN; oil > 50,000 BBL/month → `outlier_flag=True`; adds `unit` column via `validate_physical_bounds()`.
- Deduplicates on `[well_id, production_date, product]`, retaining highest production value, via `deduplicate_records()`.
- Sorts per-well records chronologically and repartitions by `well_id` via `sort_and_repartition()`.
- Writes processed Parquet to `data/processed/` partitioned by `well_id` with fixed `pyarrow.schema()` via `write_processed_parquet()`.
- Zero production preserved as `0.0`; missing data preserved as `NaN` (zero ≠ null distinction).
- Generates `cleaning_report.json` via `generate_cleaning_report()`.
- Single `.compute()` call in `write_processed_parquet()`.

### Component 4 — Features (`kgs_pipeline/features.py`) + Pipeline Orchestrator (`kgs_pipeline/pipeline.py`)
- **Tasks 22–32**
- Loads processed Parquet from `data/processed/` via `load_processed_parquet()`.
- Computes cumulative production (Np/Gp) — monotonically non-decreasing per well — via `compute_cumulative_production()`.
- Computes month-over-month decline rate, clipped to `[-1.0, 10.0]`, with zero-denominator protection, via `compute_decline_rate()`.
- Computes rolling mean and std dev at 3, 6, and 12-month windows via `compute_rolling_features()`.
- Computes lag-1 and lag-3 production features via `compute_lag_features()`.
- Computes `production_year`, `production_month`, `production_quarter`, `months_since_first_prod` via `compute_time_features()`.
- Computes GOR (gas/oil pivot join), `water_cut = NaN`, `wor = NaN` placeholders via `compute_ratio_features()`.
- Builds globally consistent categorical encoding maps and encodes `county`, `operator`, `producing_zone`, `field`, `product` via `build_encoding_maps()` + `encode_categorical_features()`.
- Writes ML-ready feature Parquet to `data/processed/features/` with fixed `pyarrow.schema()` via `write_features_parquet()`.
- Exports sampled feature matrix CSV to `data/processed/features/feature_matrix.csv` via `export_feature_matrix_csv()`.
- Two permitted `.compute()` calls: one in `build_encoding_maps()`, one in `write_features_parquet()`.
- **Pipeline orchestrator** (`kgs_pipeline/pipeline.py`): CLI entry point with `--stages`, `--years`, `--workers` arguments; runs all four stages in sequence; structured logging to `logs/pipeline.log`; saves `data/pipeline_metadata.json`.

## Test File Index

| Test File | Covers |
|-----------|--------|
| `tests/test_acquire.py` | Tasks 01–05: config validation, utility helpers, URL loading, Playwright scraper mocking, parallel orchestrator, idempotency, file integrity |
| `tests/test_ingest.py` | Tasks 06–11: raw file discovery, Dask CSV reading, monthly filtering, metadata join, schema casting, Parquet write, partition correctness, row count reconciliation, schema stability |
| `tests/test_transform.py` | Tasks 12–21: Parquet loading, date parsing, column normalization, API number explosion, physical bounds validation, deduplication idempotency, sort stability, Parquet write, cleaning report, data integrity spot-check, zero production preservation |
| `tests/test_features.py` | Tasks 22–31: cumulative monotonicity, flat shut-in periods, decline rate clip bounds, rolling feature correctness, lag correctness, GOR zero-denominator handling, water cut boundary, categorical encoding consistency, feature column presence, schema stability, lazy Dask evaluation |
| `tests/test_pipeline.py` | Task 32: CLI argument parsing, stage selection, stage failure isolation, metadata JSON output, end-to-end integration |

## Pytest Markers Used

| Marker | Meaning |
|--------|---------|
| `@pytest.mark.unit` | Does not require network access or data files on disk in `data/raw/`, `data/interim/`, `data/processed/`, or `data/processed/features/` |
| `@pytest.mark.integration` | Requires network access or data files in `data/raw/`, `data/interim/`, `data/processed/`, or `data/processed/features/` |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Pydantic `BaseSettings` for config | Type-safe, validated, self-documenting configuration; `ValidationError` on bad values instead of silent bugs |
| Playwright async API + `asyncio.Semaphore(5)` for scraping | Prevents overloading KGS (a public academic resource); Playwright handles dynamic JS-rendered pages that `requests` cannot |
| Dask for all parallel data processing | Lazy evaluation, partition-level parallelism, scalable out-of-core computation across thousands of lease files |
| Partition by `LEASE_KID` in interim, by `well_id` in processed/features | Interim partitioning matches source data granularity; processed/features partitioning co-locates all records for a well, enabling correct per-well cumulative and rolling calculations |
| Zero ≠ NaN distinction | Zeros are valid shut-in measurements; nulls are missing data — ML models must treat them differently, and physical law requires we preserve the distinction |
| `outlier_flag` column instead of dropping | Preserves data for human review; lets ML models learn from or exclude flagged data |
| `pyarrow.schema()` at write time | Prevents schema drift across well partitions when one well has all-null columns and type inference guesses incorrectly |
| Global encoding map in `build_encoding_maps()` + single `.compute()` | Guarantees consistent integer codes across all Dask partitions for ML reproducibility; a per-partition encoding would produce inconsistent codes |
| Single `.compute()` per write step | All pipeline functions remain lazy; computation is deferred until the write step, maximising Dask graph optimisation |
| GOR computed via oil/gas pivot join | GOR is a well-level ratio requiring oil and gas to be aligned on the same row; a simple per-row computation is not possible with the row-per-product schema |
| Decline rate zero-denominator → NaN before clip | Prevents `inf` values from bypassing the clip and entering the feature matrix silently |
