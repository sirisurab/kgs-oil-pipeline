# Task Index

## KGS Oil Production Data Pipeline — Component Task Specifications

This index lists all task specification files for the KGS 2020–2025 oil production data pipeline.  
Each file contains all tasks, design decisions, and test cases for one pipeline component.  
Files are listed in pipeline execution order.

---

| # | Component      | Task File                          | Description                                                                 |
|---|----------------|------------------------------------|-----------------------------------------------------------------------------|
| 1 | **Acquire**    | `tasks/acquire_tasks.md`           | Web-scraping workflow to download per-lease monthly data files from KGS     |
| 2 | **Ingest**     | `tasks/ingest_tasks.md`            | Read, parse, and combine raw `.txt` files into interim partitioned Parquet  |
| 3 | **Transform**  | `tasks/transform_tasks.md`         | Clean, validate, pivot, gap-fill, and compute cumulative production Parquet |
| 4 | **Features**   | `tasks/features_tasks.md`          | Engineer ML-ready features and write final feature Parquet per well         |

---

## Pipeline Overview

```
kgs/data/external/oil_leases_2020_present.txt
        │
        ▼
[ 1. ACQUIRE ]  acquire.py
        │  Async Playwright scraper (asyncio.Semaphore(5))
        │  Dask-parallelised across lease URL batches
        │  Downloads per-lease .txt files
        ▼
kgs/data/raw/lp*.txt   (one file per lease)
        │
        ▼
[ 2. INGEST ]   ingest.py
        │  Dask read_csv across all raw .txt files
        │  Parse MONTH-YEAR → datetime, filter yearly/cumulative rows
        │  Explode comma-separated API_NUMBER → one row per well per month
        ▼
kgs/data/interim/kgs_monthly_raw.parquet/
        │
        ▼
[ 3. TRANSFORM ]  transform.py
        │  Cast dtypes, deduplicate
        │  Physical bounds validation (negative production, rate ceiling, geo bounds)
        │  Pivot PRODUCT column → oil_bbl + gas_mcf wide format
        │  Fill monthly date gaps per well (NaN for missing, preserve zeros)
        │  Compute cumulative_oil_bbl, cumulative_gas_mcf per well
        ▼
kgs/data/processed/wells/   (Parquet partitioned by well_id)
        │
        ▼
[ 4. FEATURES ]  features.py
        │  Time features: months_since_first_prod, producing_months_count, production_phase
        │  Rolling means: oil/gas 3-month, 6-month, 12-month windows per well
        │  Decline rates: month-on-month oil and gas decline per well
        │  GOR proxy: gas_mcf / oil_bbl per row
        │  Label-encode: county, field, producing_zone, operator → *_encoded int32
        │  Save encoding_map.json sidecar for inference
        ▼
kgs/data/processed/features/   (Parquet partitioned by well_id + encoding_map.json)
```

---

## Task Summary

| Task | Module            | Function / Artefact                          | Component  |
|------|-------------------|----------------------------------------------|------------|
| 01   | config.py / .gitignore | Project scaffolding, paths, .gitignore  | Acquire    |
| 02   | acquire.py        | `extract_lease_urls()`                       | Acquire    |
| 03   | acquire.py        | `scrape_lease_page()`                        | Acquire    |
| 04   | acquire.py        | `run_scrape_pipeline()`                      | Acquire    |
| 05   | ingest.py         | `discover_raw_files()`                       | Ingest     |
| 06   | ingest.py         | `read_raw_files()`                           | Ingest     |
| 07   | ingest.py         | `parse_month_year()`                         | Ingest     |
| 08   | ingest.py         | `explode_api_numbers()`                      | Ingest     |
| 09   | ingest.py         | `write_interim_parquet()`                    | Ingest     |
| 10   | ingest.py         | `run_ingest_pipeline()`                      | Ingest     |
| 11   | transform.py      | `read_interim_parquet()`                     | Transform  |
| 12   | transform.py      | `cast_column_types()`                        | Transform  |
| 13   | transform.py      | `deduplicate()`                              | Transform  |
| 14   | transform.py      | `validate_physical_bounds()`                 | Transform  |
| 15   | transform.py      | `pivot_product_columns()`                    | Transform  |
| 16   | transform.py      | `fill_date_gaps()`                           | Transform  |
| 17   | transform.py      | `compute_cumulative_production()`            | Transform  |
| 18   | transform.py      | `write_processed_parquet()`                  | Transform  |
| 19   | transform.py      | `run_transform_pipeline()`                   | Transform  |
| 20   | features.py       | `read_processed_parquet()`                   | Features   |
| 21   | features.py       | `compute_time_features()`                    | Features   |
| 22   | features.py       | `compute_rolling_features()`                 | Features   |
| 23   | features.py       | `compute_decline_and_gor()`                  | Features   |
| 24   | features.py       | `encode_categorical_features()`              | Features   |
| 25   | features.py       | `save_encoding_map()` / `load_encoding_map()`| Features   |
| 26   | features.py       | `write_feature_parquet()`                    | Features   |
| 27   | features.py       | `run_features_pipeline()`                    | Features   |

---

## Key Design Decisions (Cross-Cutting)

- **Lazy Dask evaluation**: `.compute()` is never called inside pipeline functions. The Dask graph is built lazily across all steps and materialized only at `to_parquet` writes. One exception: `encode_categorical_features` calls `.compute()` once for a metadata aggregation to build the encoding map.
- **Parquet storage**: Interim data at `data/interim/`, processed well data at `data/processed/wells/`, feature data at `data/processed/features/`. All use Snappy compression and are partitioned by `well_id`.
- **Well identity**: `well_id` = individual API number (exploded from the comma-separated `API_NUMBER` lease field). This is the canonical identifier throughout the pipeline.
- **Zero vs NaN**: Zero production (`0.0`) is a valid reported measurement and is preserved. `NaN` means the data is missing. This distinction is enforced throughout transform and features.
- **Rate limiting**: Playwright scraping is limited to 5 concurrent requests via `asyncio.Semaphore(5)`.
- **Idempotency**: All download and write operations are idempotent — re-running does not duplicate data.
- **Test markers**: `@pytest.mark.unit` for pure logic tests; `@pytest.mark.integration` for tests requiring network access or files on disk under `kgs/data/`.
