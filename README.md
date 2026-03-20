# KGS Oil & Gas Well Data Pipeline

A comprehensive data pipeline for processing Kansas Geological Survey (KGS) oil and gas well production data. The pipeline orchestrates data acquisition, ingestion, transformation, and feature engineering using Dask for distributed processing.

## Project Structure

```
kgs/
├── data/
│   ├── raw/                 # Downloaded lease data files (.txt)
│   ├── interim/             # Ingested and parsed data (Parquet)
│   ├── processed/           # Transformed well-level data (Parquet)
│   ├── external/            # Oil leases archive file
│   └── features/            # Engineered features for modeling
├── kgs_pipeline/
│   ├── __init__.py
│   ├── config.py            # Configuration and paths
│   ├── acquire.py           # Data scraping and downloading
│   ├── ingest.py            # Raw file reading and parsing
│   ├── transform.py         # Data cleaning and transformation
│   └── features.py          # Feature engineering
├── tests/
│   ├── test_acquire.py
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_features.py
└── README.md
```

## Components

### 1. Acquire (`kgs_pipeline/acquire.py`)

Orchestrates the scraping and downloading of KGS lease data from web pages.

**Functions:**
- `extract_lease_urls(leases_file)` - Extract unique lease URLs from archive file
- `scrape_lease_page(lease_url, output_dir, semaphore, playwright_instance)` - Async scraper for single lease
- `run_scrape_pipeline(leases_file, output_dir)` - Parallel orchestration using Dask

**Output:** Downloaded `.txt` files in `data/raw/`

### 2. Ingest (`kgs_pipeline/ingest.py`)

Reads raw per-lease `.txt` files and converts to a unified Dask DataFrame.

**Functions:**
- `discover_raw_files(raw_dir)` - Scan for available raw files
- `read_raw_files(file_paths)` - Read all files into Dask DataFrame
- `parse_month_year(ddf)` - Parse month-year and filter non-monthly records
- `explode_api_numbers(ddf)` - Transform lease-level rows to well-level rows
- `write_interim_parquet(ddf, output_dir)` - Write to Parquet
- `run_ingest_pipeline(raw_dir, output_dir)` - Top-level entry point

**Output:** Parquet dataset at `data/interim/kgs_monthly_raw.parquet`

### 3. Transform (`kgs_pipeline/transform.py`)

Cleans, validates, and structures data into well-month grain.

**Functions:**
- `read_interim_parquet(interim_dir)` - Read ingest output
- `cast_column_types(ddf)` - Type casting with coercion
- `deduplicate(ddf)` - Remove duplicates by (well_id, production_date, product)
- `validate_physical_bounds(ddf)` - Domain-specific validation
- `pivot_product_columns(ddf)` - Convert to wide format (oil, gas columns)
- `fill_date_gaps(ddf)` - Insert missing months
- `compute_cumulative_production(ddf)` - Cumulative sums per well
- `write_processed_parquet(ddf, output_dir)` - Write partitioned by well_id
- `run_transform_pipeline(interim_dir, output_dir)` - Top-level entry point

**Output:** Parquet dataset at `data/processed/wells/` (partitioned by well_id)

### 4. Features (`kgs_pipeline/features.py`)

Computes ML-ready features for modeling.

**Functions:**
- `read_processed_parquet(processed_dir)` - Read transform output
- `compute_time_features(ddf)` - Months since first prod, producing months, phase
- `compute_rolling_features(ddf)` - 3, 6, 12-month rolling means
- `compute_decline_and_gor(ddf)` - Month-on-month decline rates and gas-oil ratio
- `encode_categorical_features(ddf, encoding_map)` - Label encoding with optional map
- `save_encoding_map(encoding_map, output_dir)` - Persist encoding map to JSON
- `load_encoding_map(output_dir)` - Load encoding map from JSON
- `write_feature_parquet(ddf, output_dir)` - Write partitioned features
- `run_features_pipeline(processed_dir, output_dir)` - Top-level entry point

**Output:** Feature Parquet dataset at `data/features/` with `encoding_map.json`

## Installation

```bash
pip install -r requirements.txt
```

## Running Tests

```bash
pytest tests/
```

Run specific component tests:

```bash
pytest tests/test_acquire.py -v
pytest tests/test_ingest.py -v
pytest tests/test_transform.py -v
pytest tests/test_features.py -v
```

## Configuration

All paths and settings are defined in `kgs_pipeline/config.py`:

```python
PROJECT_ROOT  # Root directory
RAW_DATA_DIR  # data/raw/
INTERIM_DATA_DIR  # data/interim/
PROCESSED_DATA_DIR  # data/processed/
FEATURES_DATA_DIR  # data/processed/features/
MAX_CONCURRENT_SCRAPES  # Default: 5
```

## Key Design Principles

1. **Lazy Evaluation**: All pipelines use Dask and return lazy DataFrames; `.compute()` is only called when necessary
2. **Partitioning**: Data is partitioned by well_id for efficient distributed processing
3. **Idempotency**: Pipelines can be re-run safely (downloads skip existing files, writes use overwrite mode)
4. **Logging**: All operations are logged at INFO/WARNING/ERROR levels
5. **Error Handling**: Explicit error types (ScrapeError, etc.) and graceful degradation
6. **Testing**: Comprehensive unit and integration tests with mocking where appropriate

## Data Schema

### Input (Raw Lease Files)
- LEASE_KID, URL, MONTH_YEAR, API_NUMBER, PRODUCTION, PRODUCT, etc.

### Interim (After Ingest)
- Lease-level rows exploded to well-level by API_NUMBER
- MONTH_YEAR parsed to production_date (datetime)
- Columns normalized (lowercase, no hyphens)

### Processed (After Transform)
- Well-month grain
- oil_bbl, gas_mcf columns (pivoted by product)
- cumulative_oil_bbl, cumulative_gas_mcf
- Date gaps filled with forward-filled metadata

### Features (After Feature Engineering)
- All features from transform plus:
- months_since_first_prod, producing_months_count, production_phase
- oil_bbl_roll3/6/12, gas_mcf_roll3/6/12
- oil_decline_rate_mom, gas_decline_rate_mom, gor
- county_encoded, field_encoded, producing_zone_encoded, operator_encoded

## Performance Considerations

- Dask uses threads scheduler for I/O-heavy operations (reading Parquet, writing CSV)
- Max 5 concurrent browser instances for web scraping (configurable)
- Intermediate DataFrames are not computed; only final outputs are materialized
- Partitioning by well_id balances load for distributed processing

## Future Enhancements

- Add caching layer for intermediate outputs
- Support incremental updates (only scrape/ingest new leases)
- Add data quality reporting and anomaly detection
- Implement streaming aggregations for large datasets
