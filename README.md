# KGS Oil & Gas Production Data Pipeline

A high-performance data pipeline for acquiring, ingesting, transforming, and engineering features from Kansas Geological Survey (KGS) oil production data (2020-2025).

## Features

- **Acquire Component**: Web scrapes KGS lease pages and downloads monthly production data using Playwright
- **Ingest Component**: Reads raw .txt files, merges with lease metadata, outputs interim Parquet
- **Transform Component**: Cleans data, standardizes types, explodes wells, validates bounds, outputs processed Parquet partitioned by well_id
- **Features Component**: Engineers time-series features (rolling averages, cumulative production, trends) ready for ML/analytics
- **Parallel Processing**: Uses Dask DataFrames for distributed computation across all components
- **Production-Ready**: Full error handling, logging, type hints, and pytest test coverage

## Architecture

```
Raw KGS Data (web)
    ↓
[Acquire] → raw/*.txt
    ↓
[Ingest] → interim/parquet
    ↓
[Transform] → processed/parquet (partitioned by well_id)
    ↓
[Features] → features/parquet (ML-ready features)
```

## Installation

```bash
# Clone/download the project and enter the directory
cd kgs

# Install dependencies
make install

# Or manually:
pip install -r requirements.txt
```

## Quick Start

### Run the full pipeline end-to-end:
```bash
make run-all
```

### Run individual components:
```bash
make run-acquire      # Web scrape KGS data
make run-ingest       # Read and merge raw data
make run-transform    # Clean and standardize
make run-features     # Engineer ML features
```

### Run tests:
```bash
make test             # Run all tests with coverage report
```

### Code quality:
```bash
make lint             # Auto-fix linting issues
make type             # Type check with mypy
```

## Project Structure

```
kgs/
├── data/
│   ├── raw/           ← Downloaded .txt files from KGS (acquire output)
│   ├── interim/       ← Parquet (ingest output)
│   ├── processed/     ← Parquet partitioned by well_id (transform output)
│   ├── external/      ← oil_leases_2020_present.txt, KGS data dictionaries
│
├── kgs_pipeline/
│   ├── __init__.py
│   ├── config.py      ← Centralized configuration
│   ├── acquire.py     ← Web scraping + async concurrency
│   ├── ingest.py      ← Read raw files + metadata merge
│   ├── transform.py   ← Data cleaning + standardization
│   ├── features.py    ← Feature engineering
│
├── tests/
│   ├── test_acquire.py
│   ├── test_ingest.py
│   ├── test_transform.py
│   ├── test_features.py
│
├── requirements.txt
├── Makefile
└── README.md
```

## Component Details

### Acquire

**Function**: `run_acquire_pipeline(lease_index_path, output_dir)`

- Reads lease index file (`oil_leases_2020_present.txt`)
- Launches Playwright browser to scrape each lease's KGS web page
- Uses asyncio + semaphore for rate-limited concurrency (default: 5 concurrent)
- Downloads monthly production data as .txt files
- Saves to `data/raw/`
- **Error handling**: Logs warnings for missing pages; returns None on failure
- **Parallel**: Dask delayed tasks + asyncio semaphore

### Ingest

**Function**: `run_ingest_pipeline(raw_dir, lease_index_path, interim_dir)`

- Reads all `lp*.txt` files from raw directory
- Filters to monthly records only (removes yearly and starting cumulative)
- Reads lease index and deduplicates by LEASE_KID
- Left-joins production data with metadata
- Outputs Parquet partitioned by LEASE_KID to `data/interim/`
- **Error handling**: Raises FileNotFoundError if required columns missing
- **Parallel**: Dask lazy evaluation

### Transform

**Function**: `run_transform_pipeline(interim_dir, processed_dir)`

1. **Parse dates**: Convert "M-YYYY" format to datetime
2. **Rename/cast columns**: Snake_case, strip whitespace, type conversion
3. **Explode API numbers**: One row per well (separate comma-separated API numbers)
4. **Validate physical bounds**: 
   - Negative production → NaN
   - Oil outliers (>50k BBL/month) → flag
   - Coordinates outside Kansas → NaN
5. **Deduplicate**: Remove duplicate well-date-product records
6. **Add units**: BBL for oil, MCF for gas
7. **Sort by well and date**: Repartition by well_id, sort chronologically
8. Outputs Parquet partitioned by well_id to `data/processed/`
- **Error handling**: Raises KeyError for missing mandatory columns
- **Parallel**: Dask map_partitions + set_index

### Features

**Function**: `run_features_pipeline(processed_dir, features_dir)`

1. **Filter 2020-2025**: Temporal filter
2. **Aggregate by well-month**: Sum, mean, std, count per well-month-product
3. **Rolling averages**: 12-month rolling average production
4. **Cumulative production**: Cumulative sum per well
5. **Production trend**: Increasing/decreasing/stable indicator
6. **Well lifetime**: Months of production per well
7. **Standardize**: Z-score normalization of numeric features
8. Outputs Parquet to `data/features/` (ready for ML)
- **Error handling**: Raises KeyError for missing columns
- **Parallel**: Dask groupby + map_partitions

## Key Configuration

See `kgs_pipeline/config.py`:

- `KGS_BASE_URL`: Base lease page URL
- `SCRAPE_CONCURRENCY`: Max concurrent Playwright sessions (default: 5)
- `SCRAPE_TIMEOUT_MS`: Playwright timeout (default: 30,000 ms)
- `MAX_REALISTIC_OIL_BBL_PER_MONTH`: Outlier threshold (default: 50,000 BBL)

## Data Formats

### Input
- **oil_leases_2020_present.txt**: CSV with LEASE_KID, URL, and metadata columns
- **KGS web pages**: HTML pages with monthly production data tables

### Output Parquet Schema (Processed)

| Column | Type | Notes |
|--------|------|-------|
| lease_kid | string | |
| well_id | string | Exploded from API_NUMBER |
| production_date | timestamp | |
| product | string | "O" (oil) or "G" (gas) |
| production | float64 | |
| unit | string | "BBL" or "MCF" |
| operator | string | |
| field_name | string | |
| county | string | |
| latitude | float64 | |
| longitude | float64 | |
| outlier_flag | bool | True if production exceeds threshold |
| source_file | string | Original raw .txt filename |

## Testing

All components include comprehensive pytest test cases covering:

- **Happy path**: Function succeeds with valid input
- **Error handling**: FileNotFoundError, KeyError, TypeError
- **Data validation**: Correct aggregations, deduplication, filtering
- **Edge cases**: Empty data, missing columns, null values

Run tests:
```bash
pytest tests -v --cov=kgs_pipeline
```

## Performance Considerations

- **Dask lazy evaluation**: All DataFrame operations are lazy until `.compute()` or `.to_parquet()`
- **Partitioning**: Transform output partitioned by well_id for efficient downstream queries
- **Async concurrency**: Playwright sessions pooled with asyncio semaphore (configurable)
- **Parquet format**: Columnar storage for efficient analytics workloads

## Dependencies

- **pandas** (2.0+): Data manipulation
- **dask** (2024.1+): Lazy distributed DataFrames
- **pyarrow** (14.0+): Parquet serialization
- **playwright** (1.40+): Browser automation for web scraping
- **pytest** (7.4+): Testing framework
- **ruff** (0.1+): Linting
- **mypy** (1.7+): Type checking

## License

Open source (license file TBD)

## Notes for ML/Analytics Consumers

The processed features are ready for:

- **Time-series forecasting** (production trends)
- **Anomaly detection** (outlier flags, domain constraints)
- **Clustering** (operator, field, county groupings)
- **Regression/classification** (production prediction, decline curves)
- **Geospatial analysis** (latitude/longitude coordinates)

All features are:
- ✅ Validated within physical domain constraints
- ✅ Deduplicated and standardized
- ✅ Partitioned for efficient querying
- ✅ Z-score normalized where applicable
- ✅ Timestamped and well-identified for reproducibility
