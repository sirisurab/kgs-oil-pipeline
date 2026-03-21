# KGS Oil & Gas Well Data Pipeline

A data engineering pipeline for processing Kansas Geological Survey (KGS) oil and gas well production data. The pipeline scrapes lease-level monthly data from the KGS portal, ingests and transforms it into a structured format, and engineers features for modeling.

## Project Structure

```
kgs/
├── kgs_pipeline/          # Main pipeline package
│   ├── __init__.py
│   ├── config.py          # Project configuration and paths
│   ├── acquire.py         # Web scraping and data acquisition
│   ├── ingest.py          # Raw data reading and parsing
│   ├── transform.py       # Data cleaning and preprocessing
│   └── features.py        # Feature engineering
├── tests/                 # Test suite
│   ├── test_acquire.py
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_features.py
├── data/                  # Data directories
│   ├── raw/               # Raw .txt files from KGS portal
│   ├── interim/           # Interim Parquet datasets
│   ├── processed/         # Processed well-partitioned data
│   └── external/          # Oil leases lookup file
├── requirements.txt       # Python dependencies
├── Makefile               # Convenience commands
└── README.md              # This file
```

## Installation

### Prerequisites
- Python 3.11+
- pip

### Setup
```bash
# Clone or enter the project directory
cd kgs

# Install dependencies
make install

# Or manually:
pip install -r requirements.txt
playwright install
```

## Usage

### Running Tests
```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_acquire.py -v

# Run with coverage
pytest tests/ --cov=kgs_pipeline
```

### Running Linters
```bash
# Run ruff and mypy
make lint

# Ruff only
ruff check kgs_pipeline tests

# MyPy only
mypy kgs_pipeline --ignore-missing-imports
```

### Pipeline Stages

The pipeline consists of four main stages:

#### 1. Acquire (`kgs_pipeline/acquire.py`)
- Scrapes lease URLs from the KGS portal
- Downloads per-lease monthly production .txt files
- Uses Playwright for browser automation and Dask for parallelization

**Key Functions:**
- `extract_lease_urls(leases_file)` - Parse lease IDs and URLs
- `scrape_lease_page(lease_url, output_dir, semaphore, playwright_instance)` - Download data for one lease
- `run_scrape_pipeline(leases_file, output_dir)` - Orchestrate full scraping

**Output:** Raw .txt files in `data/raw/`

#### 2. Ingest (`kgs_pipeline/ingest.py`)
- Reads all raw .txt files into a Dask DataFrame
- Parses month_year field and filters non-monthly records
- Explodes API numbers to well level
- Normalizes column names

**Key Functions:**
- `discover_raw_files(raw_dir)` - Find .txt files
- `read_raw_files(file_paths)` - Read and combine into Dask DataFrame
- `parse_month_year(ddf)` - Parse production dates
- `explode_api_numbers(ddf)` - Expand to well level
- `run_ingest_pipeline(raw_dir, output_dir)` - Orchestrate ingestion

**Output:** Partitioned Parquet at `data/interim/kgs_monthly_raw.parquet`

#### 3. Transform (`kgs_pipeline/transform.py`)
- Casts columns to correct types
- Deduplicates on (well_id, production_date, product)
- Validates physical bounds (coordinates, production rates)
- Pivots product column to oil_bbl and gas_mcf columns
- Fills monthly date gaps per well
- Computes cumulative production

**Key Functions:**
- `read_interim_parquet(interim_dir)` - Load interim data
- `cast_column_types(ddf)` - Type conversion
- `deduplicate(ddf)` - Remove duplicates
- `validate_physical_bounds(ddf)` - Domain validation
- `pivot_product_columns(ddf)` - Long to wide format
- `fill_date_gaps(ddf)` - Complete monthly time series
- `compute_cumulative_production(ddf)` - Cumulative sums
- `run_transform_pipeline(interim_dir, output_dir)` - Orchestrate transformation

**Output:** Well-partitioned Parquet at `data/processed/wells`

#### 4. Features (`kgs_pipeline/features.py`)
- Computes time-based features (months since first production, production phase)
- Computes rolling production statistics (3, 6, 12-month rolling means)
- Computes decline rates and gas-oil ratio (GOR)
- Label-encodes categorical columns
- Saves encoding map for inference

**Key Functions:**
- `read_processed_parquet(processed_dir)` - Load processed data
- `compute_time_features(ddf)` - Time-based features
- `compute_rolling_features(ddf)` - Rolling statistics
- `compute_decline_and_gor(ddf)` - Decline rates and GOR
- `encode_categorical_features(ddf, encoding_map=None)` - Categorical encoding
- `save_encoding_map(encoding_map, output_dir)` - Persist encoding map
- `write_feature_parquet(ddf, output_dir)` - Write output
- `run_features_pipeline(processed_dir, output_dir)` - Orchestrate engineering

**Output:** Feature-engineered Parquet at `data/processed/features`

## Configuration

Edit `kgs_pipeline/config.py` to customize:
- Data directory paths
- Number of concurrent web scrapes (default: 5)

## Dependencies

Core libraries:
- **pandas**: Data manipulation
- **dask**: Distributed dataframe operations
- **playwright**: Browser automation for web scraping
- **pytest**: Unit and integration testing
- **ruff**: Fast Python linter
- **mypy**: Static type checking

## Testing

All components have comprehensive test suites with unit and integration tests:

```bash
# Run all tests
pytest tests/ -v

# Run specific test class
pytest tests/test_transform.py::TestPivotProductColumns -v

# Run with markers
pytest tests/ -m unit         # Unit tests only
pytest tests/ -m integration  # Integration tests only
```

## Logging

The pipeline uses Python's standard logging module. To enable logging:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Error Handling

- **ScrapeError**: Raised when critical scraping failures occur (e.g., button not found)
- **FileNotFoundError**: When required input files don't exist
- **KeyError**: When required columns are missing
- **ValueError**: When data validation fails

## Architecture Notes

- **Parallelization**: Dask is used for parallel processing of large datasets
- **Partitioning**: Processed and feature datasets are partitioned by well_id for efficient queries
- **Idempotency**: The acquire step checks for existing files and skips re-downloading
- **Type Safety**: MyPy type hints throughout the codebase
- **Lazy Evaluation**: Dask operations are lazy; computation is triggered only at write time

## Contributing

When adding new features:
1. Update the appropriate module (acquire.py, ingest.py, etc.)
2. Add corresponding test cases in tests/
3. Run linters: `make lint`
4. Run tests: `make test`
5. Ensure all tests pass before committing

## License

See LICENSE file for details.
