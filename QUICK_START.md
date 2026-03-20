# Quick Start Guide

## Installation

```bash
cd kgs
pip install -r requirements.txt
```

## Running Tests

### All Tests
```bash
pytest tests/ -v
```

### By Component
```bash
# Acquire tests
pytest tests/test_acquire.py -v

# Ingest tests
pytest tests/test_ingest.py -v

# Transform tests
pytest tests/test_transform.py -v

# Features tests
pytest tests/test_features.py -v
```

### By Type
```bash
# Unit tests only
pytest tests/ -v -m unit

# Integration tests only
pytest tests/ -v -m integration
```

### With Coverage
```bash
pytest tests/ --cov=kgs_pipeline --cov-report=html
```

## Code Quality

### Format Code
```bash
make format
```

### Lint Code
```bash
make lint
```

### Clean Artifacts
```bash
make clean
```

## Using the Pipeline

### Acquire Phase
```python
from kgs_pipeline.acquire import run_scrape_pipeline
from kgs_pipeline.config import OIL_LEASES_FILE, RAW_DATA_DIR

result = run_scrape_pipeline(OIL_LEASES_FILE, RAW_DATA_DIR)
print(f"Downloaded: {len(result['downloaded'])} files")
print(f"Failed: {len(result['failed'])} leases")
```

### Ingest Phase
```python
from kgs_pipeline.ingest import run_ingest_pipeline
from kgs_pipeline.config import RAW_DATA_DIR, INTERIM_DATA_DIR

output = run_ingest_pipeline(RAW_DATA_DIR, INTERIM_DATA_DIR)
print(f"Output: {output}")
```

### Transform Phase
```python
from kgs_pipeline.transform import run_transform_pipeline
from kgs_pipeline.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR

output = run_transform_pipeline(INTERIM_DATA_DIR, PROCESSED_DATA_DIR)
print(f"Output: {output}")
```

### Features Phase
```python
from kgs_pipeline.features import run_features_pipeline
from kgs_pipeline.config import PROCESSED_DATA_DIR, FEATURES_DATA_DIR

output = run_features_pipeline(PROCESSED_DATA_DIR, FEATURES_DATA_DIR)
print(f"Output: {output}")
```

## File Structure

```
kgs/
├── kgs_pipeline/          # Main code
│   ├── config.py
│   ├── acquire.py
│   ├── ingest.py
│   ├── transform.py
│   └── features.py
├── tests/                 # Test files
│   ├── test_acquire.py
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_features.py
├── data/                  # Data directories (auto-created)
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── external/
├── requirements.txt
├── pytest.ini
├── Makefile
└── README.md
```

## Configuration

All paths and settings are in `kgs_pipeline/config.py`:

```python
PROJECT_ROOT        # /path/to/kgs
RAW_DATA_DIR        # /path/to/kgs/data/raw
INTERIM_DATA_DIR    # /path/to/kgs/data/interim
PROCESSED_DATA_DIR  # /path/to/kgs/data/processed
FEATURES_DATA_DIR   # /path/to/kgs/data/processed/features
MAX_CONCURRENT_SCRAPES  # 5 (for web scraping)
```

## Common Tasks

### Run a single test
```bash
pytest tests/test_acquire.py::TestExtractLeaseUrls::test_extract_lease_urls_basic -v
```

### Run tests matching a pattern
```bash
pytest tests/ -k "parse_month_year" -v
```

### Generate coverage report
```bash
pytest tests/ --cov=kgs_pipeline --cov-report=html
open htmlcov/index.html
```

### Check code with ruff
```bash
ruff check kgs_pipeline/ tests/
```

### Check types with mypy
```bash
mypy kgs_pipeline/ --strict
```

## Troubleshooting

### Import errors
Run the import test:
```bash
python test_imports.py
```

### Missing dependencies
Reinstall requirements:
```bash
pip install -r requirements.txt --upgrade
```

### Dask worker issues
Check Dask configuration in pipeline functions - they use threads scheduler by default.

### Playwright issues
Install Playwright browsers:
```bash
playwright install chromium
```

## Key Concepts

**Lazy Evaluation**: All pipelines return Dask DataFrames. Use `.compute()` only when needed.

**Partitioning**: Processed and feature data are partitioned by well_id for distributed processing.

**Idempotency**: Pipelines can be safely re-run (downloads skip existing files, writes overwrite).

**Logging**: All operations logged; use Python logging to control verbosity.

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Performance Tips

1. **Partitioning**: More partitions = better parallelism (but more I/O)
2. **Lazy evaluation**: Avoid calling `.compute()` until final results
3. **Dask scheduler**: Use "threads" for I/O-heavy, "processes" for CPU-heavy
4. **Memory**: Monitor with `ddf.memory_usage_per_partition()`
