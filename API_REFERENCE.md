# API Reference

## kgs_pipeline.config

Configuration and path constants.

### Variables
```python
PROJECT_ROOT: Path          # Root project directory
RAW_DATA_DIR: Path          # data/raw/
INTERIM_DATA_DIR: Path      # data/interim/
PROCESSED_DATA_DIR: Path    # data/processed/
FEATURES_DATA_DIR: Path     # data/processed/features/
EXTERNAL_DATA_DIR: Path     # data/external/
REFERENCES_DIR: Path        # references/
OIL_LEASES_FILE: Path       # data/external/oil_leases_2020_present.txt
MAX_CONCURRENT_SCRAPES: int # Default: 5
```

---

## kgs_pipeline.acquire

Web scraping and data acquisition module.

### Exceptions
```python
class ScrapeError(Exception)
    """Raised when lease page scraping fails unrecoverably."""
```

### Functions

#### extract_lease_urls
```python
def extract_lease_urls(leases_file: Path) -> pd.DataFrame:
    """
    Extract unique lease URLs from archive file.
    
    Returns:
        DataFrame with columns ["lease_kid", "url"]
    
    Raises:
        FileNotFoundError: If leases_file doesn't exist
        KeyError: If URL column missing
    """
```

#### scrape_lease_page
```python
async def scrape_lease_page(
    lease_url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    playwright_instance
) -> Optional[Path]:
    """
    Async scraper for single lease page.
    
    Returns:
        Path to downloaded .txt file, or None
    
    Raises:
        ScrapeError: If "Save Monthly Data to File" button not found
    """
```

#### run_scrape_pipeline
```python
def run_scrape_pipeline(leases_file: Path, output_dir: Path) -> dict:
    """
    Orchestrate parallel scraping with Dask.
    
    Returns:
        Dict with keys:
        - "downloaded": list[Path]
        - "failed": list[str] (URLs)
        - "skipped": list[Path]
        - "total_leases": int
    
    Raises:
        FileNotFoundError: If leases_file doesn't exist
    """
```

---

## kgs_pipeline.ingest

Raw file ingestion module.

### Functions

#### discover_raw_files
```python
def discover_raw_files(raw_dir: Path) -> list[Path]:
    """
    Discover all .txt files in raw_dir.
    
    Returns:
        Sorted list of Path objects
    
    Raises:
        FileNotFoundError: If raw_dir doesn't exist
    """
```

#### read_raw_files
```python
def read_raw_files(file_paths: list[Path]) -> dd.DataFrame:
    """
    Read all .txt files into Dask DataFrame.
    
    Features:
    - Normalizes column names (lowercase, no hyphens)
    - Adds source_file column
    
    Returns:
        Dask DataFrame
    
    Raises:
        ValueError: If file_paths is empty
    """
```

#### parse_month_year
```python
def parse_month_year(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Parse month_year to production_date (datetime).
    
    Filters out non-monthly records:
    - Month = 0 (yearly aggregates)
    - Month = -1 (cumulative)
    
    Returns:
        Dask DataFrame with production_date column (datetime64[ns])
    """
```

#### explode_api_numbers
```python
def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Transform lease-level to well-level by exploding API numbers.
    
    - Splits comma-separated api_number column
    - Renames api_number to well_id
    
    Returns:
        Dask DataFrame (well-level grain)
    
    Raises:
        KeyError: If api_number column missing
    """
```

#### write_interim_parquet
```python
def write_interim_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write to kgs_monthly_raw.parquet with snappy compression.
    
    Returns:
        Path to output parquet directory
    """
```

#### run_ingest_pipeline
```python
def run_ingest_pipeline(raw_dir: Path, output_dir: Path) -> Optional[Path]:
    """
    Top-level ingest orchestrator.
    
    Pipeline: discover → read → parse → explode → write
    
    Returns:
        Path to output parquet, or None if no files discovered
    """
```

---

## kgs_pipeline.transform

Data cleaning and transformation module.

### Functions

#### read_interim_parquet
```python
def read_interim_parquet(interim_dir: Path) -> dd.DataFrame:
    """
    Read kgs_monthly_raw.parquet from interim directory.
    
    Returns:
        Dask DataFrame
    
    Raises:
        FileNotFoundError: If parquet doesn't exist
    """
```

#### cast_column_types
```python
def cast_column_types(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Cast columns to correct types.
    
    Numeric (float64):
    - latitude, longitude, production, wells
    
    String (stripped):
    - well_id, lease_kid, county, operator, etc.
    
    Datetime (datetime64[ns]):
    - production_date
    
    Returns:
        Dask DataFrame with correct types
    """
```

#### deduplicate
```python
def deduplicate(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Remove duplicates by (well_id, production_date, product).
    
    Returns:
        Dask DataFrame (deduplicated)
    
    Raises:
        KeyError: If required columns missing
    """
```

#### validate_physical_bounds
```python
def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Enforce domain-specific constraints.
    
    Actions:
    - Negative production → NaN
    - Oil > 50,000 BBL → is_suspect_rate = True
    - Latitude outside [35.0, 40.5] → NaN
    - Longitude outside [-102.5, -94.5] → NaN
    
    Returns:
        Dask DataFrame with validated columns
    """
```

#### pivot_product_columns
```python
def pivot_product_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Transform long format (product column) to wide format.
    
    Output columns:
    - oil_bbl (from product = "O")
    - gas_mcf (from product = "G")
    
    Returns:
        Dask DataFrame with pivoted columns
    """
```

#### fill_date_gaps
```python
def fill_date_gaps(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Fill missing monthly dates per well.
    
    - Inserts missing months with NaN production
    - Forward-fills metadata columns
    
    Returns:
        Dask DataFrame with complete monthly coverage
    """
```

#### compute_cumulative_production
```python
def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute cumulative sums per well.
    
    Output columns:
    - cumulative_oil_bbl
    - cumulative_gas_mcf
    
    Returns:
        Dask DataFrame with cumulative columns
    """
```

#### write_processed_parquet
```python
def write_processed_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write to wells/ partitioned by well_id.
    
    Returns:
        Path to wells directory
    """
```

#### run_transform_pipeline
```python
def run_transform_pipeline(interim_dir: Path, output_dir: Path) -> Path:
    """
    Top-level transform orchestrator.
    
    Pipeline: read → cast → dedup → validate → pivot → 
              fill_gaps → cumulative → write
    
    Returns:
        Path to processed output directory
    """
```

---

## kgs_pipeline.features

Feature engineering module.

### Functions

#### read_processed_parquet
```python
def read_processed_parquet(processed_dir: Path) -> dd.DataFrame:
    """
    Read wells/ parquet dataset.
    
    Returns:
        Dask DataFrame
    
    Raises:
        FileNotFoundError: If wells directory missing
        ValueError: If no parquet files found
    """
```

#### compute_time_features
```python
def compute_time_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute time-based features per well.
    
    Output columns:
    - months_since_first_prod (float)
    - producing_months_count (cumulative count)
    - production_phase ("early", "mid", "late")
    
    Returns:
        Dask DataFrame with time features
    """
```

#### compute_rolling_features
```python
def compute_rolling_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute rolling mean production statistics.
    
    Output columns:
    - oil_bbl_roll3, oil_bbl_roll6, oil_bbl_roll12
    - gas_mcf_roll3, gas_mcf_roll6, gas_mcf_roll12
    
    Returns:
        Dask DataFrame with rolling features
    """
```

#### compute_decline_and_gor
```python
def compute_decline_and_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute month-on-month decline rates and gas-oil ratio.
    
    Output columns:
    - oil_decline_rate_mom (fractional change)
    - gas_decline_rate_mom (fractional change)
    - gor (gas-oil ratio: gas_mcf / oil_bbl)
    
    Returns:
        Dask DataFrame with decline and GOR features
    """
```

#### encode_categorical_features
```python
def encode_categorical_features(
    ddf: dd.DataFrame,
    encoding_map: Optional[dict] = None,
) -> Tuple[dd.DataFrame, dict]:
    """
    Label-encode categorical columns.
    
    Encoded columns:
    - county_encoded
    - field_encoded
    - producing_zone_encoded
    - operator_encoded
    
    Args:
        encoding_map: Optional pre-computed encoding dict
    
    Returns:
        (Dask DataFrame with encoded columns, encoding_map dict)
    """
```

#### save_encoding_map
```python
def save_encoding_map(encoding_map: dict, output_dir: Path) -> Path:
    """
    Save encoding map to JSON.
    
    Returns:
        Path to encoding_map.json
    """
```

#### load_encoding_map
```python
def load_encoding_map(output_dir: Path) -> dict:
    """
    Load encoding map from JSON.
    
    Returns:
        encoding_map dict
    
    Raises:
        FileNotFoundError: If encoding_map.json not found
    """
```

#### write_feature_parquet
```python
def write_feature_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write feature DataFrame to parquet.
    
    Partitioned by: well_id
    
    Returns:
        Path to output directory
    """
```

#### run_features_pipeline
```python
def run_features_pipeline(processed_dir: Path, output_dir: Path) -> Path:
    """
    Top-level features orchestrator.
    
    Pipeline: read → time_features → rolling → 
              decline_gor → encode → write
    
    Returns:
        Path to features output directory
    """
```

---

## Data Schema

### Raw (Downloaded .txt)
```
LEASE_KID, URL, MONTH_YEAR, API_NUMBER, 
PRODUCTION, PRODUCT, LEASE, DOR_CODE, FIELD, 
PRODUCING_ZONE, OPERATOR, COUNTY, TOWNSHIP, 
RANGE, SPOT, LATITUDE, LONGITUDE, WELLS
```

### Interim (After Ingest)
```
well_id, production_date, oil_bbl, gas_mcf, 
lease_kid, county, field, operator, ...
(well-level, one row per well × month)
```

### Processed (After Transform)
```
well_id, production_date, oil_bbl, gas_mcf,
cumulative_oil_bbl, cumulative_gas_mcf,
is_suspect_rate, ...
(well × month, with all months per well)
```

### Features (After Feature Engineering)
```
well_id, production_date, oil_bbl, gas_mcf,
months_since_first_prod, producing_months_count,
production_phase,
oil_bbl_roll3/6/12, gas_mcf_roll3/6/12,
oil_decline_rate_mom, gas_decline_rate_mom, gor,
county_encoded, field_encoded, 
producing_zone_encoded, operator_encoded,
...
(well × month, with engineered features)
```

---

## Type Hints Quick Reference

```python
Path              # pathlib.Path
dd.DataFrame      # dask.dataframe.DataFrame
pd.DataFrame      # pandas.DataFrame
dict              # Python dict
list[str]         # List of strings
list[Path]        # List of Path objects
Optional[Path]    # Path or None
Tuple[dd.DataFrame, dict]  # Tuple of DataFrame and dict
```

---

## Error Handling Summary

| Function | Exception | Condition |
|----------|-----------|-----------|
| extract_lease_urls | FileNotFoundError | File not found |
| extract_lease_urls | KeyError | URL column missing |
| scrape_lease_page | ScrapeError | Button not found |
| discover_raw_files | FileNotFoundError | Directory not found |
| read_raw_files | ValueError | Empty file list |
| read_interim_parquet | FileNotFoundError | Parquet not found |
| read_interim_parquet | ValueError | No parquet files |
| pivot_product_columns | KeyError | Required columns missing |
| encode_categorical_features | KeyError | Categorical columns missing |
| load_encoding_map | FileNotFoundError | JSON file not found |
