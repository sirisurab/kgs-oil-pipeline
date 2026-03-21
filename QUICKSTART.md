# KGS Pipeline - Quick Start Guide

## Prerequisites

- Python 3.11+
- pip or conda
- Internet connection (for web scraping)

## 1. Install

```bash
cd kgs
make install
```

Or manually:
```bash
pip install -r requirements.txt
```

## 2. Prepare Input Data

The pipeline requires:
- **`data/external/oil_leases_2020_present.txt`** (provided separately)

CSV format with columns:
- LEASE_KID (unique lease identifier)
- URL (KGS lease page URL)
- LEASE, FIELD, OPERATOR, COUNTY, LATITUDE, LONGITUDE, etc.

## 3. Run the Pipeline

### Option A: Full Pipeline (All 4 Components)
```bash
make run-all
```

### Option B: Run Components Individually

```bash
# Step 1: Web scrape raw KGS data
make run-acquire
# Output: data/raw/lp*.txt files

# Step 2: Read and merge with metadata
make run-ingest
# Output: data/interim/ (Parquet)

# Step 3: Clean and standardize
make run-transform
# Output: data/processed/ (Parquet, partitioned by well_id)

# Step 4: Engineer ML features
make run-features
# Output: data/features/ (Parquet, ML-ready)
```

### Option C: Python API

```python
from kgs_pipeline.acquire import run_acquire_pipeline
from kgs_pipeline.ingest import run_ingest_pipeline
from kgs_pipeline.transform import run_transform_pipeline
from kgs_pipeline.features import run_features_pipeline

# Run sequentially
run_acquire_pipeline()
run_ingest_pipeline()
run_transform_pipeline()
run_features_pipeline()

print("Pipeline complete! Data ready in data/features/")
```

## 4. Verify Output

After the pipeline completes, you'll have:

```
data/
├── raw/              ← Raw .txt files from KGS
├── interim/          ← Cleaned data (Parquet)
├── processed/        ← Standardized data (Parquet, by well_id)
└── features/         ← ML-ready features (Parquet)
```

Check the output:

```python
import dask.dataframe as dd

# Load features
features = dd.read_parquet("data/features/")
print(f"Total well-month records: {len(features)}")
print(features.head())

# Summary statistics
print(features.groupby("product")["production_sum_month"].mean().compute())
```

## 5. Run Tests

```bash
make test              # Run all tests with coverage report
pytest tests -v       # Verbose output
pytest tests/test_transform.py  # Run specific test file
```

## 6. Code Quality

```bash
make lint              # Lint and auto-fix code
make type              # Type checking
make clean             # Remove temp files
```

## 7. Common Tasks

### Load Processed Data
```python
import dask.dataframe as dd

processed = dd.read_parquet("data/processed/")
print(f"Total records: {len(processed)}")
print(processed.columns.tolist())
```

### Filter by Well
```python
well_data = processed[processed["well_id"] == "12345"].compute()
print(well_data[["production_date", "production", "product"]])
```

### Aggregate by Operator
```python
by_operator = (
    processed
    .groupby("operator")["production"]
    .sum()
    .compute()
    .sort_values(ascending=False)
)
print(by_operator.head(10))
```

### Find Outliers
```python
outliers = processed[processed["outlier_flag"] == True].compute()
print(f"Flagged {len(outliers)} outlier records")
```

### Time Series by Field
```python
field_ts = (
    processed[processed["field_name"] == "Field A"]
    .groupby("production_date")["production"]
    .sum()
    .compute()
    .sort_index()
)
print(field_ts)
```

## 8. Troubleshooting

### Issue: FileNotFoundError - lease index not found

**Solution**: Place `oil_leases_2020_present.txt` in `data/external/`

```bash
mkdir -p data/external
# Copy or download file to data/external/oil_leases_2020_present.txt
```

### Issue: Playwright browsers not installed

**Solution**: Auto-install on first scrape, or manually:

```bash
playwright install chromium
```

### Issue: Memory error with large datasets

**Solution**: Dask by default uses your system RAM. Reduce partitions:

```python
# In your script, use smaller partition count
ddf = dd.read_parquet("data/interim/", blocksize="64MB")
```

### Issue: Slow web scraping

**Solution**: Adjust concurrency in `kgs_pipeline/config.py`:

```python
SCRAPE_CONCURRENCY = 3  # Reduce from 5 if hitting rate limits
```

### Issue: Tests fail with import errors

**Solution**: Ensure kgs package is in PYTHONPATH:

```bash
pip install -e .
pytest tests -v
```

## 9. Next Steps

### Use Features for ML

```python
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

# Load features
features_df = pd.read_parquet("data/features/")

# Prepare for model
X = features_df[[
    "production_sum_month",
    "rolling_avg_12mo",
    "well_lifetime_months",
    "cumulative_production"
]]
y = features_df["production_trend"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train model...
```

### Create Dashboards

```python
# Create visualization data
import matplotlib.pyplot as plt

features_df = pd.read_parquet("data/features/")

# Production trend by operator
features_df.groupby("operator")["cumulative_production"].sum().sort_values().tail(10).plot(kind="barh")
plt.title("Top 10 Operators by Cumulative Production")
plt.show()
```

### Export for Analytics

```python
# Export to CSV for Excel/Tableau
features_df.to_csv("features_export.csv", index=False)

# Or Parquet (more efficient)
features_df.to_parquet("features_sample.parquet")
```

## 10. Documentation

- **README.md**: Comprehensive overview
- **IMPLEMENTATION_SUMMARY.md**: Technical details
- **kgs_pipeline/config.py**: Configuration reference
- **Code docstrings**: Function signatures and parameters

## 11. Performance Tips

1. **Use Parquet format**: Already stored as Parquet, 10x faster than CSV
2. **Filter early**: Use well_id/field_name filters before compute()
3. **Partitions matter**: Processed data partitioned by well_id for efficiency
4. **Lazy evaluation**: Dask operations don't run until .compute() or write
5. **Parallel by default**: 4+ cores will see speedup without extra config

## 12. Sample Pipeline Configurations

### Fast Demo (1 hour)
```bash
# Edit config.py: SCRAPE_CONCURRENCY = 2, test with small subset
python -c "
from kgs_pipeline.acquire import load_lease_urls
leases = load_lease_urls(...)
# Sample first 10 leases
leases = leases[:10]
"
```

### Production (Full Data)
```bash
# Use defaults, run overnight
make run-all
```

### Resume From Checkpoint
```bash
# If ingest failed but acquire succeeded:
make run-ingest run-transform run-features
```

---

**Ready to process your KGS data!** 🚀

Questions? See README.md for detailed component documentation.
