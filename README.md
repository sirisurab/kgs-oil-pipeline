# KGS Oil & Gas Production Pipeline

Dask-based pipeline for processing Kansas Geological Survey (KGS) monthly oil and gas production data. Covers acquisition through feature engineering, producing ML-ready Parquet output partitioned by lease.

**Data source:** [Kansas Geological Survey](https://www.kgs.ku.edu/) — monthly lease-level production reports

---

## Pipeline Stages

| Stage | Input | Output |
|---|---|---|
| **acquire** | Lease index file (`data/external/`) → KGS download URLs | Raw `.txt` production files in `data/raw/` |
| **ingest** | Raw `.txt` files | Schema-enforced Parquet in `data/interim/` (rows from 2024 onwards) |
| **transform** | Interim Parquet | Cleaned, indexed, deduplicated Parquet in `data/processed/transform/` |
| **features** | Processed Parquet | ML-ready Parquet with derived features in `data/processed/features/` |

### Derived features

`cum_oil`, `cum_gas`, `cum_water` · `gor`, `water_cut` · `decline_rate` · `rolling_3m_oil`, `rolling_6m_oil`, `rolling_3m_gas`, `rolling_6m_gas` · `lag1_oil`, `lag1_gas`

---

## Setup

```bash
# Create virtual environment and install dependencies
make venv
source .venv/bin/activate
make install
```

---

## Prerequisites

The acquire stage requires a lease index file at the path set in `config.yaml`:

```
data/external/oil_leases_2020_present.txt
```

This file lists active lease IDs used to build download URLs. It must be in place before running `acquire`. The `data/external/` directory is preserved by the cleanup script and not cleared between runs.

---

## Configuration

All settings live in `config.yaml` (committed). Key sections:

```yaml
acquire:
  lease_index_path: data/external/oil_leases_2020_present.txt
  target_year: 2024      # only months from this year onwards are downloaded
  max_workers: 5

ingest:
  data_dict_path: references/kgs_monthly_data_dictionary.csv

dask:
  n_workers: 4
  memory_limit: "3GB"
  dashboard_port: 8787   # http://localhost:8787 when pipeline is running
```

Adjust `n_workers` and `memory_limit` to match available hardware.

---

## Running

```bash
# Full pipeline (all four stages in sequence)
make pipeline

# Individual stages
make acquire
make ingest
make transform
make features

# Equivalent CLI commands (with venv active)
kgs-pipeline
kgs-pipeline acquire
kgs-pipeline ingest
kgs-pipeline transform
kgs-pipeline features
```

---

## Testing and Code Quality

```bash
make test        # pytest
make lint        # ruff
make typecheck   # mypy
```

---

## Data Directory Layout

```
data/
  external/          # lease index file — preserved between runs
  raw/               # downloaded .txt files (acquire output)
  interim/           # schema-enforced Parquet (ingest output)
  processed/
    transform/       # cleaned, indexed Parquet (transform output)
    features/        # ML-ready Parquet (features output)
logs/
  pipeline.log       # stage-level logging (INFO + ERROR)
```

---

## References

```
references/
  kgs_monthly_data_dictionary.csv    # schema for monthly production files
  kgs_archives_data_dictionary.csv   # schema for historical archive files
```