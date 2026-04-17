# KGS Pipeline

End-to-end pipeline for downloading, ingesting, cleaning, and feature-engineering
Kansas Geological Survey (KGS) oil and gas production data.

## Stages

| Stage | Description |
|---|---|
| **acquire** | Download raw KGS lease production `.txt` files from the KGS portal |
| **ingest** | Load raw files, enforce canonical schema, write interim Parquet |
| **transform** | Derive `production_date`, validate bounds, deduplicate, cast categoricals |
| **features** | Compute ML features (cumulative production, GOR, decline rate, rolling averages, etc.) |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
pip install -r requirements.txt
```

## Configuration

All settings live in `config.yaml`. Key sections:

- `acquire` — lease index path, output directory, worker count
- `ingest` / `transform` / `features` — input/output directories
- `dask` — scheduler settings (local cluster by default)
- `logging` — log file path and level

## Usage

Run all stages:

```bash
kgs-pipeline
```

Run specific stages:

```bash
kgs-pipeline --stages acquire ingest
kgs-pipeline --stages transform features
```

Or via Makefile targets:

```bash
make pipeline   # all stages
make acquire    # acquire only
make ingest     # ingest only
make transform  # transform only
make features   # features only
```

## Running Tests

```bash
pytest tests/
```

Run only unit tests (no network or data files required):

```bash
pytest tests/ -m unit
```

## Data directories

| Directory | Contents |
|---|---|
| `data/external/` | Lease index file (`oil_leases_2020_present.txt`) |
| `data/raw/` | Downloaded raw `.txt` files from KGS |
| `data/interim/` | Parquet after ingest |
| `data/interim/transformed/` | Parquet after transform |
| `data/processed/` | ML-ready features Parquet + feature manifest |

## Output files

- `data/processed/feature_manifest.json` — feature column names and encoding maps
- `data/processed/pipeline_report.json` — per-stage timing and run summary
- `logs/pipeline.log` — full log output

## Schema reference

The canonical schema is defined in `references/kgs_monthly_data_dictionary.csv`.
