# Task Index

| File | Description |
|------|-------------|
| `tasks/acquire_tasks.md` | Lease index filtering, lease-ID extraction, MonthSave HTML parsing, per-file download, parallel acquisition via Dask threaded scheduler |
| `tasks/ingest_tasks.md` | Data-dictionary loading, dtype mapping, per-file raw read with schema enforcement, Dask graph construction via `dd.from_delayed`, interim Parquet write |
| `tasks/transform_tasks.md` | Production date parsing, categorical casting, physical-bounds validation, deduplication, date-gap filling, entity indexing, sort, processed Parquet write |
| `tasks/features_tasks.md` | Cumulative production, GOR, water cut, decline rate, rolling averages, lag features, ML-ready Parquet write |
| `tasks/pipeline_tasks.md` | Config loading, dual-channel logging setup, Dask distributed scheduler init, CLI entry point with per-stage orchestration and error handling |
| `tasks/config_tasks.md` | config.yaml, pyproject.toml, Makefile, .gitignore build and environment artefacts |
