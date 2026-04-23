# Task Index

- `tasks/acquire_tasks.md`   <- acquire stage: lease index filtering, lease ID extraction, MonthSave link fetching, parallel file download with Dask threaded scheduler and caching
- `tasks/ingest_tasks.md`    <- ingest stage: dtype map from data dictionary, raw file parsing, date-range filtering, parallel Dask-distributed consolidation to interim Parquet
- `tasks/transform_tasks.md` <- transform stage: production date parsing, production value cleaning, deduplication, categorical casting, date-gap filling, Dask-distributed orchestration to processed Parquet
- `tasks/features_tasks.md`  <- features stage: cumulative production, GOR and water cut, decline rate, rolling averages and lag features, Dask-distributed orchestration to ML-ready Parquet
- `tasks/pipeline_tasks.md`  <- pipeline orchestration: config loading, dual-channel logging, Dask scheduler initialization, CLI entry point, build artefacts (pyproject.toml, Makefile, config.yaml, .gitignore, requirements.txt)
