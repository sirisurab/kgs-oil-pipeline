# Task Index

- `tasks/acquire_tasks.md`   <- all acquire stage tasks: lease index loading, lease ID extraction, MonthSave HTML parsing, single-lease download, retry decorator, parallel Dask-threaded orchestration, file integrity validation, and acquire entry point
- `tasks/ingest_tasks.md`    <- all ingest stage tasks: data dictionary schema loader, raw file reader, date filter, schema enforcer, single-file worker, parallel Dask-distributed orchestrator, Parquet writer, schema completeness validator, and ingest entry point
- `tasks/transform_tasks.md` <- all transform stage tasks: production date parser, null/invalid cleaner, unit range validator, deduplicator, entity indexer and temporal sorter, well completeness checker, map_partitions orchestrator, Parquet writer, data integrity spot-checker, and transform entry point
- `tasks/features_tasks.md`  <- all features stage tasks: cumulative production calculator, GOR and water cut ratios, decline rate calculator, rolling statistics, lag features, categorical encoder, map_partitions orchestrator, output schema validator, schema stability checker, features Parquet writer, feature correctness validator, and features entry point
