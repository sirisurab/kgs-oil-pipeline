# Task Specification: Pipeline Orchestration Component

**Module:** `kgs/src/pipeline.py`
**Config file:** `kgs/config/pipeline_config.yaml`
**Component purpose:** Provide a top-level pipeline runner that chains all four
stages (acquisition → ingestion → cleaning → processing) with structured logging,
per-stage timing, error handling with stage-level isolation, and a YAML
configuration file that controls all runtime parameters.

---

## Overview

The pipeline orchestrator is the single entry point for running the complete
KGS data pipeline. It:

1. Loads a YAML configuration file and builds typed config objects for each
   component.
2. Chains the four pipeline stages in order.
3. Logs structured progress and timing information for each stage.
4. Handles stage-level errors without crashing unexpectedly — failed stages
   are reported and the run is marked as failed in the final summary.
5. Supports a `--dry-run` mode that resolves URLs and discovers files without
   downloading or writing data.
6. Supports running individual stages independently (e.g. `--stage ingest`).

---

## Task 01: Define and validate the pipeline YAML configuration schema

**Module:** `kgs/src/pipeline.py`
**Config file:** `kgs/config/pipeline_config.yaml`
**Function/Class:** `PipelineConfig` (dataclass), `load_config(config_path: str | Path) -> PipelineConfig`

**Description:**
Define a `PipelineConfig` dataclass that aggregates the per-component configs,
and implement a YAML loader that parses and validates the configuration file.

**PipelineConfig fields:**
- `acquisition: AcquisitionConfig` — settings for the acquisition component
- `ingestion: IngestionConfig` — settings for the ingestion component
- `cleaning: CleaningConfig` — settings for the cleaning component
- `processing: ProcessingConfig` — settings for the processing component
- `log_level: str` — Python logging level string, default `"INFO"`
- `log_file: str | None` — optional path to a log file, default `None`
- `dry_run: bool` — if True, no downloads or writes are performed, default `False`
- `enabled_stages: list[str]` — stages to run; valid values are `"acquire"`,
  `"ingest"`, `"clean"`, `"process"`; if empty, all stages run in order

**pipeline_config.yaml structure (must be documented in the file as comments):**
```
pipeline:
  log_level: INFO
  log_file: null
  dry_run: false
  enabled_stages: []

acquisition:
  base_url: https://www.kgs.ku.edu/PRS/petro/productn
  years: [2020, 2021, 2022, 2023, 2024, 2025]
  output_dir: data/raw
  max_concurrency: 5
  max_retries: 3
  retry_backoff_base: 2.0
  request_timeout_seconds: 60
  checksum_algorithm: md5

ingestion:
  raw_dir: data/raw
  years: []
  max_workers: 4
  encoding: utf-8
  dtype_backend: numpy_nullable

cleaning:
  oil_production_upper_bound: 50000.0
  gas_production_upper_bound: 500000.0
  flag_outliers: true
  drop_yearly_summary_rows: true
  dedup_subset: [lease_kid, month_year, product]

processing:
  processed_dir: data/processed
  interim_dir: data/interim
  partition_column: lease_kid
  scale_numeric: true
  encode_categoricals: true
  n_workers: 4
  compute_aggregations: true
```

**load_config behaviour:**
- Read the YAML file using `PyYAML`.
- Construct each component's config dataclass from the corresponding section.
- Apply defaults for any missing optional keys.
- Raise `ConfigError` (custom exception) if required keys are missing or have
  invalid types (e.g. `years` is not a list of ints).

**Error handling:**
- Raise `ConfigError` if the YAML file does not exist.
- Raise `ConfigError` if `enabled_stages` contains an unrecognised stage name.
- Raise `ConfigError` if `acquisition.years` is empty and `dry_run=False`.

**Dependencies:** dataclasses, pathlib, PyYAML, logging

**Test cases:**
- `@pytest.mark.unit` Write a minimal valid YAML to a temp file; assert
  `load_config` returns a `PipelineConfig` with correct field values.
- `@pytest.mark.unit` Omit `acquisition.max_retries` from the YAML; assert
  the default value of 3 is used.
- `@pytest.mark.unit` Provide a non-existent YAML path; assert `ConfigError`
  is raised.
- `@pytest.mark.unit` Provide `enabled_stages: [acquire, unknown_stage]`;
  assert `ConfigError` is raised.
- `@pytest.mark.unit` Provide `acquisition.years: "not_a_list"`; assert
  `ConfigError` is raised.

**Definition of done:** Dataclass, YAML file, `load_config`, and `ConfigError`
are implemented, all test cases pass, ruff and mypy report no errors,
requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement structured logger factory

**Module:** `kgs/src/pipeline.py`
**Function:** `configure_logging(config: PipelineConfig) -> logging.Logger`

**Description:**
Set up a structured logger for the pipeline run with console and optional file
handlers.

**Behaviour:**
- Create a named logger `"kgs_pipeline"`.
- Set the logging level from `config.log_level`.
- Attach a `StreamHandler` to stdout with a formatter that includes:
  `%(asctime)s | %(levelname)-8s | %(name)s | %(message)s`
- If `config.log_file` is not None, attach a `FileHandler` to the specified path
  with the same format.
- Ensure handlers are not duplicated if `configure_logging` is called more than
  once (clear existing handlers first).
- Return the configured logger.

**Error handling:**
- Raise `ConfigError` if `config.log_level` is not a valid Python logging level name.
- Log an INFO message confirming logging is configured and showing the log level.

**Dependencies:** logging, pathlib

**Test cases:**
- `@pytest.mark.unit` Call `configure_logging` with level `"DEBUG"`; assert the
  returned logger has level `logging.DEBUG`.
- `@pytest.mark.unit` Provide a temp file as `log_file`; assert a FileHandler is
  attached to the logger.
- `@pytest.mark.unit` Call `configure_logging` twice; assert no duplicate handlers
  are added.
- `@pytest.mark.unit` Provide an invalid log level `"VERBOSE"`; assert `ConfigError`
  is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 03: Implement stage runner with timing and error isolation

**Module:** `kgs/src/pipeline.py`
**Function:** `run_stage(stage_name: str, stage_fn: callable, *args, logger: logging.Logger, **kwargs) -> tuple[bool, any]`

**Description:**
A generic wrapper that executes a single pipeline stage function with timing,
logging, and error isolation.

**Behaviour:**
- Log `INFO`: "Starting stage: {stage_name}"
- Record wall-clock start time using `time.perf_counter`.
- Call `stage_fn(*args, **kwargs)`.
- Record end time; compute elapsed seconds.
- Log `INFO`: "Stage {stage_name} completed in {elapsed:.2f}s"
- Return `(True, result)` where `result` is the return value of `stage_fn`.
- If `stage_fn` raises any exception:
  - Log `ERROR`: "Stage {stage_name} failed: {exception}"
  - Log the full traceback at DEBUG level.
  - Return `(False, None)`.
  - Do NOT re-raise the exception.

**Error handling:**
- Catch all `Exception` subclasses.
- Do not catch `SystemExit` or `KeyboardInterrupt`.

**Dependencies:** logging, time, traceback

**Test cases:**
- `@pytest.mark.unit` Provide a stage function that returns `42`; assert the
  return value is `(True, 42)`.
- `@pytest.mark.unit` Provide a stage function that raises `RuntimeError("boom")`;
  assert the return value is `(False, None)` and no exception propagates.
- `@pytest.mark.unit` Assert the INFO log contains the stage name and elapsed time.
- `@pytest.mark.unit` Assert that `KeyboardInterrupt` is NOT caught (re-raises).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 04: Implement full pipeline runner

**Module:** `kgs/src/pipeline.py`
**Function:** `run_pipeline(config: PipelineConfig) -> dict`

**Description:**
Orchestrate the full pipeline run by chaining all enabled stages in order,
collecting results and stage statuses, and returning a run summary dict.

**Stage execution order:**
1. `"acquire"` → calls `acquire(config.acquisition)` from `acquisition.py`
2. `"ingest"` → calls `ingest(config.ingestion)` from `ingestion.py`
3. `"clean"` → calls `clean(raw_df, config.cleaning)` from `cleaning.py`
   (passes the Dask DataFrame from ingest)
4. `"process"` → calls `process(cleaned_df, config.processing)` from
   `processing.py` (passes the Dask DataFrame from clean)

**Behaviour:**
- If `config.dry_run=True`, skip all actual stage functions but log what would
  be executed.
- If `config.enabled_stages` is non-empty, run only those stages in the canonical
  order (skip others).
- For stages that require input from a previous stage (ingest needs nothing,
  clean needs ingest result, process needs clean result): if a required upstream
  stage was skipped or failed, log a WARNING and skip the dependent stage too.
- Use `run_stage` for each stage call.
- Collect a `stage_results` dict: `{stage_name: {"success": bool, "elapsed": float}}`.
- Return a `run_summary` dict with keys:
  - `"success": bool` — True only if all enabled stages succeeded.
  - `"stages": stage_results`
  - `"start_time": str` (ISO 8601)
  - `"end_time": str` (ISO 8601)
  - `"total_elapsed_seconds": float`

**Error handling:**
- If a stage fails, log the failure but continue running independent downstream
  stages if possible.
- The pipeline should never crash with an unhandled exception; all stage
  exceptions are isolated by `run_stage`.

**Dependencies:** logging, time, datetime, acquisition, ingestion, cleaning,
processing

**Test cases:**
- `@pytest.mark.unit` Mock all four stage functions to return successfully; assert
  `run_pipeline` returns `{"success": True, ...}`.
- `@pytest.mark.unit` Mock the `"acquire"` stage to fail; assert `"success"` in
  the summary is `False` but remaining stages that don't depend on acquire still
  report results.
- `@pytest.mark.unit` With `dry_run=True`, assert no stage functions are called.
- `@pytest.mark.unit` With `enabled_stages=["ingest"]`, assert only `"ingest"`
  is called.
- `@pytest.mark.unit` Assert the returned dict always contains `"start_time"`,
  `"end_time"`, `"total_elapsed_seconds"`, `"stages"`, and `"success"` keys.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 05: Implement CLI entry point

**Module:** `kgs/src/pipeline.py`
**Function:** `main() -> None`

**Description:**
Implement a command-line interface entry point using `argparse`.

**CLI arguments:**
- `--config` / `-c`: path to the YAML config file (required).
- `--stage` / `-s`: run only this stage; valid values `acquire`, `ingest`,
  `clean`, `process`. Overrides `enabled_stages` in the config file.
- `--dry-run`: boolean flag; if present, sets `config.dry_run=True`.
- `--log-level`: override log level from config; valid values `DEBUG`, `INFO`,
  `WARNING`, `ERROR`.

**Behaviour:**
- Parse arguments.
- Call `load_config(args.config)`.
- Apply CLI overrides to the loaded config.
- Call `configure_logging(config)`.
- Call `run_pipeline(config)`.
- Print the run summary as formatted JSON to stdout.
- Exit with code `0` if `run_summary["success"]` is True, else exit with code `1`.

**Error handling:**
- If `load_config` raises `ConfigError`, print the error message to stderr and
  exit with code `2`.
- Do not let any exception propagate out of `main()`.

**Dependencies:** argparse, json, sys, logging

**Test cases:**
- `@pytest.mark.unit` Simulate CLI args `["--config", "path/to/config.yaml"]`;
  assert `main` calls `load_config` with the correct path.
- `@pytest.mark.unit` Simulate `--dry-run` flag; assert `config.dry_run=True`
  is passed to `run_pipeline`.
- `@pytest.mark.unit` Simulate `--stage ingest`; assert `enabled_stages` is set
  to `["ingest"]`.
- `@pytest.mark.unit` Simulate a bad config path; assert exit code is `2`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Design decisions and constraints

- The pipeline orchestrator must never import from `kgs.src.pipeline` within
  the other component modules. Dependency direction is strictly:
  `pipeline.py` → `acquisition.py`, `ingestion.py`, `cleaning.py`, `processing.py`.
- Stage isolation via `run_stage` means a single stage failure does not abort
  the run. This is intentional: if acquisition fails for one year but the rest
  succeed, ingestion and cleaning can still proceed on the available files.
- The YAML config file is the single source of truth for all runtime parameters.
  Hardcoded values in component modules are limited to default dataclass field
  values only.
- `ConfigError` must be defined in `pipeline.py` and must be importable from
  `kgs.src.pipeline`.
- Logging must use the `"kgs_pipeline"` logger name consistently across all
  modules so all log output is captured by the single handler configured in
  `configure_logging`.
- The `main()` function must be registered as a console script entry point in
  `setup.py` or `pyproject.toml` under the name `kgs-pipeline`.
