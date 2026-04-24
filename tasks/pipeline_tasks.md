# Pipeline Orchestration, Logging, Entry Point — Task Specifications

All tasks in this file implement the pipeline-level (cross-stage) concerns
for the `kgs_pipeline` package: configuration loading, logging setup, Dask
client initialization, stage orchestration, CLI entry point, build/env
artifacts, and end-to-end integration tests.

**Governing documents (read before implementing any task in this file):**
- `/agent_docs/ADRs.md` — especially ADR-001, ADR-005, ADR-006, ADR-007,
  ADR-008
- `/agent_docs/build-env-manifest.md` — entry point, config structure,
  Makefile targets, scheduler initialization, dependencies, gitignore
- `/agent_docs/stage-manifest-acquire.md`, `stage-manifest-ingest.md`,
  `stage-manifest-transform.md`, `stage-manifest-features.md`
- `/agent_docs/boundary-ingest-transform.md`,
  `/agent_docs/boundary-transform-features.md`
- `/test-requirements.xml` — TR-17, TR-24, TR-25, TR-26, TR-27

All design decisions are governed by those documents. Where they speak,
cite by name — do not restate.

---

## Task P1: Configuration loader

**Module:** `kgs_pipeline/pipeline.py` (or a small supporting module
imported by it).

**Function:** loads `config.yaml` into a Python dict usable by every stage.

**Output contract:**
- Returns a dict with the top-level sections described in
  `build-env-manifest.md` "Configuration structure": `acquire`, `ingest`,
  `transform`, `features`, `dask`, `logging`. If any required top-level
  section is missing, raise a clear error naming which section is missing.
- No default values are injected for configurable settings — all values
  come from `config.yaml` per `build-env-manifest.md` "Configuration
  structure".

**Design decisions governed by docs:**
- Configuration structure → `build-env-manifest.md`.
- No hardcoded configurables, no env-var reads → `build-env-manifest.md`.

**Test cases (unit):**
- Given a minimal valid `config.yaml` in `tmp_path`, assert the returned
  dict has all six top-level sections.
- Given a `config.yaml` missing the `dask` section, assert a clear error
  naming `dask` is raised.
- Given a non-existent path, assert `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, tests pass, ruff and
mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task P2: Logging setup

**Module:** `kgs_pipeline/pipeline.py` (or a small supporting module).

**Function:** configures the Python `logging` module for the whole run
based on the `logging` section of `config.yaml`.

**Behavior:**
- Writes logs to both the console and the file at the path in
  `config.yaml` → `logging.log_file` (ADR-006 dual-channel).
- Log level is read from `config.yaml` → `logging.level`.
- The parent directory of the log file is created if absent (ADR-006 —
  log directory must exist at startup).
- Must be called exactly once at pipeline startup; stages do not configure
  their own handlers (ADR-006).

**Design decisions governed by docs:**
- Dual-channel logging → ADR-006.
- Log file and level from config, not hardcoded → ADR-006.
- Log directory created at startup → ADR-006.

**Test cases (unit):**
- Given a `logging` config pointing to a file under a non-existent
  `tmp_path` subdirectory, assert the directory is created and log lines
  appear in the file.
- Assert the configured level is applied (a DEBUG-only log line is
  absent when level = INFO, present when level = DEBUG).

**Definition of done:** Function is implemented, tests pass, ruff and
mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task P3: Dask client initialization

**Module:** `kgs_pipeline/pipeline.py`.

**Function:** initializes the Dask scheduler client per the `dask` section
of `config.yaml`, after the acquire stage completes and before ingest
begins, per `build-env-manifest.md` "Dask scheduler initialization".

**Behavior:**
- If `dask.scheduler == "local"`, initialize a local distributed cluster
  using `n_workers`, `threads_per_worker`, `memory_limit`, and
  `dashboard_port` from the `dask` section.
- If `dask.scheduler` is a URL, connect to that remote scheduler.
- Log the dashboard URL after initialization.
- Stages (ingest, transform, features) must reuse this client when
  invoked through the pipeline entry point — they must not initialize
  their own cluster (`build-env-manifest.md`).

**Design decisions governed by docs:**
- Distributed client for CPU-bound stages, threaded for acquire →
  ADR-001 and acquire stage manifest H1.
- Scheduler initialization source → `build-env-manifest.md`.
- Client reuse across stages → `build-env-manifest.md`.

**Test cases (unit):**
- Given a `dask.scheduler == "local"` config, assert a client is
  initialized and the function returns a handle the pipeline can pass to
  stages.
- Assert the dashboard URL is emitted to the logger after init.
- (No unit test required for the remote URL branch — integration only.)

**Definition of done:** Function is implemented, tests pass, ruff and
mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task P4: Pipeline orchestrator and CLI entry point

**Module:** `kgs_pipeline/pipeline.py`.

**Function:** the command-line entry point declared in the package
configuration per `build-env-manifest.md` "Pipeline entry point". Runs the
configured stages in order: acquire → ingest → transform → features.

**Behavior (from `build-env-manifest.md` "Pipeline entry point"):**
- Accepts an optional list of stages to run; defaults to all four when
  not specified.
- Reads all settings from `config.yaml` (P1).
- Sets up logging before any stage runs (P2).
- Initializes the Dask client between acquire and ingest (P3), per
  `build-env-manifest.md` "Dask scheduler initialization".
- Runs each stage in order with per-stage timing and error logging.
- Stops execution on stage failure; downstream stages do not run.

**Design decisions governed by docs:**
- Entry point contract → `build-env-manifest.md` "Pipeline entry point".
- Stage ordering → `build-env-manifest.md` "Pipeline targets".
- Single-invocation rule for the Makefile full-pipeline target →
  `build-env-manifest.md` "Pipeline targets".
- No hardcoded config → `build-env-manifest.md` "Configuration structure".
- Logging and Dask init sequencing → ADR-006, `build-env-manifest.md`.

**Test cases (unit):**
- Given a list `["ingest", "transform"]`, assert only those two stages
  are invoked in that order; acquire and features are not.
- Given no stage argument, assert all four stages are invoked in order.
- Given a mocked ingest that raises, assert transform and features are
  not invoked and the exception is logged.
- Assert that the Dask client is initialized between acquire and ingest
  (not before acquire, not after ingest).

**Definition of done:** Function is implemented, CLI entry point is
registered in the package configuration, tests pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported
in this task.

---

## Task P5: Build configuration, Makefile, and environment artifacts

**Artifacts:**
- Package build configuration (choose backend per
  `build-env-manifest.md` "Build backend" — stable / established API).
- `Makefile` with targets per `build-env-manifest.md` "Pipeline targets":
  a target per stage (runnable independently) and a full-pipeline target
  that runs acquire → ingest → transform → features.
- Environment setup targets per `build-env-manifest.md` "Environment
  setup": a target to create a clean virtual environment, and a target
  to install all dependencies including dev tools. The install sequence
  must bootstrap the installer itself first.
- `requirements.txt` and a dev-dependencies requirements file (or
  equivalent in the build configuration), including the Dask dashboard
  library at runtime and the pandas / HTTP-library type stubs for
  development (`build-env-manifest.md` "Dependencies").
- `.gitignore` entries per `build-env-manifest.md` "Version control
  exclusions": `data/`, `logs/`, `large_tool_results/`,
  `conversation_history/`.

**Design decisions governed by docs:**
- Backend stability preference → `build-env-manifest.md` "Build backend".
- Makefile single-invocation rule for the full-pipeline target →
  `build-env-manifest.md` "Pipeline targets" (never both chain
  dependencies and invoke the entry point in the recipe body — that runs
  every stage twice).
- Runtime vs dev dependency split → `build-env-manifest.md` "Dependencies".
- Python version floor → ADR-007 (3.11+).

**Test cases (unit):**
- Assert the full-pipeline Makefile target does not both chain stage
  targets as dependencies AND invoke the entry point in its recipe body
  (a textual check on the Makefile contents is sufficient).
- Assert the package configuration declares the CLI entry point.
- Assert `.gitignore` contains the four required entries.

**Definition of done:** Artifacts exist and are correct, tests pass, ruff
and mypy report no errors, requirements.txt updated with all third-party
packages used at runtime.

---

## Task P6: End-to-end integration test

**Module:** `tests/test_pipeline.py`.

**Test (TR-27, integration marker):**
Run the full pipeline (ingest → transform → features) on 2–3 real raw
source files using a config dict whose output paths (interim, processed,
features) all point inside pytest's `tmp_path`. Acquire may be skipped or
mocked to stage the fixture files in `tmp_path/data/raw/`.

**Assertions:**
- Ingest output satisfies every upstream guarantee in
  `boundary-ingest-transform.md`.
- Transform output satisfies every upstream guarantee in
  `boundary-transform-features.md`.
- Features output satisfies TR-26 — all expected derived columns present,
  schema consistent across a sample of partitions.
- No unhandled exception is raised across the full run.

**Design decisions governed by docs:**
- tmp_path for all stage outputs → ADR-008. Stage functions under test
  must accept a config dict with output paths overridden to `tmp_path` —
  the test must never write under the project's `data/` directory.
- Integration marker → ADR-008 (exactly one of `unit` or `integration`
  per test).

**Definition of done:** Test is implemented, passes with `pytest -m
integration` when fixture files are present, is skipped cleanly when
they are not, ruff and mypy report no errors, requirements.txt updated
with all third-party packages imported in this task.
