# Pipeline Orchestration Tasks

**Modules:** `kgs_pipeline/pipeline.py`, `kgs_pipeline/config.py`
**Test file:** `tests/test_pipeline.py`

Read `ADRs.md` (ADR-001, ADR-005, ADR-006, ADR-007, ADR-008) and
`build-env-manifest.md` in full before implementing any task in this file.
`build-env-manifest.md` is the authoritative source for all entry point, Makefile,
configuration structure, Dask scheduler initialization, and logging requirements.

---

## Task P-01: Configuration loading

**Module:** `kgs_pipeline/config.py`
**Function:** `load_config(config_path: str | Path) -> dict`

**Description:** Read `config.yaml` from `config_path` and return its contents as a
plain Python dict. The configuration structure must conform to the top-level sections
defined in `build-env-manifest.md`: `acquire`, `ingest`, `transform`, `features`,
`dask`, and `logging`. All configurable values for every pipeline stage must be read
from this file — no configurable values may be hardcoded in pipeline code
(`build-env-manifest.md`).

**Error handling:**
- If `config_path` does not exist, raise `FileNotFoundError` with a descriptive
  message.
- If the YAML is malformed or missing a required top-level section, raise
  `ValueError` with the name of the missing section.

**Dependencies:** PyYAML, pathlib

**Test cases (unit):**
- Given a valid `config.yaml` with all required sections, assert the returned dict
  contains all six top-level keys.
- Given a config file missing the `dask` section, assert `ValueError` is raised.
- Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- Given a malformed YAML file, assert an appropriate exception is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-02: Logging setup

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `setup_logging(config: dict) -> None`

**Description:** Configure the root logger to write to both the console and a log
file simultaneously (ADR-006). The log file path and log level must be read from the
`logging` section of `config` — not hardcoded. Create the log output directory if it
does not exist before opening the file handler. This function must be called at
pipeline startup before any stage runs — stages must not configure their own handlers
(ADR-006).

**Error handling:**
- If the `logging` key or required sub-keys (`log_file`, `level`) are absent from
  `config`, raise `KeyError`.
- If the log directory cannot be created (e.g. permission error), let the OS
  exception propagate.

**Dependencies:** logging, pathlib

**Test cases (unit):**
- Given a config with `log_file` and `level` set, assert both a `StreamHandler`
  and a `FileHandler` are attached to the root logger after the call.
- Assert the log directory is created if it does not exist.
- Given a config missing the `logging` key, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-03: Dask scheduler initialization

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `init_scheduler(config: dict) -> distributed.Client`

**Description:** Initialize the Dask distributed scheduler after acquire completes
and before ingest begins, using the `dask` section of `config`
(`build-env-manifest.md`). If `config["dask"]["scheduler"]` is `"local"`, create a
local `distributed.LocalCluster` with `n_workers`, `threads_per_worker`, and
`memory_limit` from config and return a `distributed.Client` connected to it. If
`config["dask"]["scheduler"]` is a URL string, connect to the remote scheduler at
that address and return the `Client`. Log the dashboard URL after initialization
(`build-env-manifest.md`). Individual stages must reuse an existing client — they
must not initialize their own cluster when invoked through the pipeline entry point.

**Error handling:**
- If the `dask` key or required sub-keys are absent from `config`, raise `KeyError`.
- If a remote scheduler URL is unreachable, let the `distributed` exception
  propagate — do not silently swallow it.

**Dependencies:** dask, distributed

**Test cases (unit):**
- Given a config with `scheduler: "local"`, assert `init_scheduler` returns a
  `distributed.Client` instance.
- Assert the dashboard URL is logged after initialization.
- Given a config missing `dask.n_workers`, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-04: Pipeline entry point and stage orchestration

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `main(stages: list[str] | None = None) -> None`

**Description:** Implement the CLI entry point that chains all pipeline stages in
order: `acquire → ingest → transform → features`. The `stages` argument is an
optional list of stage names to run; if `None` or empty, all four stages are run in
order (`build-env-manifest.md`). The entry point must:
1. Read all settings from `config.yaml` via `load_config`.
2. Call `setup_logging` before any stage runs (ADR-006).
3. Run the `acquire` stage first.
4. Call `init_scheduler` after acquire completes and before ingest begins
   (`build-env-manifest.md`).
5. Run `ingest`, `transform`, and `features` in order, reusing the scheduler client
   initialized in step 4.
6. Log per-stage timing and errors (ADR-006).
7. Stop execution on stage failure — downstream stages must not run if an upstream
   stage fails (`build-env-manifest.md`).

The entry point must be registered in the package configuration so it is runnable as
a CLI command (`build-env-manifest.md`). Independent computations within the same
stage must be batched into a single Dask compute call — sequential `.compute()` calls
on independent results are prohibited (ADR-005).

**Error handling:**
- If a stage raises an exception, log it at ERROR level with the stage name and
  elapsed time, then re-raise to stop execution.
- If `config.yaml` is not found, raise `FileNotFoundError` before any stage runs.

**Dependencies:** dask, distributed, logging, time, pathlib

**Test cases (unit):**
- Given a config that specifies `stages: ["acquire"]`, assert only the acquire
  function is called and the scheduler is not initialized.
- Given a config that specifies all stages, assert the stages are called in order
  and each stage's elapsed time is logged.
- Given a stage that raises an exception, assert the subsequent stages are not
  called and the exception is re-raised.
- Assert `setup_logging` is called before any stage function.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-05: Package configuration and build artefacts

**Module:** `pyproject.toml`, `Makefile`, `config.yaml`, `.gitignore`, `requirements.txt`
**Function:** N/A (configuration and build artefacts)

**Description:** Create all build and environment artefacts required by
`build-env-manifest.md`:

**`pyproject.toml`:**
- Declare the build backend explicitly — do not rely on setuptools defaults
  (`build-env-manifest.md`).
- Register the CLI entry point so the pipeline is runnable as a command.
- List all runtime and development dependencies.
- Runtime dependencies must include the Dask dashboard library
  (`build-env-manifest.md`).
- Development dependencies must include type stub packages for pandas and the HTTP
  request library (`build-env-manifest.md`).

**`Makefile`:**
- Provide targets: `venv`, `install`, `acquire`, `ingest`, `transform`, `features`,
  `pipeline`.
- The `pipeline` target must run all four stages in order using exactly one
  invocation approach — not both chained targets and an entry point call in the same
  recipe (a dual invocation runs every stage twice, `build-env-manifest.md`).
- The `install` target must bootstrap the package installer before installing
  project dependencies (`build-env-manifest.md`).

**`config.yaml`:**
- Must contain all six top-level sections defined in `build-env-manifest.md`:
  `acquire`, `ingest`, `transform`, `features`, `dask`, `logging`.
- Must contain all stage-specific settings consumed by each stage's `config` dict.

**`.gitignore`:**
- Must exclude: `data/`, `logs/`, `large_tool_results/`, `conversation_history/`
  (`build-env-manifest.md`).

**`requirements.txt`:**
- Must list all third-party runtime packages imported anywhere in the pipeline.

**Test cases (unit):**
- Assert `pyproject.toml` declares a build backend explicitly.
- Assert the CLI entry point is registered in `pyproject.toml`.
- Assert `config.yaml` contains all six required top-level sections.
- Assert `.gitignore` contains entries for `data/` and `logs/`.

**Definition of done:** All artefacts are present and valid, the package installs
cleanly via `pip install -e .`, the CLI entry point is runnable, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
