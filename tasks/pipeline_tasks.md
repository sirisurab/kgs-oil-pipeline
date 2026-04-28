# Pipeline Orchestration Tasks

**Module:** `kgs_pipeline/pipeline.py`  
**Test file:** `tests/test_pipeline.py`  
**Governing docs:** build-env-manifest.md, ADR-001, ADR-005, ADR-006, ADR-007

---

## Task P-01: Load configuration from config.yaml

**Module:** `kgs_pipeline/pipeline.py`  
**Function:** `load_config(config_path: str) -> dict`

**Description:**  
Read `config.yaml` from `config_path` and return its contents as a Python dict. All
pipeline settings are read from this file — no configurable values are hardcoded or read
from environment variables (see build-env-manifest.md). The returned dict must contain
all top-level sections defined in build-env-manifest.md: `acquire`, `ingest`, `transform`,
`features`, `dask`, and `logging`.

**Input:** `config_path: str` — path to `config.yaml`.  
**Output:** `dict` — full pipeline configuration.

**Constraints:**
- Configuration structure: see build-env-manifest.md. No configurable values may be
  hardcoded.
- No retry logic, no caching.

**Test cases:**
- Given a valid `config.yaml` with all required top-level sections, assert the returned
  dict contains keys: `acquire`, `ingest`, `transform`, `features`, `dask`, `logging`.
- Given a `config.yaml` missing a required section, assert the function raises a
  descriptive error or the absence is detectable by the caller.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-02: Configure dual-channel logging

**Module:** `kgs_pipeline/pipeline.py`  
**Function:** `setup_logging(config: dict) -> None`

**Description:**  
Configure the Python `logging` module to write to both the console and a log file
simultaneously. Log file path and log level are read from `config["logging"]["log_file"]`
and `config["logging"]["level"]` respectively (see ADR-006 and build-env-manifest.md).
Create the log output directory if it does not exist. This function must be called before
any stage runs — stages must not configure their own log handlers independently (see
ADR-006).

**Input:** `config: dict` — full pipeline configuration.  
**Output:** None. Side effect: logging configured with console and file handlers.

**Constraints:**
- Dual-channel logging: both console and file — see ADR-006.
- Log file path and level from config — not hardcoded — see ADR-006.
- Log directory must be created if absent — see ADR-006.
- No retry logic, no caching.

**Test cases:**
- Given a config with `logging.log_file = "logs/pipeline.log"` and
  `logging.level = "INFO"`, assert the root logger has at least one `StreamHandler` and
  at least one `FileHandler` after setup.
- Assert the log directory is created if it does not exist.
- Assert the `FileHandler` writes to the path specified in config.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-03: Initialize the Dask distributed scheduler

**Module:** `kgs_pipeline/pipeline.py`  
**Function:** `init_dask_client(config: dict) -> distributed.Client`

**Description:**  
Initialize the Dask scheduler before any CPU-bound stage (ingest, transform, features)
runs. The scheduler must not be initialized inside the acquire branch — acquire uses the
threaded scheduler and does not require a distributed cluster (see build-env-manifest.md
and ADR-001). Scheduler initialization behavior is governed by
`config["dask"]["scheduler"]`:

- If the value is `"local"`, initialize a local distributed cluster using
  `config["dask"]["n_workers"]`, `config["dask"]["threads_per_worker"]`, and
  `config["dask"]["memory_limit"]`.
- If the value is a URL (e.g., `"tcp://host:port"`), connect to the remote scheduler at
  that address.

After initialization, log the Dask dashboard URL (see ADR-006). Return the
`distributed.Client` instance.

**Input:** `config: dict`.  
**Output:** `distributed.Client`.

**Constraints:**
- Scheduler type is determined by config — see build-env-manifest.md.
- Distributed cluster must not be initialized for acquire-only invocations — see
  build-env-manifest.md.
- Dashboard URL must be logged — see build-env-manifest.md and ADR-006.
- Individual stages reuse an existing client — they must not initialize their own cluster
  — see build-env-manifest.md.
- No retry logic, no caching.

**Test cases:**
- Given `config["dask"]["scheduler"] = "local"`, assert the function returns a
  `distributed.Client` instance.
- Assert the dashboard URL is logged after initialization.
- Assert that when `scheduler` is a URL string (not `"local"`), the client attempts to
  connect to that address (mock `distributed.Client` constructor to verify the call).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task P-04: Pipeline entry point with stage orchestration

**Module:** `kgs_pipeline/pipeline.py`  
**Function:** `main(stages: list[str] | None = None) -> None`

**Description:**  
The command-line entry point for the pipeline. Accepts an optional list of stage names to
run; defaults to all four stages `["acquire", "ingest", "transform", "features"]` when
not specified. Reads configuration from `config.yaml` (via `load_config`). Sets up
logging (via `setup_logging`) before any stage runs. Initializes the Dask distributed
scheduler (via `init_dask_client`) if at least one CPU-bound stage (`ingest`, `transform`,
`features`) is in the requested stages — does not initialize if only `acquire` is
requested (see build-env-manifest.md). Runs each requested stage in order, logging
per-stage timing (start time, end time, elapsed) and catching per-stage errors. Stops
execution on stage failure — downstream stages must not run if an upstream stage fails
(see build-env-manifest.md).

**Input:** `stages: list[str] | None` — list of stage names from the command line, or
`None` to run all four.  
**Output:** None. Side effect: stages execute in order.

**Constraints:**
- Entry point registration: see build-env-manifest.md (must be registered as a console
  script in pyproject.toml).
- Stage execution order: acquire → ingest → transform → features — see build-env-manifest.md.
- Stop on failure: downstream stages must not run after an upstream failure — see
  build-env-manifest.md.
- Logging setup happens before any stage — see ADR-006.
- Distributed scheduler initialized only when a CPU-bound stage is present — see
  build-env-manifest.md and ADR-001.
- No retry logic, no caching.

**Test cases (TR-27):**
- Given `stages=["ingest", "transform", "features"]` with mocked stage functions, assert
  all three are called in order and results are logged.
- Given a stage that raises an exception, assert subsequent stages are not called and the
  exception is logged.
- Given `stages=["acquire"]`, assert `init_dask_client` is not called.
- Given `stages=None`, assert all four stage functions are invoked in order.
- Run the full pipeline (ingest → transform → features) on 2–3 raw files using `tmp_path`;
  assert no unhandled exceptions across the full run (TR-27).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
