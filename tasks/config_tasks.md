# Configuration Component Tasks

**Modules:** `kgs_pipeline/config.py`, `kgs_pipeline/logging_utils.py`
**Support files:** `pyproject.toml`, `Makefile`, `.gitignore`, `logging_config.yaml`
**Test file:** `tests/test_config.py`

## Overview

The configuration component provides two artefacts consumed by every other pipeline
module:

1. **`kgs_pipeline/config.py`** — a central `Config` dataclass (or module-level
   constants) that defines all tuneable parameters: data directory paths, KGS URL
   templates, parallel worker count, rate-limit sleep, and the minimum target year.
   Every value is overridable via an environment variable so that CI, Docker, and local
   development environments can all use the same code with different settings.

2. **`kgs_pipeline/logging_utils.py`** — a `get_logger(name)` factory that returns a
   `logging.Logger` configured with a custom `JsonFormatter`, writing to both stdout and
   a rotating file at `logs/pipeline.log`. This module is imported by every other module
   in the pipeline and must therefore be the first module to be implemented.

3. **`logging_config.yaml`** — a YAML file at the project root that provides an
   alternative/additional logging configuration loadable via `logging.config.dictConfig`.
   This file is for operators who want to reconfigure logging without changing Python code.

4. **Project scaffold** — `pyproject.toml`, `Makefile`, and `.gitignore` as specified
   in the acquire component tasks (Task 01 of `acquire_tasks.md`). These are listed here
   for completeness; the authoritative specification lives in `acquire_tasks.md` Task 01.

## Design Decisions

- `config.py` uses a `dataclasses.dataclass` named `Config` with a module-level singleton
  `config = Config()` so that importers can write `from kgs_pipeline.config import config`
  and access `config.RAW_DATA_DIR`, etc.
- All `Config` fields have defaults; environment-variable overrides are applied in
  `__post_init__` using `os.environ.get(...)`.
- `logging_utils.py` does not depend on `config.py` to avoid circular imports; it reads
  `LOG_LEVEL` from `os.environ` directly.
- `logging_config.yaml` uses the Python `logging.config` `dictConfig` schema version 1.
- The `logs/` directory is created automatically by `get_logger` if it does not exist.
- `pyproject.toml` build-backend must be `"setuptools.build_meta"` — never
  `"setuptools.backends.legacy:build"` (this breaks editable install on Python 3.13).
- Dev dependencies in `pyproject.toml` must include `pandas-stubs` and `types-requests`
  (required for mypy to type-check pandas and requests usage without errors).
- `.gitignore` must include `data/`, `large_tool_results/`, and `conversation_history/`
  as top-level entries.

---

## Task 01: Central configuration module

**Module:** `kgs_pipeline/config.py`
**Class:** `Config` (dataclass)
**Instance:** `config` (module-level singleton)

**Description:**
Implement a `Config` dataclass with the following fields and default values. Each field
is overridable via a matching environment variable read in `__post_init__`. The
environment variable name is the upper-case field name prefixed with `KGS_` (e.g.,
`KGS_RAW_DATA_DIR`).

Fields and defaults:

| Field | Type | Default | Env var |
|-------|------|---------|---------|
| `LEASE_INDEX_PATH` | `str` | `"data/external/oil_leases_2020_present.txt"` | `KGS_LEASE_INDEX_PATH` |
| `RAW_DATA_DIR` | `str` | `"data/raw"` | `KGS_RAW_DATA_DIR` |
| `INTERIM_DATA_DIR` | `str` | `"data/interim"` | `KGS_INTERIM_DATA_DIR` |
| `PROCESSED_DATA_DIR` | `str` | `"data/processed"` | `KGS_PROCESSED_DATA_DIR` |
| `FEATURES_DATA_DIR` | `str` | `"data/features"` | `KGS_FEATURES_DATA_DIR` |
| `MAX_WORKERS` | `int` | `5` | `KGS_MAX_WORKERS` |
| `SLEEP_SECONDS` | `float` | `0.5` | `KGS_SLEEP_SECONDS` |
| `MIN_YEAR` | `int` | `2024` | `KGS_MIN_YEAR` |
| `MONTH_SAVE_URL_TEMPLATE` | `str` | `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"` | `KGS_MONTH_SAVE_URL_TEMPLATE` |
| `MAIN_LEASE_URL_PREFIX` | `str` | `"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="` | `KGS_MAIN_LEASE_URL_PREFIX` |
| `LOG_DIR` | `str` | `"logs"` | `KGS_LOG_DIR` |
| `LOG_LEVEL` | `str` | `"INFO"` | `KGS_LOG_LEVEL` |

After the class definition, instantiate the singleton: `config = Config()`.

All path fields (`LEASE_INDEX_PATH`, `RAW_DATA_DIR`, `INTERIM_DATA_DIR`,
`PROCESSED_DATA_DIR`, `FEATURES_DATA_DIR`, `LOG_DIR`) must resolve to absolute paths at
runtime. Implement a `resolve_path(p: str) -> Path` helper inside the module that joins
a relative path to the project root (the directory two levels above `config.py`).

**Dependencies:** `dataclasses`, `os`, `pathlib` (all stdlib)

**Test cases:**

- `@pytest.mark.unit` — Import `config` and assert `config.MAX_WORKERS == 5` (default).
- `@pytest.mark.unit` — Set `os.environ["KGS_MAX_WORKERS"] = "10"`, instantiate a fresh
  `Config()`, and assert `max_workers == 10`. Clean up the env var after the test.
- `@pytest.mark.unit` — Assert `config.MONTH_SAVE_URL_TEMPLATE` contains the substring
  `"chasm.kgs.ku.edu"`.
- `@pytest.mark.unit` — Assert `config.MIN_YEAR` is an `int` (not a string).
- `@pytest.mark.unit` — Assert `config.SLEEP_SECONDS` is a `float`.
- `@pytest.mark.unit` — Assert `config.RAW_DATA_DIR` is either a `str` or can be used
  as a path argument (i.e., `Path(config.RAW_DATA_DIR)` does not raise).

**Definition of done:** `Config` dataclass and `config` singleton implemented, all tests
pass, `ruff` and `mypy` report no errors. `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 02: Structured JSON logging utility

**Module:** `kgs_pipeline/logging_utils.py`
**Classes/Functions:**
- `JsonFormatter(logging.Formatter)` — custom formatter
- `get_logger(name: str) -> logging.Logger` — factory function

**Description:**
Implement a reusable logging factory used by every pipeline module. The logger must
emit structured JSON-formatted log entries to both stdout (a `logging.StreamHandler`)
and a rotating file handler at `{LOG_DIR}/pipeline.log`
(`logging.handlers.RotatingFileHandler`, max bytes = 10 MB, backup count = 5). Create
the `logs/` directory (or whatever `LOG_DIR` resolves to) if it does not exist.

Each log record must contain at minimum the following JSON fields:
- `timestamp`: ISO-8601 datetime string (UTC).
- `level`: the log level name string (e.g., `"INFO"`, `"WARNING"`).
- `logger`: the logger name passed to `get_logger`.
- `message`: the formatted log message.
- Any additional keys passed via `extra=` to the logging call (e.g., `extra={"lease_id": "123"}`).

Implementation requirements:
- `JsonFormatter` subclasses `logging.Formatter` and overrides `format(record)` to
  return a JSON-serialised string built from a dict of the required fields plus any
  items from `record.__dict__` that were injected via `extra=`.
- `get_logger(name)` must be idempotent: calling it a second time with the same name
  must not add duplicate handlers to the logger. Check `if not logger.handlers` before
  adding handlers.
- Default log level is `logging.INFO`. The level is overridable via the `LOG_LEVEL`
  environment variable (read directly from `os.environ`, not from `config.py`, to avoid
  circular imports).
- The function must return a `logging.Logger` object.

**Dependencies:** `logging`, `logging.handlers`, `json`, `os`, `datetime`, `pathlib`
(all stdlib)

**Test cases:**

- `@pytest.mark.unit` — Call `get_logger("test_logger")`. Assert the returned object
  is an instance of `logging.Logger`.
- `@pytest.mark.unit` — Call `get_logger("test_dup")` twice. Assert the logger has
  the same number of handlers after the second call as after the first (no duplicates).
- `@pytest.mark.unit` — Using `unittest.mock.patch` on the stream handler's `emit`
  method (or by using a `logging.handlers.MemoryHandler` as a test handler), call
  `logger.info("hello", extra={"lease_id": "123"})`. Capture the output and assert it
  is valid JSON containing keys `"timestamp"`, `"level"`, `"logger"`, `"message"`,
  and `"lease_id"`.
- `@pytest.mark.unit` — Assert the `"level"` field in the JSON output equals `"INFO"`
  for an `info`-level message.
- `@pytest.mark.unit` — Set `os.environ["KGS_LOG_LEVEL"] = "WARNING"`. Obtain a fresh
  logger and assert its effective level is `logging.WARNING`. Clean up the env var.

**Definition of done:** `JsonFormatter` and `get_logger` implemented, all tests pass,
`ruff` and `mypy` report no errors. `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 03: YAML logging configuration file

**File:** `logging_config.yaml` (project root)

**Description:**
Write a YAML file at the project root following the Python `logging.config.dictConfig`
schema (version 1). This file is an operator-facing configuration alternative to the
programmatic setup in `logging_utils.py`. It must define:

- **version**: `1`
- **disable_existing_loggers**: `false`
- **formatters**:
  - `json_formatter`: uses `kgs_pipeline.logging_utils.JsonFormatter`
  - `simple`: a plain-text formatter for development convenience, format
    `"%(asctime)s %(levelname)s %(name)s %(message)s"`
- **handlers**:
  - `console`: `StreamHandler` using `json_formatter`, level `INFO`
  - `file`: `RotatingFileHandler` writing to `logs/pipeline.log`, max bytes 10485760
    (10 MB), backup count 5, using `json_formatter`, level `DEBUG`
- **loggers**:
  - `kgs_pipeline`: level `INFO`, handlers `[console, file]`, propagate `false`
- **root**: level `WARNING`, handlers `[console]`

Document at the top of the YAML file (as a YAML comment) how to load this config in
Python:
```yaml
# Load with:
#   import logging.config, yaml
#   with open("logging_config.yaml") as f:
#       logging.config.dictConfig(yaml.safe_load(f))
```

**Dependencies:** None (YAML file, no Python imports).

**Test cases:**

- `@pytest.mark.unit` — Load `logging_config.yaml` using `yaml.safe_load`. Assert the
  result is a `dict` containing keys `"version"`, `"formatters"`, `"handlers"`,
  `"loggers"`.
- `@pytest.mark.unit` — Assert `result["version"] == 1`.
- `@pytest.mark.unit` — Assert `"kgs_pipeline"` is a key in `result["loggers"]`.
- `@pytest.mark.unit` — Assert `result["handlers"]["file"]["filename"]` equals
  `"logs/pipeline.log"`.

**Definition of done:** `logging_config.yaml` written at the project root, all tests
pass. `requirements.txt` updated to include `pyyaml` (needed to load the YAML in
tests and optionally at runtime).

---

## Task 04: Project scaffold (pyproject.toml, Makefile, .gitignore)

**Files:** `pyproject.toml`, `Makefile`, `.gitignore`, `kgs_pipeline/__init__.py`,
`tests/__init__.py`, directory stubs

**Description:**
Create the full project scaffold. This task is a prerequisite for all other tasks and
must be implemented first. (Also specified in `acquire_tasks.md` Task 01 — implement
once, not twice.)

1. **`kgs_pipeline/__init__.py`** — empty file; sets `__version__ = "0.1.0"`.

2. **`pyproject.toml`** with:
   - `[build-system]`: `requires = ["setuptools>=68", "wheel"]`,
     `build-backend = "setuptools.build_meta"`. Never use
     `"setuptools.backends.legacy:build"`.
   - `[project]`: `name = "kgs-pipeline"`, `version = "0.1.0"`,
     `requires-python = ">=3.11"`.
   - `[project.dependencies]`: `requests`, `beautifulsoup4`, `lxml`, `dask[complete]`,
     `pandas`, `pyarrow`, `scikit-learn`, `numpy`, `urllib3`, `pyyaml`.
   - `[project.optional-dependencies]` `dev` group: `pytest`, `pytest-mock`, `ruff`,
     `mypy`, `pandas-stubs`, `types-requests`.
   - `[project.scripts]`:
     - `kgs-acquire = "kgs_pipeline.acquire:main"`
     - `kgs-ingest = "kgs_pipeline.ingest:main"`
     - `kgs-transform = "kgs_pipeline.transform:main"`
     - `kgs-features = "kgs_pipeline.features:main"`
     - `kgs-pipeline = "kgs_pipeline.pipeline:main"`
   - `[tool.setuptools.packages.find]`: `where = ["."]` so that `kgs_pipeline` is
     auto-discovered.

3. **`Makefile`** with targets:
   - `env`: `python3 -m venv .venv`
   - `install`: activates `.venv`, runs `pip install --upgrade pip setuptools wheel`,
     then `pip install -e ".[dev]"`. Never use `pip install -r requirements.txt` as the
     sole install step.
   - `test`: `pytest tests/ -v`
   - `lint`: `ruff check kgs_pipeline/ tests/`
   - `typecheck`: `mypy kgs_pipeline/`
   - `clean`: remove `__pycache__`, `*.egg-info`, `.ruff_cache`, `.mypy_cache`, `.pytest_cache`

4. **`.gitignore`** — must include at minimum:
   - `.venv/`
   - `__pycache__/`
   - `*.pyc`
   - `*.egg-info/`
   - `data/` — the full data directory is not committed to source control
   - `large_tool_results/`
   - `conversation_history/`
   - `.DS_Store`
   - `.pytest_cache/`
   - `.ruff_cache/`
   - `.mypy_cache/`
   - `logs/`

5. **`tests/__init__.py`** — empty file.

6. **Directory stubs**:
   - `data/raw/.gitkeep`
   - `data/interim/.gitkeep`
   - `data/processed/.gitkeep`
   - `data/features/.gitkeep`
   - `data/external/.gitkeep`
   - `logs/.gitkeep`

**Dependencies:** setuptools, pip (build tools only)

**Test cases:**

- `@pytest.mark.unit` — Assert `pyproject.toml` can be read as valid TOML (using
  `tomllib` from stdlib in Python 3.11+) and that `tool.build-system.build-backend`
  equals `"setuptools.build_meta"`.
- `@pytest.mark.unit` — Assert all five console scripts are declared in `pyproject.toml`
  under `[project.scripts]`: `kgs-acquire`, `kgs-ingest`, `kgs-transform`,
  `kgs-features`, `kgs-pipeline`.
- `@pytest.mark.unit` — Assert `"pandas-stubs"` and `"types-requests"` appear in the
  `dev` optional-dependencies list.
- `@pytest.mark.unit` — Assert `"data/"`, `"large_tool_results/"`, and
  `"conversation_history/"` are all present as entries in `.gitignore`.

**Definition of done:** All files created, `pip install -e ".[dev]"` completes without
error in a fresh `.venv`, `ruff` and `mypy` report no errors on the empty
`kgs_pipeline/` package. All unit tests pass. `requirements.txt` updated with all
third-party packages imported in this task.
