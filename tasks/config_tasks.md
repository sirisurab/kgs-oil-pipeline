# Configuration and Build Environment Tasks

**Artefacts:** `config.yaml`, `pyproject.toml`, `Makefile`, `.gitignore`, `requirements.txt`  
**Governing docs:** build-env-manifest.md, ADR-006, ADR-007

---

## Task C-01: Write config.yaml

**Artefact:** `config.yaml`

**Description:**  
Create `config.yaml` at the project root. The file must contain all top-level sections
and keys defined in build-env-manifest.md. No configurable value may be absent from this
file — no pipeline code may hardcode a value that belongs in config (see
build-env-manifest.md).

Required top-level sections and representative keys:

```
acquire:
  lease_index_path: data/external/oil_leases_2020_present.txt
  data_dict_path: references/kgs_archives_data_dictionary.csv
  raw_dir: data/raw
  target_year: 2024
  max_workers: 5

ingest:
  raw_dir: data/raw
  data_dict_path: references/kgs_monthly_data_dictionary.csv
  interim_dir: data/interim

transform:
  interim_dir: data/interim
  processed_dir: data/processed/transform
  data_dict_path: references/kgs_monthly_data_dictionary.csv

features:
  processed_dir: data/processed/transform
  output_dir: data/processed/features
  data_dict_path: references/kgs_monthly_data_dictionary.csv

dask:
  scheduler: local
  n_workers: 4
  threads_per_worker: 2
  memory_limit: "3GB"
  dashboard_port: 8787

logging:
  log_file: logs/pipeline.log
  level: INFO
```

**Constraints:**
- Configuration structure: governed entirely by build-env-manifest.md.
- All paths must be relative to the project root.
- `max_workers` for acquire must not exceed 5 (task-writer-kgs.md).
- No hardcoded values in pipeline code — all values live in this file.

**Definition of done:** `config.yaml` is present at the project root with all required
sections and keys, ruff and mypy report no errors on pipeline code that reads it.

---

## Task C-02: Write pyproject.toml

**Artefact:** `pyproject.toml`

**Description:**  
Create `pyproject.toml` declaring the project metadata, build backend, runtime
dependencies, development dependencies, and the pipeline console script entry point.

Requirements (all governed by build-env-manifest.md and ADR-007):

- **Build backend:** must be the primary public entry point exposed by the package
  installer — not an internal alias or submodule path (see build-env-manifest.md).
- **Package name:** `kgs-pipeline` (or `kgs_pipeline`).
- **Python version floor:** `>=3.11` — see ADR-007.
- **Runtime dependencies:** must include all packages imported by pipeline modules:
  `dask[distributed]`, `pandas`, `pyarrow`, `requests`, `beautifulsoup4`,
  `pyyaml`, `bokeh` (Dask dashboard — required at runtime, see build-env-manifest.md).
- **Development dependencies:** `pytest`, `pytest-cov`, `mypy`, `ruff`, `types-requests`,
  `pandas-stubs` — see build-env-manifest.md.
- **Console script entry point:** `kgs-pipeline = kgs_pipeline.pipeline:main` — see
  build-env-manifest.md.
- **Static type checking:** mypy must be configured to check `kgs_pipeline/` — see ADR-007.

**Constraints:**
- Build backend: see build-env-manifest.md (primary public entry point, not internal path).
- `bokeh` is a runtime dependency — not dev-only — see build-env-manifest.md.
- Type stub packages (`types-requests`, `pandas-stubs`) belong in dev dependencies — see
  build-env-manifest.md.
- No other test frameworks besides pytest — see ADR-007.

**Definition of done:** `pyproject.toml` is present, `pip install -e .[dev]` succeeds,
`kgs-pipeline --help` runs without error, ruff and mypy report no errors.

---

## Task C-03: Write Makefile

**Artefact:** `Makefile`

**Description:**  
Create a `Makefile` at the project root exposing the following targets (all governed by
build-env-manifest.md):

- `venv` — create a clean virtual environment using the unversioned `python3` interpreter
  (not `python3.11` or any version-specific binary — see build-env-manifest.md).
- `install` — bootstrap the package installer before installing project dependencies
  (do not assume installer is up to date — see build-env-manifest.md). Install all
  runtime and development dependencies.
- `acquire` — run the acquire stage independently.
- `ingest` — run the ingest stage independently.
- `transform` — run the transform stage independently.
- `features` — run the features stage independently.
- `pipeline` — run all four stages in order using exactly one invocation approach: either
  chain stage targets as dependencies with no recipe body, or invoke the entry point once.
  Must not combine both (a pipeline target that both chains stage targets AND invokes the
  entry point runs every stage twice — see build-env-manifest.md).
- `test` — run all tests with pytest.
- `lint` — run ruff check.
- `typecheck` — run mypy on `kgs_pipeline/`.

**Constraints:**
- `venv` target must use `python3` — not a version-specific binary — see build-env-manifest.md.
- `pipeline` target must use exactly one invocation approach — see build-env-manifest.md.
- Build bootstrap must precede dependency install — see build-env-manifest.md.

**Definition of done:** All Makefile targets execute without error in a clean environment,
ruff and mypy targets pass, `make test` runs the full test suite.

---

## Task C-04: Write .gitignore

**Artefact:** `.gitignore`

**Description:**  
Create a `.gitignore` at the project root that excludes all runtime-generated artifacts
from version control. The following paths must be excluded (see build-env-manifest.md):

- `data/` — raw, interim, and processed data files
- `logs/` — runtime log files
- `large_tool_results/`
- `conversation_history/`

Additionally include standard Python excludes:
- `__pycache__/`, `*.pyc`, `*.pyo`
- `.venv/`, `venv/`, `env/`
- `*.egg-info/`, `dist/`, `build/`
- `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`

**Constraints:**
- Required exclusions: governed by build-env-manifest.md.
- No pipeline code or configuration files may be excluded.

**Definition of done:** `.gitignore` is present with all required exclusions. Running
`git status` in a project with data and log directories shows those directories as
untracked and ignored.
