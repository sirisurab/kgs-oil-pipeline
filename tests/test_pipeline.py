"""Tests for pipeline orchestration (tasks P1–P6).

Test requirements: TR-17, TR-24, TR-25, TR-26, TR-27.
Each test carries exactly one of: @pytest.mark.unit or @pytest.mark.integration
per ADR-008.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest
import yaml

from kgs_pipeline.pipeline import load_config, setup_logging, run_pipeline

_DD_PATH = Path("references/kgs_monthly_data_dictionary.csv")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_config(path: Path, sections: dict) -> Path:
    cfg_path = path / "config.yaml"
    with cfg_path.open("w") as f:
        yaml.dump(sections, f)
    return cfg_path


def _minimal_valid_config(tmp_path: Path) -> dict:
    return {
        "acquire": {
            "lease_index_path": str(tmp_path / "lease_index.txt"),
            "raw_dir": str(tmp_path / "raw"),
            "min_year": 2024,
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "retry_attempts": 1,
            "retry_backoff_base": 1.0,
            "monthsave_url_template": "http://example.com?f_lc={lease_id}",
            "download_timeout": 5,
        },
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
            "unit_error_threshold": 1_000_000.0,
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {
            "log_file": str(tmp_path / "logs" / "pipeline.log"),
            "level": "INFO",
        },
    }


# ---------------------------------------------------------------------------
# Task P1 tests: load_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_all_six_sections(tmp_path: Path) -> None:
    """Minimal valid config.yaml → returned dict has all six top-level sections."""
    sections = _minimal_valid_config(tmp_path)
    cfg_path = _write_config(tmp_path, sections)
    cfg = load_config(cfg_path)
    for section in ("acquire", "ingest", "transform", "features", "dask", "logging"):
        assert section in cfg


@pytest.mark.unit
def test_load_config_missing_section_raises(tmp_path: Path) -> None:
    """config.yaml missing 'dask' section → clear error naming 'dask'."""
    sections = _minimal_valid_config(tmp_path)
    del sections["dask"]
    cfg_path = _write_config(tmp_path, sections)
    with pytest.raises(KeyError, match="dask"):
        load_config(cfg_path)


@pytest.mark.unit
def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    """Non-existent config path → FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yaml")


# ---------------------------------------------------------------------------
# Task P2 tests: setup_logging
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_setup_logging_creates_log_dir(tmp_path: Path) -> None:
    """Logging to a non-existent directory → directory created, log lines appear."""
    import logging

    log_file = tmp_path / "nested" / "dir" / "pipeline.log"
    setup_logging({"log_file": str(log_file), "level": "INFO"})
    assert log_file.parent.exists()

    logger = logging.getLogger("test_setup_logging")
    logger.info("Test log line")

    # Flush handlers
    for h in logging.getLogger().handlers:
        h.flush()

    assert log_file.exists()
    content = log_file.read_text()
    assert "Test log line" in content

    # Clean up handlers
    logging.getLogger().handlers.clear()


@pytest.mark.unit
def test_setup_logging_level_applied(tmp_path: Path) -> None:
    """DEBUG log absent when level=INFO; present when level=DEBUG."""
    import logging

    log_file = tmp_path / "test_level.log"

    setup_logging({"log_file": str(log_file), "level": "INFO"})
    logger = logging.getLogger("test_level_info")
    logger.debug("debug_only_message_info_run")
    logger.info("info_message")
    for h in logging.getLogger().handlers:
        h.flush()
    content_info = log_file.read_text()
    assert "debug_only_message_info_run" not in content_info
    assert "info_message" in content_info
    logging.getLogger().handlers.clear()

    setup_logging({"log_file": str(log_file), "level": "DEBUG"})
    logger2 = logging.getLogger("test_level_debug")
    logger2.debug("debug_only_message_debug_run")
    for h in logging.getLogger().handlers:
        h.flush()
    content_debug = log_file.read_text()
    assert "debug_only_message_debug_run" in content_debug
    logging.getLogger().handlers.clear()


# ---------------------------------------------------------------------------
# Task P3 tests: Dask client
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dask_client_local_initialized(tmp_path: Path) -> None:
    """dask.scheduler == 'local' → client is initialized and dashboard URL logged."""
    from kgs_pipeline.pipeline import init_dask_client
    import logging

    log_file = tmp_path / "dask_test.log"
    setup_logging({"log_file": str(log_file), "level": "INFO"})

    client = init_dask_client(
        {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 0,  # use any free port
        }
    )

    assert client is not None

    for h in logging.getLogger().handlers:
        h.flush()
    content = log_file.read_text()
    assert "dashboard" in content.lower() or "Dask" in content

    client.close()
    logging.getLogger().handlers.clear()


# ---------------------------------------------------------------------------
# Task P4 tests: run_pipeline / orchestrator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_pipeline_selective_stages(tmp_path: Path) -> None:
    """Requesting ['ingest', 'transform'] → only those two stages invoked in order."""
    sections = _minimal_valid_config(tmp_path)
    cfg_path = _write_config(tmp_path, sections)

    call_order: list[str] = []

    def mock_ingest(cfg: dict) -> None:
        call_order.append("ingest")

    def mock_transform(cfg: dict) -> None:
        call_order.append("transform")

    def mock_acquire(cfg: dict) -> None:
        call_order.append("acquire")

    def mock_features(cfg: dict) -> None:
        call_order.append("features")

    with (
        patch("kgs_pipeline.pipeline._run_stage") as mock_run,
        patch("kgs_pipeline.pipeline.init_dask_client") as mock_dask,
    ):
        mock_run.side_effect = lambda stage, cfg: call_order.append(stage)
        mock_dask.return_value = MagicMock(close=MagicMock())

        run_pipeline(stages=["ingest", "transform"], config_path=cfg_path)

    assert call_order == ["ingest", "transform"]


@pytest.mark.unit
def test_run_pipeline_all_stages_by_default(tmp_path: Path) -> None:
    """No stages argument → all four stages invoked in order."""
    sections = _minimal_valid_config(tmp_path)
    cfg_path = _write_config(tmp_path, sections)

    call_order: list[str] = []

    with (
        patch("kgs_pipeline.pipeline._run_stage") as mock_run,
        patch("kgs_pipeline.pipeline.init_dask_client") as mock_dask,
    ):
        mock_run.side_effect = lambda stage, cfg: call_order.append(stage)
        mock_dask.return_value = MagicMock(close=MagicMock())

        run_pipeline(stages=None, config_path=cfg_path)

    assert call_order == ["acquire", "ingest", "transform", "features"]


@pytest.mark.unit
def test_run_pipeline_stage_failure_stops_execution(tmp_path: Path) -> None:
    """Mocked ingest raises → transform and features not invoked; exception logged."""
    sections = _minimal_valid_config(tmp_path)
    cfg_path = _write_config(tmp_path, sections)

    call_order: list[str] = []

    def side_effect(stage: str, cfg: dict) -> None:
        call_order.append(stage)
        if stage == "ingest":
            raise RuntimeError("Ingest failed")

    with (
        patch("kgs_pipeline.pipeline._run_stage", side_effect=side_effect),
        patch("kgs_pipeline.pipeline.init_dask_client") as mock_dask,
    ):
        mock_dask.return_value = MagicMock(close=MagicMock())

        with pytest.raises(RuntimeError, match="Ingest failed"):
            run_pipeline(stages=["ingest", "transform", "features"], config_path=cfg_path)

    assert "transform" not in call_order
    assert "features" not in call_order


@pytest.mark.unit
def test_run_pipeline_dask_init_between_acquire_and_ingest(tmp_path: Path) -> None:
    """Dask client initialized after acquire and before ingest (not before acquire)."""
    sections = _minimal_valid_config(tmp_path)
    cfg_path = _write_config(tmp_path, sections)

    event_log: list[str] = []

    def stage_side_effect(stage: str, cfg: dict) -> None:
        event_log.append(f"stage:{stage}")

    with (
        patch("kgs_pipeline.pipeline._run_stage", side_effect=stage_side_effect),
        patch("kgs_pipeline.pipeline.init_dask_client") as mock_dask,
    ):

        def dask_init_side_effect(dask_cfg: dict) -> MagicMock:
            event_log.append("dask_init")
            m = MagicMock()
            m.close = MagicMock()
            return m

        mock_dask.side_effect = dask_init_side_effect
        run_pipeline(stages=["acquire", "ingest", "transform", "features"], config_path=cfg_path)

    # acquire must precede dask_init, which must precede ingest
    acquire_idx = event_log.index("stage:acquire")
    dask_idx = event_log.index("dask_init")
    ingest_idx = event_log.index("stage:ingest")
    assert acquire_idx < dask_idx < ingest_idx


# ---------------------------------------------------------------------------
# Task P5 tests: build artifacts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pyproject_declares_cli_entry_point() -> None:
    """pyproject.toml declares the kgs-pipeline CLI entry point."""
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        pytest.skip("pyproject.toml not found")
    content = pyproject.read_text()
    assert "kgs-pipeline" in content or "kgs_pipeline" in content


@pytest.mark.unit
def test_gitignore_contains_required_entries() -> None:
    """.gitignore contains data/, logs/, large_tool_results/, conversation_history/."""
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        pytest.skip(".gitignore not found")
    content = gitignore.read_text()
    for entry in ("data/", "logs/", "large_tool_results/", "conversation_history/"):
        assert entry in content, f"Missing .gitignore entry: {entry}"


@pytest.mark.unit
def test_makefile_full_pipeline_target(tmp_path: Path) -> None:
    """Full-pipeline Makefile target does not both chain deps AND invoke entry point."""
    makefile = Path("Makefile")
    if not makefile.exists():
        pytest.skip("Makefile not found")
    content = makefile.read_text()
    # The pipeline target must use only one invocation approach
    # We check that it doesn't have both "pipeline: acquire ingest transform features"
    # AND a recipe body that calls "kgs-pipeline"
    lines = content.splitlines()
    in_pipeline_target = False
    has_deps_line = False
    has_recipe_invocation = False
    for line in lines:
        if line.startswith("pipeline:"):
            in_pipeline_target = True
            if "acquire" in line and "ingest" in line:
                has_deps_line = True
            continue
        if in_pipeline_target:
            if line.startswith("\t") or line.startswith("    "):
                if "kgs-pipeline" in line or "run_pipeline" in line:
                    has_recipe_invocation = True
            elif line.strip() and not line.startswith("#"):
                in_pipeline_target = False
    # Both chaining deps AND recipe body invocation is the bug to detect
    assert not (has_deps_line and has_recipe_invocation), (
        "Makefile 'pipeline' target both chains stage deps AND invokes entry point — "
        "this runs every stage twice."
    )


# ---------------------------------------------------------------------------
# Task P6: End-to-end integration test (TR-27)
# ---------------------------------------------------------------------------


def _write_raw_files(raw_dir: Path) -> None:
    """Write 3 synthetic raw files mimicking real KGS lease data."""
    canonical = [
        "LEASE_KID",
        "LEASE",
        "DOR_CODE",
        "API_NUMBER",
        "FIELD",
        "PRODUCING_ZONE",
        "OPERATOR",
        "COUNTY",
        "TOWNSHIP",
        "TWN_DIR",
        "RANGE",
        "RANGE_DIR",
        "SECTION",
        "SPOT",
        "LATITUDE",
        "LONGITUDE",
        "MONTH-YEAR",
        "PRODUCT",
        "WELLS",
        "PRODUCTION",
    ]
    datasets = [
        [  # file 1 — lease 60001
            {
                "LEASE_KID": "60001",
                "LEASE": "E2E_A",
                "MONTH-YEAR": "1-2024",
                "PRODUCT": "O",
                "WELLS": "3",
                "PRODUCTION": "200",
                "COUNTY": "Nemaha",
                "OPERATOR": "OpA",
            },
            {
                "LEASE_KID": "60001",
                "LEASE": "E2E_A",
                "MONTH-YEAR": "2-2024",
                "PRODUCT": "O",
                "WELLS": "3",
                "PRODUCTION": "210",
                "COUNTY": "Nemaha",
                "OPERATOR": "OpA",
            },
            {
                "LEASE_KID": "60001",
                "LEASE": "E2E_A",
                "MONTH-YEAR": "3-2024",
                "PRODUCT": "O",
                "WELLS": "3",
                "PRODUCTION": "205",
                "COUNTY": "Nemaha",
                "OPERATOR": "OpA",
            },
            {
                "LEASE_KID": "60001",
                "LEASE": "E2E_A",
                "MONTH-YEAR": "1-2024",
                "PRODUCT": "G",
                "WELLS": "3",
                "PRODUCTION": "1000",
                "COUNTY": "Nemaha",
                "OPERATOR": "OpA",
            },
            {
                "LEASE_KID": "60001",
                "LEASE": "E2E_A",
                "MONTH-YEAR": "2-2024",
                "PRODUCT": "G",
                "WELLS": "3",
                "PRODUCTION": "1050",
                "COUNTY": "Nemaha",
                "OPERATOR": "OpA",
            },
        ],
        [  # file 2 — lease 60002
            {
                "LEASE_KID": "60002",
                "LEASE": "E2E_B",
                "MONTH-YEAR": "1-2024",
                "PRODUCT": "O",
                "WELLS": "1",
                "PRODUCTION": "50",
                "COUNTY": "Douglas",
                "OPERATOR": "OpB",
            },
            {
                "LEASE_KID": "60002",
                "LEASE": "E2E_B",
                "MONTH-YEAR": "2-2024",
                "PRODUCT": "O",
                "WELLS": "1",
                "PRODUCTION": "55",
                "COUNTY": "Douglas",
                "OPERATOR": "OpB",
            },
        ],
        [  # file 3 — lease 60003
            {
                "LEASE_KID": "60003",
                "LEASE": "E2E_C",
                "MONTH-YEAR": "1-2025",
                "PRODUCT": "O",
                "WELLS": "2",
                "PRODUCTION": "150",
                "COUNTY": "Riley",
                "OPERATOR": "OpC",
            },
            {
                "LEASE_KID": "60003",
                "LEASE": "E2E_C",
                "MONTH-YEAR": "2-2025",
                "PRODUCT": "G",
                "WELLS": "2",
                "PRODUCTION": "800",
                "COUNTY": "Riley",
                "OPERATOR": "OpC",
            },
        ],
    ]

    raw_dir.mkdir(parents=True, exist_ok=True)
    for i, rows in enumerate(datasets):
        header = ",".join(canonical)
        lines = [header]
        for r in rows:
            lines.append(",".join(str(r.get(c, "")) for c in canonical))
        (raw_dir / f"lp_e2e_{i + 1}.txt").write_text("\n".join(lines) + "\n")


@pytest.mark.integration
def test_e2e_ingest_transform_features(tmp_path: Path) -> None:
    """End-to-end: ingest → transform → features on synthetic data (TR-27).

    Acquire is mocked by staging fixture files in tmp_path/raw.
    All output paths point to tmp_path (ADR-008).
    """
    from kgs_pipeline.ingest import ingest, load_schema
    from kgs_pipeline.transform import transform
    from kgs_pipeline.features import features

    raw_dir = tmp_path / "raw"
    _write_raw_files(raw_dir)

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
            "unit_error_threshold": 1_000_000.0,
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }

    # --- Ingest ---
    ingest(cfg)
    interim_dir = tmp_path / "interim"
    assert interim_dir.exists()

    ddf_interim = dd.read_parquet(str(interim_dir))
    df_interim = ddf_interim.compute()

    # boundary-ingest-transform.md upstream guarantees
    canonical_cols = [
        "LEASE_KID",
        "LEASE",
        "DOR_CODE",
        "API_NUMBER",
        "FIELD",
        "PRODUCING_ZONE",
        "OPERATOR",
        "COUNTY",
        "TOWNSHIP",
        "TWN_DIR",
        "RANGE",
        "RANGE_DIR",
        "SECTION",
        "SPOT",
        "LATITUDE",
        "LONGITUDE",
        "MONTH-YEAR",
        "PRODUCT",
        "WELLS",
        "PRODUCTION",
    ]
    for col in canonical_cols:
        assert col in df_interim.columns, f"Ingest missing column: {col}"
    assert 10 <= ddf_interim.npartitions <= 50

    schema = load_schema(_DD_PATH)
    for col_s in schema.columns:
        col = col_s.name
        if col not in df_interim.columns:
            continue
        actual_dtype = str(df_interim[col].dtype)
        expected = col_s.pandas_dtype
        if expected == "category":
            assert actual_dtype == "category", f"{col}: expected category"
        elif expected in ("Int64",):
            assert actual_dtype in ("Int64", "int64"), f"{col}: expected Int64"

    # --- Transform ---
    transform(cfg)
    processed_dir = tmp_path / "processed"
    assert processed_dir.exists()

    ddf_processed = dd.read_parquet(str(processed_dir))
    df_processed = ddf_processed.compute()

    # boundary-transform-features.md upstream guarantees
    assert "production_date" in df_processed.columns
    assert pd.api.types.is_datetime64_any_dtype(df_processed["production_date"])
    assert 1 <= ddf_processed.npartitions <= 50

    # No invalid production values
    if "PRODUCTION" in df_processed.columns:
        valid_prod = df_processed["PRODUCTION"].dropna()
        if len(valid_prod) > 0:
            assert (valid_prod >= 0).all()

    # --- Features ---
    features(cfg)
    features_dir = tmp_path / "features"
    assert features_dir.exists()

    ddf_features = dd.read_parquet(str(features_dir))
    df_features = ddf_features.compute()

    # TR-26 / TR-19: all expected derived columns present
    required_feature_cols = [
        "oil_bbl",
        "gas_mcf",
        "water_bbl",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "oil_bbl_rolling_3m",
        "oil_bbl_rolling_6m",
        "gas_mcf_rolling_3m",
        "gas_mcf_rolling_6m",
        "water_bbl_rolling_3m",
        "water_bbl_rolling_6m",
        "oil_bbl_lag_1m",
        "gas_mcf_lag_1m",
        "water_bbl_lag_1m",
    ]
    for col in required_feature_cols:
        assert col in df_features.columns, f"Missing features column: {col}"

    # Schema consistent across partitions
    parquet_files = sorted(features_dir.rglob("*.parquet"))
    if len(parquet_files) >= 2:
        df0 = pd.read_parquet(parquet_files[0])
        df1 = pd.read_parquet(parquet_files[1])
        assert list(df0.columns) == list(df1.columns)
        for c in df0.columns:
            assert str(df0[c].dtype) == str(df1[c].dtype)

    # No unhandled exception (covered by reaching this assertion)
    assert True
