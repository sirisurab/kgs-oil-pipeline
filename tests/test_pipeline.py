"""Unit tests for kgs_pipeline/pipeline.py and kgs_pipeline/config.py."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from kgs_pipeline.config import load_config
from kgs_pipeline.pipeline import setup_logging

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Task P-01: load_config
# ---------------------------------------------------------------------------


def _write_valid_config(path: Path) -> None:
    cfg = {
        "acquire": {
            "index_path": "data/external/oil_leases_2020_present.txt",
            "raw_dir": "data/raw",
        },
        "ingest": {"raw_dir": "data/raw", "interim_path": "data/interim", "min_year": 2024},
        "transform": {"interim_path": "data/interim", "processed_path": "data/processed"},
        "features": {"processed_path": "data/processed", "output_path": "data/features"},
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 2,
            "memory_limit": "3GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": "logs/pipeline.log", "level": "INFO"},
    }
    with path.open("w") as fh:
        yaml.safe_dump(cfg, fh)


class TestLoadConfig:
    def test_returns_all_six_sections(self, tmp_path: Path) -> None:
        p = tmp_path / "config.yaml"
        _write_valid_config(p)
        cfg = load_config(p)
        for section in ("acquire", "ingest", "transform", "features", "dask", "logging"):
            assert section in cfg

    def test_raises_value_error_missing_dask(self, tmp_path: Path) -> None:
        p = tmp_path / "config.yaml"
        cfg: dict[str, Any] = {
            "acquire": {},
            "ingest": {},
            "transform": {},
            "features": {},
            "logging": {},
        }
        with p.open("w") as fh:
            yaml.safe_dump(cfg, fh)
        with pytest.raises(ValueError, match="dask"):
            load_config(p)

    def test_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_config("/no/such/config.yaml")

    def test_raises_on_malformed_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text("{invalid yaml: [}")
        with pytest.raises((ValueError, Exception)):
            load_config(p)


# ---------------------------------------------------------------------------
# Task P-02: setup_logging
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def setup_method(self) -> None:
        # Clear root logger handlers before each test
        root = logging.getLogger()
        root.handlers.clear()

    def test_adds_stream_and_file_handler(self, tmp_path: Path) -> None:
        cfg = {
            "logging": {
                "log_file": str(tmp_path / "logs" / "test.log"),
                "level": "INFO",
            }
        }
        with patch("kgs_pipeline.pipeline.logging.FileHandler") as mock_fh:
            setup_logging(cfg)
            mock_fh.assert_called_once_with(cfg["logging"]["log_file"], encoding="utf-8")

    def test_creates_log_directory(self, tmp_path: Path) -> None:
        log_dir = tmp_path / "deep" / "log" / "dir"
        cfg = {
            "logging": {
                "log_file": str(log_dir / "pipeline.log"),
                "level": "DEBUG",
            }
        }
        setup_logging(cfg)
        assert log_dir.exists()

    def test_raises_keyerror_missing_logging_section(self) -> None:
        with pytest.raises(KeyError, match="logging"):
            setup_logging({})

    def teardown_method(self) -> None:
        root = logging.getLogger()
        for h in root.handlers[:]:
            h.close()
        root.handlers.clear()


# ---------------------------------------------------------------------------
# Task P-03: init_scheduler
# ---------------------------------------------------------------------------


class TestInitScheduler:
    def test_returns_client_for_local(self, tmp_path: Path) -> None:
        pytest.importorskip("distributed")
        from kgs_pipeline.pipeline import init_scheduler

        cfg = {
            "dask": {
                "scheduler": "local",
                "n_workers": 1,
                "threads_per_worker": 1,
                "memory_limit": "512MB",
                "dashboard_port": 0,
            }
        }
        client = init_scheduler(cfg)
        try:
            from distributed import Client

            assert isinstance(client, Client)
        finally:
            client.close()

    def test_raises_keyerror_missing_n_workers(self) -> None:
        from kgs_pipeline.pipeline import init_scheduler

        cfg = {
            "dask": {
                "scheduler": "local",
                "threads_per_worker": 1,
                "memory_limit": "512MB",
            }
        }
        with pytest.raises(KeyError):
            init_scheduler(cfg)

    def test_dashboard_url_logged(self, tmp_path: Path, caplog: Any) -> None:
        pytest.importorskip("distributed")
        from kgs_pipeline.pipeline import init_scheduler

        cfg = {
            "dask": {
                "scheduler": "local",
                "n_workers": 1,
                "threads_per_worker": 1,
                "memory_limit": "512MB",
                "dashboard_port": 0,
            }
        }
        with caplog.at_level(logging.INFO, logger="kgs_pipeline.pipeline"):
            client = init_scheduler(cfg)
        try:
            assert any("dashboard" in r.message.lower() for r in caplog.records)
        finally:
            client.close()


# ---------------------------------------------------------------------------
# Task P-04: main (pipeline entry point)
# ---------------------------------------------------------------------------


class TestMain:
    def test_only_acquire_called_when_stage_is_acquire(self, tmp_path: Path) -> None:
        _write_valid_config(tmp_path / "config.yaml")
        with (
            patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
            patch("kgs_pipeline.pipeline.setup_logging"),
            patch("kgs_pipeline.acquire.acquire", return_value=[]) as mock_acquire,
            patch("kgs_pipeline.pipeline.init_scheduler") as mock_sched,
        ):
            from kgs_pipeline.pipeline import main

            main(stages=["acquire"])
            mock_acquire.assert_called_once()
            # Scheduler not initialized when no downstream stages run
            mock_sched.assert_not_called()

    def test_setup_logging_called_first(self, tmp_path: Path) -> None:
        call_order: list[str] = []

        def _fake_logging(cfg: Any) -> None:
            call_order.append("setup_logging")

        def _fake_acquire(cfg: Any) -> None:
            call_order.append("acquire")

        with (
            patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
            patch("kgs_pipeline.pipeline.setup_logging", side_effect=_fake_logging),
            patch("kgs_pipeline.pipeline.init_scheduler", return_value=MagicMock()),
            patch("kgs_pipeline.acquire.acquire", side_effect=_fake_acquire),
        ):
            from kgs_pipeline.pipeline import main

            main(stages=["acquire"])
        assert call_order[0] == "setup_logging"

    def test_exception_stops_downstream_stages(self, tmp_path: Path) -> None:
        with (
            patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
            patch("kgs_pipeline.pipeline.setup_logging"),
            patch("kgs_pipeline.acquire.acquire", side_effect=RuntimeError("acquire failed")),
        ):
            from kgs_pipeline.pipeline import main

            with pytest.raises(RuntimeError, match="acquire failed"):
                main(stages=["acquire", "ingest", "transform"])


def _full_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "acquire": {
            "index_path": "data/external/oil_leases_2020_present.txt",
            "raw_dir": str(tmp_path / "raw"),
        },
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_path": str(tmp_path / "interim"),
            "min_year": 2024,
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "transform": {
            "interim_path": str(tmp_path / "interim"),
            "processed_path": str(tmp_path / "processed"),
        },
        "features": {
            "processed_path": str(tmp_path / "processed"),
            "output_path": str(tmp_path / "features"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 0,
        },
        "logging": {
            "log_file": str(tmp_path / "logs" / "pipeline.log"),
            "level": "INFO",
        },
    }
