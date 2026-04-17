"""Tests for kgs_pipeline/pipeline.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kgs_pipeline.pipeline import _parse_args, main


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_args_specific_stages() -> None:
    """--stages acquire ingest → stages list is ['acquire', 'ingest']."""
    args = _parse_args(["--stages", "acquire", "ingest"])
    assert args.stages == ["acquire", "ingest"]


@pytest.mark.unit
def test_parse_args_default_all_stages() -> None:
    """No --stages argument → args.stages is None (defaults to all four)."""
    args = _parse_args([])
    assert args.stages is None


@pytest.mark.unit
def test_parse_args_single_stage() -> None:
    args = _parse_args(["--stages", "transform"])
    assert args.stages == ["transform"]


# ---------------------------------------------------------------------------
# Stage skipping / selection
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_main_skips_unconfigured_stages(tmp_path: Path) -> None:
    """Only requested stages are called; others are not invoked."""
    cfg = {
        "acquire": {"index_path": "data/external/x.txt", "output_dir": str(tmp_path / "raw")},
        "ingest": {
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "interim"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "transform": {
            "input_dir": str(tmp_path / "interim"),
            "output_dir": str(tmp_path / "transformed"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "features": {
            "input_dir": str(tmp_path / "transformed"),
            "output_dir": str(tmp_path / "processed"),
            "manifest_path": str(tmp_path / "processed/manifest.json"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "WARNING"},
    }

    with (
        patch("kgs_pipeline.pipeline._load_config", return_value=cfg),
        patch("kgs_pipeline.pipeline._run_acquire_stage") as mock_acq,
        patch("kgs_pipeline.pipeline._run_ingest_stage") as mock_ing,
        patch("kgs_pipeline.pipeline._run_transform_stage") as mock_tr,
        patch("kgs_pipeline.pipeline._run_features_stage") as mock_ft,
        patch("kgs_pipeline.pipeline._init_dask_client", return_value=MagicMock()),
        patch("kgs_pipeline.pipeline._write_pipeline_report"),
    ):
        main(["--stages", "acquire", "ingest"])

    mock_acq.assert_called_once()
    mock_ing.assert_called_once()
    mock_tr.assert_not_called()
    mock_ft.assert_not_called()


@pytest.mark.unit
def test_main_all_stages_called(tmp_path: Path) -> None:
    """When no --stages given, all four stages are invoked."""
    cfg = {
        "acquire": {"index_path": "data/external/x.txt", "output_dir": str(tmp_path / "raw")},
        "ingest": {
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "interim"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "transform": {
            "input_dir": str(tmp_path / "interim"),
            "output_dir": str(tmp_path / "transformed"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "features": {
            "input_dir": str(tmp_path / "transformed"),
            "output_dir": str(tmp_path / "processed"),
            "manifest_path": str(tmp_path / "processed/manifest.json"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "WARNING"},
    }

    with (
        patch("kgs_pipeline.pipeline._load_config", return_value=cfg),
        patch("kgs_pipeline.pipeline._run_acquire_stage") as mock_acq,
        patch("kgs_pipeline.pipeline._run_ingest_stage") as mock_ing,
        patch("kgs_pipeline.pipeline._run_transform_stage") as mock_tr,
        patch("kgs_pipeline.pipeline._run_features_stage") as mock_ft,
        patch("kgs_pipeline.pipeline._init_dask_client", return_value=MagicMock()),
        patch("kgs_pipeline.pipeline._write_pipeline_report"),
    ):
        main([])

    mock_acq.assert_called_once()
    mock_ing.assert_called_once()
    mock_tr.assert_called_once()
    mock_ft.assert_called_once()


@pytest.mark.unit
def test_main_stage_failure_stops_pipeline(tmp_path: Path) -> None:
    """When ingest fails, transform and features are not called."""
    cfg = {
        "acquire": {"index_path": "data/external/x.txt", "output_dir": str(tmp_path / "raw")},
        "ingest": {
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "interim"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "transform": {
            "input_dir": str(tmp_path / "interim"),
            "output_dir": str(tmp_path / "transformed"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "features": {
            "input_dir": str(tmp_path / "transformed"),
            "output_dir": str(tmp_path / "processed"),
            "manifest_path": str(tmp_path / "processed/manifest.json"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "WARNING"},
    }

    with (
        patch("kgs_pipeline.pipeline._load_config", return_value=cfg),
        patch("kgs_pipeline.pipeline._run_acquire_stage"),
        patch("kgs_pipeline.pipeline._run_ingest_stage", side_effect=RuntimeError("ingest failed")),
        patch("kgs_pipeline.pipeline._run_transform_stage") as mock_tr,
        patch("kgs_pipeline.pipeline._run_features_stage") as mock_ft,
        patch("kgs_pipeline.pipeline._init_dask_client", return_value=MagicMock()),
        patch("kgs_pipeline.pipeline._write_pipeline_report"),
    ):
        main([])

    mock_tr.assert_not_called()
    mock_ft.assert_not_called()


@pytest.mark.unit
def test_main_writes_pipeline_report(tmp_path: Path) -> None:
    """Pipeline report is written after run."""
    report_path = tmp_path / "processed" / "pipeline_report.json"
    cfg = {
        "acquire": {"index_path": "data/external/x.txt", "output_dir": str(tmp_path / "raw")},
        "ingest": {
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "interim"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "transform": {
            "input_dir": str(tmp_path / "interim"),
            "output_dir": str(tmp_path / "transformed"),
            "dict_path": "references/kgs_monthly_data_dictionary.csv",
        },
        "features": {
            "input_dir": str(tmp_path / "transformed"),
            "output_dir": str(tmp_path / "processed"),
            "manifest_path": str(tmp_path / "processed/manifest.json"),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "WARNING"},
    }

    captured_report: dict = {}

    def capture_report(report: dict, path: Path) -> None:
        captured_report.update(report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report), encoding="utf-8")

    with (
        patch("kgs_pipeline.pipeline._load_config", return_value=cfg),
        patch("kgs_pipeline.pipeline._run_acquire_stage"),
        patch("kgs_pipeline.pipeline._run_ingest_stage"),
        patch("kgs_pipeline.pipeline._run_transform_stage"),
        patch("kgs_pipeline.pipeline._run_features_stage"),
        patch("kgs_pipeline.pipeline._init_dask_client", return_value=MagicMock()),
        patch("kgs_pipeline.pipeline._write_pipeline_report", side_effect=capture_report),
    ):
        main([])

    assert "stages_run" in captured_report
    assert "stages_succeeded" in captured_report
    assert "stages_failed" in captured_report
    assert "total_duration_seconds" in captured_report
    assert report_path.exists()
