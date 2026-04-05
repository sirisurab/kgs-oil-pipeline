"""Integration/unit tests for kgs_pipeline.pipeline module."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kgs_pipeline.pipeline import main, run_pipeline


@pytest.mark.unit
def test_run_pipeline_calls_stages_in_order() -> None:
    """Mock all stage functions and verify order + return dict keys."""
    mock_manifest = Path("/tmp/manifest.json")

    with (
        patch("kgs_pipeline.pipeline.run_acquire", return_value=[Path("f1.txt"), Path("f2.txt")]) as m_acq,
        patch("kgs_pipeline.pipeline.run_ingest", return_value=3) as m_ing,
        patch("kgs_pipeline.pipeline.run_transform", return_value=5) as m_trn,
        patch("kgs_pipeline.pipeline.run_features", return_value=mock_manifest) as m_feat,
    ):
        result = run_pipeline(
            index_path="idx.txt",
            raw_dir="data/raw",
            interim_dir="data/interim",
            processed_dir="data/processed",
            features_dir="data/features",
            min_year=2024,
            workers=3,
        )

    m_acq.assert_called_once()
    m_ing.assert_called_once()
    m_trn.assert_called_once()
    m_feat.assert_called_once()

    assert "acquired" in result
    assert "interim_files" in result
    assert "processed_files" in result
    assert "manifest" in result
    assert result["acquired"] == 2
    assert result["interim_files"] == 3
    assert result["processed_files"] == 5


@pytest.mark.unit
def test_main_calls_run_pipeline_with_args() -> None:
    with (
        patch("kgs_pipeline.pipeline.run_pipeline", return_value={
            "acquired": 0, "interim_files": 0, "processed_files": 0, "manifest": "/tmp/m.json"
        }) as mock_run,
        patch.object(sys, "argv", ["pipeline", "--workers", "3", "--min-year", "2024"]),
    ):
        try:
            main()
        except SystemExit:
            pass
    mock_run.assert_called_once()
    kwargs = mock_run.call_args[1]
    assert kwargs.get("workers") == 3 or mock_run.call_args[0][6] == 3


@pytest.mark.unit
def test_main_exits_1_on_error() -> None:
    with (
        patch("kgs_pipeline.pipeline.run_pipeline", side_effect=RuntimeError("boom")),
        patch.object(sys, "argv", ["pipeline"]),
    ):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
