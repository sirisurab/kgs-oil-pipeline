"""Tests for kgs_pipeline/pipeline.py (Task 32)."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Task 32: run_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_pipeline_all_stages_metadata_keys():
    """run_pipeline(["all"]) returns dict with required metadata keys."""
    from kgs_pipeline.pipeline import run_pipeline

    with (
        patch("kgs_pipeline.pipeline.run_acquire_pipeline", return_value=[Path("data/raw/lp1.txt")]),
        patch("kgs_pipeline.pipeline.run_ingest_pipeline", return_value=[Path("data/interim/p1.parquet")]),
        patch("kgs_pipeline.pipeline.run_transform_pipeline", return_value=[Path("data/processed/p1.parquet")]),
        patch("kgs_pipeline.pipeline.run_features_pipeline", return_value=[Path("data/processed/features/p1.parquet")]),
    ):
        metadata = run_pipeline(["all"], None, 4)

    assert "start_time" in metadata
    assert "end_time" in metadata
    assert "stages" in metadata
    assert "pipeline_metadata_path" in metadata


@pytest.mark.unit
def test_run_pipeline_selective_stages():
    """Only ingest and transform are called when stages=['ingest', 'transform']."""
    from kgs_pipeline.pipeline import run_pipeline

    acquire_mock = MagicMock(return_value=[])
    ingest_mock = MagicMock(return_value=[Path("data/interim/p1.parquet")])
    transform_mock = MagicMock(return_value=[Path("data/processed/p1.parquet")])
    features_mock = MagicMock(return_value=[])

    with (
        patch("kgs_pipeline.pipeline.run_acquire_pipeline", acquire_mock),
        patch("kgs_pipeline.pipeline.run_ingest_pipeline", ingest_mock),
        patch("kgs_pipeline.pipeline.run_transform_pipeline", transform_mock),
        patch("kgs_pipeline.pipeline.run_features_pipeline", features_mock),
    ):
        run_pipeline(["ingest", "transform"], None, 4)

    acquire_mock.assert_not_called()
    ingest_mock.assert_called_once()
    transform_mock.assert_called_once()
    features_mock.assert_not_called()


@pytest.mark.unit
def test_run_pipeline_acquire_failure_marked_failed():
    """If acquire fails, it's marked 'failed' but pipeline continues."""
    from kgs_pipeline.pipeline import run_pipeline

    with (
        patch("kgs_pipeline.pipeline.run_acquire_pipeline", side_effect=RuntimeError("network")),
        patch("kgs_pipeline.pipeline.run_ingest_pipeline", return_value=[]),
        patch("kgs_pipeline.pipeline.run_transform_pipeline", return_value=[]),
        patch("kgs_pipeline.pipeline.run_features_pipeline", return_value=[]),
    ):
        metadata = run_pipeline(["all"], None, 4)

    assert metadata["stages"]["acquire"]["status"] == "failed"
    # Pipeline should continue; ingest still attempted
    assert "ingest" in metadata["stages"]


@pytest.mark.unit
def test_run_pipeline_does_not_raise_on_stage_failure():
    """run_pipeline never raises even when a stage fails."""
    from kgs_pipeline.pipeline import run_pipeline

    with patch("kgs_pipeline.pipeline.run_acquire_pipeline", side_effect=Exception("crash")):
        # Should not raise
        result = run_pipeline(["acquire"], None, 4)
    assert result["stages"]["acquire"]["status"] == "failed"


# ---------------------------------------------------------------------------
# Task 32: main() / CLI
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_main_writes_metadata_json(tmp_path: Path):
    """main() writes data/pipeline_metadata.json."""
    from kgs_pipeline.pipeline import main

    metadata_path = Path("data/pipeline_metadata.json")

    with (
        patch("kgs_pipeline.pipeline.run_pipeline", return_value={
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-01-01T00:01:00+00:00",
            "stages": {"acquire": {"status": "success"}},
            "pipeline_metadata_path": str(metadata_path),
        }),
        patch("sys.argv", ["pipeline.py"]),
        pytest.raises(SystemExit) as exc_info,
    ):
        main()

    assert exc_info.value.code in (0, None)
    assert metadata_path.exists()
    data = json.loads(metadata_path.read_text())
    assert "start_time" in data


@pytest.mark.unit
def test_main_acquire_ingest_only(tmp_path: Path):
    """--stages acquire,ingest only invokes acquire and ingest."""
    from kgs_pipeline.pipeline import main

    acquire_mock = MagicMock(return_value=[])
    ingest_mock = MagicMock(return_value=[])
    transform_mock = MagicMock(return_value=[])
    features_mock = MagicMock(return_value=[])

    with (
        patch("kgs_pipeline.pipeline.run_acquire_pipeline", acquire_mock),
        patch("kgs_pipeline.pipeline.run_ingest_pipeline", ingest_mock),
        patch("kgs_pipeline.pipeline.run_transform_pipeline", transform_mock),
        patch("kgs_pipeline.pipeline.run_features_pipeline", features_mock),
        patch("sys.argv", ["pipeline.py", "--stages", "acquire,ingest"]),
        pytest.raises(SystemExit),
    ):
        main()

    acquire_mock.assert_called_once()
    ingest_mock.assert_called_once()
    transform_mock.assert_not_called()
    features_mock.assert_not_called()


@pytest.mark.unit
def test_main_exit_code_1_on_failure():
    """main() exits with code 1 if any stage has status 'failed'."""
    from kgs_pipeline.pipeline import main

    with (
        patch("kgs_pipeline.pipeline.run_pipeline", return_value={
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-01-01T00:01:00+00:00",
            "stages": {"acquire": {"status": "failed", "error": "boom"}},
            "pipeline_metadata_path": "data/pipeline_metadata.json",
        }),
        patch("sys.argv", ["pipeline.py"]),
        pytest.raises(SystemExit) as exc_info,
    ):
        main()

    assert exc_info.value.code == 1
