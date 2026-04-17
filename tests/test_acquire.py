"""Tests for kgs_pipeline/acquire.py."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from kgs_pipeline.acquire import (
    AcquireConfig,
    AcquireSummary,
    download_lease_file,
    extract_lease_id,
    load_lease_index,
    parse_download_link,
    run_acquire,
    validate_raw_files,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_index_csv(tmp_path: Path) -> Path:
    """Create a minimal lease index CSV for testing."""
    content = (
        "LEASE_KID,LEASE,URL,MONTH-YEAR\n"
        "1001,Alpha,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001,1-2024\n"
        "1002,Beta,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1002,3-2025\n"
        "1003,Gamma,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1003,6-2023\n"
        "1004,Delta,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001,2-2022\n"
    )
    p = tmp_path / "index.txt"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Task 01: load_lease_index
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_lease_index_year_filter(sample_index_csv: Path) -> None:
    """Only rows with year >= 2024 are returned."""
    df = load_lease_index(sample_index_csv, min_year=2024)
    years = df["MONTH-YEAR"].str.split("-").str[-1].astype(int)
    assert (years >= 2024).all()
    assert len(df) == 2  # rows 1-2024 and 3-2025


@pytest.mark.unit
def test_load_lease_index_non_numeric_year(tmp_path: Path) -> None:
    """Rows with non-numeric year parts are dropped before filter."""
    content = (
        "LEASE_KID,LEASE,URL,MONTH-YEAR\n"
        "1001,Alpha,https://chasm.kgs.ku.edu/?f_lc=1001,-1-1965\n"
        "1002,Beta,https://chasm.kgs.ku.edu/?f_lc=1002,0-1966\n"
        "1003,Gamma,https://chasm.kgs.ku.edu/?f_lc=1003,1-2024\n"
    )
    p = tmp_path / "idx.txt"
    p.write_text(content, encoding="utf-8")
    df = load_lease_index(p, min_year=2024)
    assert len(df) == 1


@pytest.mark.unit
def test_load_lease_index_url_deduplication(sample_index_csv: Path) -> None:
    """Multiple rows sharing the same URL are deduplicated to one row per URL."""
    df = load_lease_index(sample_index_csv, min_year=2024)
    assert df["URL"].nunique() == len(df)


@pytest.mark.unit
def test_load_lease_index_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_lease_index(tmp_path / "nonexistent.txt", min_year=2024)


@pytest.mark.unit
def test_load_lease_index_missing_url_column(tmp_path: Path) -> None:
    content = "LEASE_KID,MONTH-YEAR\n1001,1-2024\n"
    p = tmp_path / "idx.txt"
    p.write_text(content)
    with pytest.raises(ValueError, match="URL"):
        load_lease_index(p, min_year=2024)


@pytest.mark.unit
def test_load_lease_index_missing_month_year_column(tmp_path: Path) -> None:
    content = "LEASE_KID,URL\n1001,https://example.com/?f_lc=1\n"
    p = tmp_path / "idx.txt"
    p.write_text(content)
    with pytest.raises(ValueError, match="MONTH-YEAR"):
        load_lease_index(p, min_year=2024)


# ---------------------------------------------------------------------------
# Task 02: extract_lease_id
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_lease_id_basic() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
    assert extract_lease_id(url) == "1001135839"


@pytest.mark.unit
def test_extract_lease_id_no_param() -> None:
    with pytest.raises(ValueError, match="f_lc"):
        extract_lease_id("https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=123")


@pytest.mark.unit
def test_extract_lease_id_multiple_params() -> None:
    url = "https://chasm.kgs.ku.edu/?other=abc&f_lc=9876&extra=1"
    assert extract_lease_id(url) == "9876"


# ---------------------------------------------------------------------------
# Task 03: parse_download_link
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_download_link_absolute_url() -> None:
    html = (
        '<html><body>'
        '<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
        '?p_file_name=lp564.txt">Download</a>'
        '</body></html>'
    )
    result = parse_download_link(html, "https://chasm.kgs.ku.edu/ords/")
    assert "anon_blobber.download" in result
    assert result.startswith("https://")


@pytest.mark.unit
def test_parse_download_link_relative_url() -> None:
    html = (
        '<html><body>'
        '<a href="/ords/anon_blobber.download?p_file_name=lp001.txt">Download</a>'
        '</body></html>'
    )
    base = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=999"
    result = parse_download_link(html, base)
    assert result.startswith("https://")
    assert "anon_blobber.download" in result


@pytest.mark.unit
def test_parse_download_link_no_anchor() -> None:
    html = "<html><body><p>No links here</p></body></html>"
    with pytest.raises(ValueError):
        parse_download_link(html, "https://example.com")


# ---------------------------------------------------------------------------
# Task 04: download_lease_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_lease_file_success(tmp_path: Path) -> None:
    """Successful two-step download saves file and returns its path."""
    html_content = (
        '<a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download'
        '?p_file_name=lp564.txt">Download</a>'
    )
    file_content = b"LEASE_KID,MONTH-YEAR\n1001,1-2024\n"

    mock_session = MagicMock()
    mock_session.get.return_value.status_code = 200
    mock_session.get.return_value.raise_for_status = MagicMock()

    responses = [
        MagicMock(status_code=200, text=html_content, raise_for_status=MagicMock()),
        MagicMock(status_code=200, content=file_content, raise_for_status=MagicMock()),
    ]
    mock_session.get.side_effect = responses

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    result = download_lease_file(
        lease_url=lease_url,
        output_dir=tmp_path,
        session=mock_session,
        sleep_seconds=0,
    )
    assert result is not None
    assert result.exists()
    assert result.name == "lp564.txt"


@pytest.mark.unit
def test_download_lease_file_already_exists(tmp_path: Path) -> None:
    """If the file already exists, no HTTP requests are made."""
    existing = tmp_path / "lp564.txt"
    existing.write_text("existing content")

    html_content = (
        '<a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download'
        '?p_file_name=lp564.txt">Download</a>'
    )

    mock_session = MagicMock()
    responses = [
        MagicMock(status_code=200, text=html_content, raise_for_status=MagicMock()),
    ]
    mock_session.get.side_effect = responses

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    result = download_lease_file(
        lease_url=lease_url,
        output_dir=tmp_path,
        session=mock_session,
        sleep_seconds=0,
    )
    # Should return existing path after fetching MonthSave page (to get filename)
    assert result == existing
    # Only one request (MonthSave page), not the download request
    assert mock_session.get.call_count == 1


@pytest.mark.unit
def test_download_lease_file_request_exception(tmp_path: Path) -> None:
    """RequestException on MonthSave request → returns None."""
    import requests

    mock_session = MagicMock()
    mock_session.get.side_effect = requests.RequestException("connection error")

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    result = download_lease_file(
        lease_url=lease_url,
        output_dir=tmp_path,
        session=mock_session,
        sleep_seconds=0,
    )
    assert result is None
    assert not any(tmp_path.iterdir())


@pytest.mark.unit
def test_download_lease_file_no_download_link(tmp_path: Path) -> None:
    """ValueError from parse_download_link → returns None."""
    mock_session = MagicMock()
    mock_session.get.return_value = MagicMock(
        status_code=200,
        text="<html><body>No link</body></html>",
        raise_for_status=MagicMock(),
    )

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    result = download_lease_file(
        lease_url=lease_url,
        output_dir=tmp_path,
        session=mock_session,
        sleep_seconds=0,
    )
    assert result is None


@pytest.mark.unit
def test_download_lease_file_empty_response(tmp_path: Path) -> None:
    """0-byte download response → returns None, no tmp file left."""
    html_content = (
        '<a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download'
        '?p_file_name=lp564.txt">Download</a>'
    )
    mock_session = MagicMock()
    responses = [
        MagicMock(status_code=200, text=html_content, raise_for_status=MagicMock()),
        MagicMock(status_code=200, content=b"", raise_for_status=MagicMock()),
    ]
    mock_session.get.side_effect = responses

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    result = download_lease_file(
        lease_url=lease_url,
        output_dir=tmp_path,
        session=mock_session,
        sleep_seconds=0,
    )
    assert result is None
    assert not any(tmp_path.glob("*.tmp"))


# ---------------------------------------------------------------------------
# Task 05: run_acquire
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_all_success(tmp_path: Path, sample_index_csv: Path) -> None:
    """3 leases downloaded → AcquireSummary.downloaded == 3, failed == 0."""
    with patch("kgs_pipeline.acquire._download_worker") as mock_worker:
        mock_worker.side_effect = [
            tmp_path / "lp001.txt",
            tmp_path / "lp002.txt",
        ]
        # Create fake downloaded files so the summary works
        (tmp_path / "lp001.txt").write_text("header\nrow1\n")
        (tmp_path / "lp002.txt").write_text("header\nrow2\n")

        config = AcquireConfig(
            index_path=sample_index_csv,
            output_dir=tmp_path,
            min_year=2024,
            max_workers=2,
        )
        summary = run_acquire(config)

    assert summary.failed == 0
    assert summary.total == 2  # rows with year >= 2024


@pytest.mark.unit
def test_run_acquire_one_failure(tmp_path: Path, sample_index_csv: Path) -> None:
    """One None result → failed == 1."""
    with patch("kgs_pipeline.acquire._download_worker") as mock_worker:
        mock_worker.side_effect = [tmp_path / "lp001.txt", None]
        (tmp_path / "lp001.txt").write_text("header\nrow1\n")

        config = AcquireConfig(
            index_path=sample_index_csv,
            output_dir=tmp_path,
            min_year=2024,
        )
        summary = run_acquire(config)

    assert summary.failed == 1
    assert summary.downloaded == 1


@pytest.mark.unit
def test_run_acquire_all_fail_raises(tmp_path: Path, sample_index_csv: Path) -> None:
    """All downloads None → RuntimeError raised."""
    with patch("kgs_pipeline.acquire._download_worker") as mock_worker:
        mock_worker.side_effect = [None, None]

        config = AcquireConfig(
            index_path=sample_index_csv,
            output_dir=tmp_path,
            min_year=2024,
        )
        with pytest.raises(RuntimeError):
            run_acquire(config)


# ---------------------------------------------------------------------------
# Task 06: AcquireConfig and AcquireSummary dataclasses
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_acquire_config_defaults(tmp_path: Path) -> None:
    config = AcquireConfig(index_path=tmp_path / "idx.txt", output_dir=tmp_path)
    assert config.min_year == 2024
    assert config.max_workers == 5
    assert config.sleep_seconds == 0.5


@pytest.mark.unit
def test_acquire_summary_attributes(tmp_path: Path) -> None:
    summary = AcquireSummary(total=10, downloaded=8, skipped=1, failed=1, output_dir=tmp_path)
    assert summary.total == 10
    assert summary.downloaded == 8
    assert summary.skipped == 1
    assert summary.failed == 1
    assert summary.output_dir == tmp_path


# ---------------------------------------------------------------------------
# Task 07: validate_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_raw_files_valid(tmp_path: Path) -> None:
    (tmp_path / "lp001.txt").write_text("LEASE_KID,MONTH-YEAR\n1001,1-2024\n", encoding="utf-8")
    failures = validate_raw_files(tmp_path)
    assert failures == []


@pytest.mark.unit
def test_validate_raw_files_empty_file(tmp_path: Path) -> None:
    (tmp_path / "lp001.txt").write_bytes(b"")
    failures = validate_raw_files(tmp_path)
    assert "lp001.txt" in failures


@pytest.mark.unit
def test_validate_raw_files_invalid_utf8(tmp_path: Path) -> None:
    (tmp_path / "lp001.txt").write_bytes(b"\xff\xfe invalid bytes")
    failures = validate_raw_files(tmp_path)
    assert "lp001.txt" in failures


@pytest.mark.unit
def test_validate_raw_files_only_header(tmp_path: Path) -> None:
    (tmp_path / "lp001.txt").write_text("LEASE_KID,MONTH-YEAR\n", encoding="utf-8")
    failures = validate_raw_files(tmp_path)
    assert "lp001.txt" in failures


@pytest.mark.unit
def test_validate_raw_files_directory_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        validate_raw_files(tmp_path / "nonexistent")
