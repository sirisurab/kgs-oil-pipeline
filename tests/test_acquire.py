"""Tests for kgs_pipeline/acquire.py."""

from __future__ import annotations

import io
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from kgs_pipeline.acquire import (
    download_lease,
    extract_lease_id,
    load_lease_index,
    parse_download_link,
    validate_raw_files,
    with_retry,
)


# ---------------------------------------------------------------------------
# ACQ-01: load_lease_index
# ---------------------------------------------------------------------------

def _make_index_csv(tmp_path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    p = tmp_path / "index.txt"
    df.to_csv(p, index=False)
    return p


def test_load_lease_index_filters_year(tmp_path: Path) -> None:
    rows = [
        {"URL": "http://a.com?f_lc=1", "MONTH-YEAR": "1-2024"},
        {"URL": "http://b.com?f_lc=2", "MONTH-YEAR": "6-2025"},
        {"URL": "http://c.com?f_lc=3", "MONTH-YEAR": "3-2022"},
        {"URL": "http://d.com?f_lc=4", "MONTH-YEAR": "-1-1965"},
        {"URL": "http://e.com?f_lc=5", "MONTH-YEAR": "0-1966"},
    ]
    p = _make_index_csv(tmp_path, rows)
    df = load_lease_index(str(p))
    years_in = set(df["MONTH-YEAR"].str.split("-").str[-1].astype(int))
    assert all(y >= 2024 for y in years_in)
    assert len(df) == 2


def test_load_lease_index_deduplicates_url(tmp_path: Path) -> None:
    rows = [
        {"URL": "http://a.com?f_lc=1", "MONTH-YEAR": "1-2024"},
        {"URL": "http://a.com?f_lc=1", "MONTH-YEAR": "2-2024"},
        {"URL": "http://b.com?f_lc=2", "MONTH-YEAR": "3-2024"},
    ]
    p = _make_index_csv(tmp_path, rows)
    df = load_lease_index(str(p))
    assert len(df) == 2


def test_load_lease_index_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_lease_index("/nonexistent/path/index.txt")


def test_load_lease_index_missing_url_column(tmp_path: Path) -> None:
    rows = [{"MONTH-YEAR": "1-2024", "OTHER": "x"}]
    df = pd.DataFrame(rows)
    p = tmp_path / "bad.txt"
    df.to_csv(p, index=False)
    with pytest.raises(ValueError):
        load_lease_index(str(p))


# ---------------------------------------------------------------------------
# ACQ-02: extract_lease_id
# ---------------------------------------------------------------------------

def test_extract_lease_id_basic() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
    assert extract_lease_id(url) == "1001135839"


def test_extract_lease_id_no_param() -> None:
    with pytest.raises(ValueError, match="f_lc"):
        extract_lease_id("https://example.com/page?other=123")


def test_extract_lease_id_extra_params() -> None:
    url = "https://example.com/page?foo=bar&f_lc=9999&baz=qux"
    assert extract_lease_id(url) == "9999"


# ---------------------------------------------------------------------------
# ACQ-03: parse_download_link
# ---------------------------------------------------------------------------

def test_parse_download_link_absolute() -> None:
    html = '<html><body><a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download?p_file_name=lp1.txt">Download</a></body></html>'
    url = parse_download_link(html, "https://chasm.kgs.ku.edu")
    assert "anon_blobber.download" in url
    assert url.startswith("https://")


def test_parse_download_link_no_match() -> None:
    html = "<html><body><a href='http://other.com/page'>Other</a></body></html>"
    with pytest.raises(ValueError):
        parse_download_link(html, "https://chasm.kgs.ku.edu")


def test_parse_download_link_relative() -> None:
    html = '<html><body><a href="/ords/anon_blobber.download?p_file_name=lp1.txt">Download</a></body></html>'
    url = parse_download_link(html, "https://chasm.kgs.ku.edu")
    assert url.startswith("https://chasm.kgs.ku.edu")


# ---------------------------------------------------------------------------
# ACQ-04: download_lease (mocked)
# ---------------------------------------------------------------------------

def test_download_lease_success(tmp_path: Path) -> None:
    month_html = '<html><a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download?p_file_name=lp42.txt">Get</a></html>'
    data_content = b"col1,col2\nval1,val2\n"

    mock_session = MagicMock()
    month_resp = MagicMock()
    month_resp.status_code = 200
    month_resp.text = month_html

    data_resp = MagicMock()
    data_resp.status_code = 200
    data_resp.content = data_content

    mock_session.get.side_effect = [month_resp, data_resp]

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=42"
    with patch("kgs_pipeline.acquire.time.sleep"):
        result = download_lease(lease_url, tmp_path, mock_session)

    assert result is not None
    assert result == tmp_path / "lp42.txt"
    assert result.exists()


def test_download_lease_idempotent(tmp_path: Path) -> None:
    existing = tmp_path / "lp42.txt"
    existing.write_bytes(b"existing content")

    month_html = '<html><a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download?p_file_name=lp42.txt">Get</a></html>'
    mock_session = MagicMock()
    month_resp = MagicMock()
    month_resp.status_code = 200
    month_resp.text = month_html
    mock_session.get.return_value = month_resp

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=42"
    result = download_lease(lease_url, tmp_path, mock_session)

    # Should return existing path without downloading
    assert result == existing
    # Only one call (MonthSave), no data download
    assert mock_session.get.call_count == 1


def test_download_lease_no_download_link(tmp_path: Path) -> None:
    month_html = "<html><body>No links here</body></html>"
    mock_session = MagicMock()
    month_resp = MagicMock()
    month_resp.status_code = 200
    month_resp.text = month_html
    mock_session.get.return_value = month_resp

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=99"
    result = download_lease(lease_url, tmp_path, mock_session)
    assert result is None
    assert not any(tmp_path.iterdir())


def test_download_lease_data_500(tmp_path: Path) -> None:
    month_html = '<html><a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download?p_file_name=lp77.txt">Get</a></html>'
    mock_session = MagicMock()
    month_resp = MagicMock()
    month_resp.status_code = 200
    month_resp.text = month_html

    data_resp = MagicMock()
    data_resp.status_code = 500
    data_resp.content = b""
    mock_session.get.side_effect = [month_resp, data_resp]

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=77"
    result = download_lease(lease_url, tmp_path, mock_session)
    assert result is None
    assert not (tmp_path / "lp77.txt").exists()


def test_download_lease_empty_content(tmp_path: Path) -> None:
    month_html = '<html><a href="https://chasm.kgs.ku.edu/ords/anon_blobber.download?p_file_name=lp88.txt">Get</a></html>'
    mock_session = MagicMock()
    month_resp = MagicMock()
    month_resp.status_code = 200
    month_resp.text = month_html

    data_resp = MagicMock()
    data_resp.status_code = 200
    data_resp.content = b""
    mock_session.get.side_effect = [month_resp, data_resp]

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=88"
    result = download_lease(lease_url, tmp_path, mock_session)
    assert result is None


# ---------------------------------------------------------------------------
# ACQ-05: with_retry
# ---------------------------------------------------------------------------

def test_retry_success_after_one_failure() -> None:
    calls: list[int] = []

    @with_retry(max_attempts=3, backoff_base=0.0)
    def flaky() -> str:
        calls.append(1)
        if len(calls) < 2:
            raise RuntimeError("fail")
        return "ok"

    with patch("kgs_pipeline.acquire.time.sleep"):
        result = flaky()

    assert result == "ok"
    assert len(calls) == 2


def test_retry_always_fails() -> None:
    calls: list[int] = []

    @with_retry(max_attempts=3, backoff_base=0.0)
    def always_fail() -> None:
        calls.append(1)
        raise ValueError("nope")

    with patch("kgs_pipeline.acquire.time.sleep"):
        with pytest.raises(ValueError, match="nope"):
            always_fail()

    assert len(calls) == 3


def test_retry_max_attempts_one() -> None:
    calls: list[int] = []

    @with_retry(max_attempts=1, backoff_base=0.0)
    def boom() -> None:
        calls.append(1)
        raise RuntimeError("immediate")

    with patch("kgs_pipeline.acquire.time.sleep"):
        with pytest.raises(RuntimeError):
            boom()

    assert len(calls) == 1


# ---------------------------------------------------------------------------
# ACQ-07: validate_raw_files
# ---------------------------------------------------------------------------

def test_validate_raw_files_valid(tmp_path: Path) -> None:
    f = tmp_path / "lease.txt"
    f.write_text("header\ndata row 1\n", encoding="utf-8")
    errors = validate_raw_files(tmp_path)
    assert errors == []


def test_validate_raw_files_zero_byte(tmp_path: Path) -> None:
    f = tmp_path / "empty.txt"
    f.write_bytes(b"")
    errors = validate_raw_files(tmp_path)
    assert len(errors) == 1
    assert "empty.txt" in errors[0]


def test_validate_raw_files_header_only(tmp_path: Path) -> None:
    f = tmp_path / "header_only.txt"
    f.write_text("LEASE_KID,PRODUCT\n", encoding="utf-8")
    errors = validate_raw_files(tmp_path)
    assert len(errors) == 1


def test_validate_raw_files_non_utf8(tmp_path: Path) -> None:
    f = tmp_path / "binary.txt"
    f.write_bytes(b"\xff\xfe binary garbage \x00\x01")
    errors = validate_raw_files(tmp_path)
    assert len(errors) == 1
    assert "binary.txt" in errors[0]


def test_validate_raw_files_dir_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        validate_raw_files(Path("/nonexistent/raw_dir"))
