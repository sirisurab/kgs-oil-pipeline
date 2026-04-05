"""Tests for kgs_pipeline.acquire module."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests  # type: ignore[import-untyped]

from kgs_pipeline.acquire import (
    download_file,
    extract_lease_id,
    fetch_month_save_page,
    load_lease_index,
    main,
    parse_download_link,
    run_acquire,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LEASE_INDEX_HEADER = "LEASE KID,URL,OPERATOR,COUNTY,MONTH-YEAR\n"


def _csv_index(rows: list[str]) -> str:
    return _LEASE_INDEX_HEADER + "\n".join(rows) + "\n"


# ---------------------------------------------------------------------------
# Task 03: load_lease_index
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_lease_index_year_filter(tmp_path: Path) -> None:
    """3 rows year=2024, 2 rows year=2023 → 3 rows returned."""
    content = _csv_index(
        [
            "1,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1,OP1,ALLEN,1-2024",
            "2,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=2,OP2,ALLEN,3-2024",
            "3,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=3,OP3,ALLEN,6-2024",
            "4,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=4,OP4,ALLEN,1-2023",
            "5,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=5,OP5,ALLEN,12-2023",
        ]
    )
    index_file = tmp_path / "leases.txt"
    index_file.write_text(content)
    df = load_lease_index(str(index_file), min_year=2024)
    assert len(df) == 3


@pytest.mark.unit
def test_load_lease_index_deduplication(tmp_path: Path) -> None:
    """Duplicate URLs are deduplicated to 1 row."""
    content = _csv_index(
        [
            "1,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1,OP1,ALLEN,1-2024",
            "2,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1,OP2,ALLEN,2-2024",
        ]
    )
    index_file = tmp_path / "leases.txt"
    index_file.write_text(content)
    df = load_lease_index(str(index_file), min_year=2024)
    assert len(df) == 1


@pytest.mark.unit
def test_load_lease_index_negative_month_year(tmp_path: Path) -> None:
    """Row with MONTH-YEAR='-1-1965' is excluded without raising."""
    content = _csv_index(
        [
            "1,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1,OP1,ALLEN,-1-1965",
            "2,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=2,OP2,ALLEN,1-2024",
        ]
    )
    index_file = tmp_path / "leases.txt"
    index_file.write_text(content)
    df = load_lease_index(str(index_file), min_year=2024)
    assert len(df) == 1


@pytest.mark.unit
def test_load_lease_index_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        load_lease_index("/nonexistent/path/leases.txt")


# ---------------------------------------------------------------------------
# Task 04: extract_lease_id
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_lease_id_standard() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
    assert extract_lease_id(url) == "1001135839"


@pytest.mark.unit
def test_extract_lease_id_no_param() -> None:
    with pytest.raises(ValueError):
        extract_lease_id("https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=123")


@pytest.mark.unit
def test_extract_lease_id_empty_value() -> None:
    with pytest.raises(ValueError):
        extract_lease_id("https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=")


@pytest.mark.unit
def test_extract_lease_id_multiple_params() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=x&f_lc=abc123"
    assert extract_lease_id(url) == "abc123"


# ---------------------------------------------------------------------------
# Task 05: fetch_month_save_page and parse_download_link
# ---------------------------------------------------------------------------

_SAMPLE_HTML = (
    "<html><body>"
    '<a href="/ords/qualified.anon_blobber.download?p_file_name=lp564.txt">Download</a>'
    "</body></html>"
)
_SAMPLE_HTML_ABS = (
    "<html><body>"
    '<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt">Download</a>'
    "</body></html>"
)


@pytest.mark.unit
def test_fetch_month_save_page_success() -> None:
    session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.text = _SAMPLE_HTML
    mock_resp.status_code = 200
    mock_resp.raise_for_status.return_value = None
    session.get.return_value = mock_resp

    result = fetch_month_save_page("123", session, max_retries=3)
    assert result == _SAMPLE_HTML
    session.get.assert_called_once()


@pytest.mark.unit
def test_fetch_month_save_page_retries_and_raises() -> None:
    session = MagicMock(spec=requests.Session)
    session.get.side_effect = requests.exceptions.ConnectionError("fail")

    with pytest.raises(RuntimeError):
        fetch_month_save_page("123", session, max_retries=3, backoff_base=0.0)

    assert session.get.call_count == 3


@pytest.mark.unit
def test_parse_download_link_relative() -> None:
    url = parse_download_link(_SAMPLE_HTML, base_url="https://chasm.kgs.ku.edu")
    assert url.startswith("https://chasm.kgs.ku.edu")
    assert "anon_blobber.download" in url


@pytest.mark.unit
def test_parse_download_link_no_link() -> None:
    with pytest.raises(ValueError):
        parse_download_link("<html><body>no link here</body></html>")


@pytest.mark.unit
def test_parse_download_link_absolute() -> None:
    url = parse_download_link(_SAMPLE_HTML_ABS)
    assert (
        url == "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )


# ---------------------------------------------------------------------------
# Task 06: download_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_file_success(tmp_path: Path) -> None:
    session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.content = b"col1,col2\nval1,val2\n"
    mock_resp.status_code = 200
    session.get.return_value = mock_resp

    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    result = download_file(url, str(tmp_path), session, sleep_seconds=0)

    assert result is not None
    assert result.name == "lp564.txt"
    assert result.parent == tmp_path


@pytest.mark.unit
def test_download_file_idempotent(tmp_path: Path) -> None:
    """Existing non-empty file skips network call."""
    existing = tmp_path / "lp564.txt"
    existing.write_bytes(b"existing content")

    session = MagicMock(spec=requests.Session)
    url = "https://example.com/download?p_file_name=lp564.txt"
    result = download_file(url, str(tmp_path), session, sleep_seconds=0)

    session.get.assert_not_called()
    assert result == existing


@pytest.mark.unit
def test_download_file_empty_response(tmp_path: Path) -> None:
    session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.content = b""
    mock_resp.status_code = 200
    session.get.return_value = mock_resp

    url = "https://example.com/download?p_file_name=lp564.txt"
    result = download_file(url, str(tmp_path), session, sleep_seconds=0)
    assert result is None


@pytest.mark.unit
def test_download_file_timeout_returns_none(tmp_path: Path) -> None:
    session = MagicMock(spec=requests.Session)
    session.get.side_effect = requests.exceptions.Timeout("timed out")

    url = "https://example.com/download?p_file_name=lp564.txt"
    result = download_file(url, str(tmp_path), session, sleep_seconds=0)
    assert result is None


@pytest.mark.unit
def test_download_file_404_raises(tmp_path: Path) -> None:
    session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.content = b"not found"
    mock_resp.status_code = 404
    session.get.return_value = mock_resp

    url = "https://example.com/download?p_file_name=lp564.txt"
    with pytest.raises(RuntimeError):
        download_file(url, str(tmp_path), session, sleep_seconds=0)


# ---------------------------------------------------------------------------
# Task 07: run_acquire (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_returns_paths(tmp_path: Path) -> None:
    index_content = _csv_index(
        [
            f"{i},https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc={i},OP,ALLEN,1-2024"
            for i in range(1, 4)
        ]
    )
    index_file = tmp_path / "leases.txt"
    index_file.write_text(index_content)

    fake_path = tmp_path / "lp001.txt"
    fake_path.write_bytes(b"data")

    with patch("kgs_pipeline.acquire._download_one_lease", return_value=fake_path):
        result = run_acquire(str(index_file), str(tmp_path), min_year=2024, max_workers=2)

    assert len(result) == 3


@pytest.mark.unit
def test_run_acquire_filters_none(tmp_path: Path) -> None:
    """One None result (failure) reduces returned list to 2."""
    index_content = _csv_index(
        [
            f"{i},https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc={i},OP,ALLEN,1-2024"
            for i in range(1, 4)
        ]
    )
    index_file = tmp_path / "leases.txt"
    index_file.write_text(index_content)

    fake_path = tmp_path / "lp001.txt"
    fake_path.write_bytes(b"data")

    side_effects = [fake_path, None, fake_path]

    with patch("kgs_pipeline.acquire._download_one_lease", side_effect=side_effects):
        result = run_acquire(str(index_file), str(tmp_path), min_year=2024, max_workers=2)

    assert len(result) == 2


# ---------------------------------------------------------------------------
# Task 08: TR-20 idempotency tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr20b_idempotent_existing_file(tmp_path: Path) -> None:
    """TR-20b: existing file skips get, content is unchanged."""
    existing = tmp_path / "lp001.txt"
    original_content = b"header\ndata row\n"
    existing.write_bytes(original_content)

    session = MagicMock(spec=requests.Session)
    url = "https://example.com/download?p_file_name=lp001.txt"
    result = download_file(url, str(tmp_path), session, sleep_seconds=0)

    session.get.assert_not_called()
    assert existing.read_bytes() == original_content
    assert result == existing


@pytest.mark.unit
def test_tr20c_no_exception_on_existing(tmp_path: Path) -> None:
    """TR-20c: no exception when file already exists."""
    existing = tmp_path / "lp001.txt"
    existing.write_bytes(b"content")

    session = MagicMock(spec=requests.Session)
    url = "https://example.com/download?p_file_name=lp001.txt"
    # Should not raise
    download_file(url, str(tmp_path), session, sleep_seconds=0)


# ---------------------------------------------------------------------------
# Task 09: CLI tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_main_calls_run_acquire(tmp_path: Path) -> None:
    index_file = tmp_path / "leases.txt"
    index_file.write_text(_csv_index([]))

    with patch("kgs_pipeline.acquire.run_acquire", return_value=[]) as mock_run:
        with patch.object(sys, "argv", ["acquire", "--min-year", "2024", "--workers", "3"]):
            try:
                main()
            except SystemExit:
                pass
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs.get("min_year") == 2024 or mock_run.call_args[0][2] == 2024


@pytest.mark.unit
def test_main_exits_1_on_error() -> None:
    with patch("kgs_pipeline.acquire.run_acquire", side_effect=RuntimeError("boom")):
        with patch.object(sys, "argv", ["acquire"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
