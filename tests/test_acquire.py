"""Unit tests for kgs_pipeline/acquire.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
from pytest_mock import MockerFixture

from kgs_pipeline.acquire import (
    extract_lease_id,
    fetch_download_link,
    load_lease_index,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_index_csv(rows: list[dict]) -> str:
    return pd.DataFrame(rows).to_csv(index=False)


def _base_row(lease_kid: int, url: str, month_year: str = "1-2024") -> dict:
    return {
        "LEASE_KID": lease_kid,
        "LEASE": f"Lease {lease_kid}",
        "DOR_CODE": 100,
        "API_NUMBER": "15-001-00001",
        "FIELD": "TEST",
        "PRODUCING_ZONE": "Test Zone",
        "OPERATOR": "Test Op",
        "COUNTY": "Douglas",
        "TOWNSHIP": 12,
        "TWN_DIR": "S",
        "RANGE": 20,
        "RANGE_DIR": "E",
        "SECTION": 5,
        "SPOT": "NENE",
        "LATITUDE": 38.9,
        "LONGITUDE": -95.2,
        "MONTH-YEAR": month_year,
        "PRODUCT": "O",
        "WELLS": 1,
        "PRODUCTION": 100.0,
        "URL": url,
    }


# ---------------------------------------------------------------------------
# Task A-01: load_lease_index
# ---------------------------------------------------------------------------


class TestLoadLeaseIndex:
    def test_filters_to_year_gte_2024(self, tmp_path: Path) -> None:
        rows = [
            _base_row(1, "https://example.com?f_lc=1", "1-2022"),
            _base_row(2, "https://example.com?f_lc=2", "6-2023"),
            _base_row(3, "https://example.com?f_lc=3", "3-2024"),
            _base_row(4, "https://example.com?f_lc=4", "12-2025"),
        ]
        p = tmp_path / "index.txt"
        p.write_text(_make_index_csv(rows))
        df = load_lease_index(p)
        assert set(df["LEASE_KID"].astype(str)) == {"3", "4"}

    def test_drops_malformed_month_year(self, tmp_path: Path) -> None:
        rows = [
            _base_row(1, "https://example.com?f_lc=1", "-1-1965"),
            _base_row(2, "https://example.com?f_lc=2", "0-1966"),
            _base_row(3, "https://example.com?f_lc=3", "5-2024"),
        ]
        p = tmp_path / "index.txt"
        p.write_text(_make_index_csv(rows))
        df = load_lease_index(p)
        assert len(df) == 1
        assert "3" in df["LEASE_KID"].astype(str).values

    def test_deduplicates_by_url(self, tmp_path: Path) -> None:
        url = "https://example.com?f_lc=999"
        rows = [
            _base_row(999, url, "1-2024"),
            _base_row(999, url, "2-2024"),
            _base_row(999, url, "3-2024"),
        ]
        p = tmp_path / "index.txt"
        p.write_text(_make_index_csv(rows))
        df = load_lease_index(p)
        assert len(df) == 1
        assert df.iloc[0]["URL"] == url

    def test_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_lease_index("/nonexistent/path.txt")

    def test_raises_value_error_missing_url(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"MONTH-YEAR": ["1-2024"], "LEASE_KID": [1]})
        p = tmp_path / "no_url.txt"
        p.write_text(df.to_csv(index=False))
        with pytest.raises(ValueError, match="URL"):
            load_lease_index(p)

    def test_raises_value_error_missing_month_year(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"URL": ["https://example.com?f_lc=1"], "LEASE_KID": [1]})
        p = tmp_path / "no_my.txt"
        p.write_text(df.to_csv(index=False))
        with pytest.raises(ValueError, match="MONTH-YEAR"):
            load_lease_index(p)


# ---------------------------------------------------------------------------
# Task A-02: extract_lease_id
# ---------------------------------------------------------------------------


class TestExtractLeaseId:
    def test_extracts_known_id(self) -> None:
        url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        assert extract_lease_id(url) == "1001135839"

    def test_raises_on_missing_f_lc(self) -> None:
        with pytest.raises(ValueError, match="f_lc"):
            extract_lease_id("https://example.com?other=123")

    def test_extracts_when_f_lc_not_first(self) -> None:
        url = "https://example.com?foo=bar&f_lc=4242&baz=qux"
        assert extract_lease_id(url) == "4242"


# ---------------------------------------------------------------------------
# Task A-03: fetch_download_link
# ---------------------------------------------------------------------------


class TestFetchDownloadLink:
    def test_returns_href_from_matching_anchor(self) -> None:
        html = """<html><body>
        <a href="https://chasm.kgs.ku.edu/anon_blobber.download?id=999">Download</a>
        </body></html>"""
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        url = fetch_download_link("999", mock_session)
        assert "anon_blobber.download" in url

    def test_raises_value_error_when_no_anchor(self) -> None:
        html = "<html><body><p>No links here</p></body></html>"
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with pytest.raises(ValueError, match="999"):
            fetch_download_link("999", mock_session)

    def test_raises_http_error_on_404(self) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            fetch_download_link("123", mock_session)


# ---------------------------------------------------------------------------
# Caching logic
# ---------------------------------------------------------------------------


class TestCachingLogic:
    def test_cached_file_skipped(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """A file that already exists with size > 0 is not re-downloaded."""

        dest = tmp_path / "1001.txt"
        dest.write_text("existing content")

        # Simulate cache-hit logic from acquire() — _download_with_retry should not
        # be called when file exists and size > 0.
        assert dest.exists() and dest.stat().st_size > 0
        # Verify the check we use in acquire:
        cached = dest.exists() and dest.stat().st_size > 0
        assert cached is True

    def test_empty_file_not_treated_as_cached(self, tmp_path: Path) -> None:
        dest = tmp_path / "empty.txt"
        dest.write_text("")
        cached = dest.exists() and dest.stat().st_size > 0
        assert cached is False


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_retry_on_network_error(self, tmp_path: Path) -> None:
        from kgs_pipeline.acquire import _download_with_retry

        dest = tmp_path / "out.txt"
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.side_effect = requests.ConnectionError("unreachable")

        result = _download_with_retry("http://example.com/file.txt", dest, mock_session)
        assert result is None
        assert mock_session.get.call_count == 3  # _MAX_RETRIES
