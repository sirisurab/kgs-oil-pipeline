"""Tests for kgs_pipeline/acquire.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from kgs_pipeline.acquire import (
    DownloadError,
    download_lease_file,
    extract_lease_id,
    load_lease_index,
    parse_download_link,
    run_acquire,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MONTH_SAVE_HTML = """
<html><body>
<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt">Download</a>
</body></html>
"""

FILE_CONTENT = b"LEASE_KID,MONTH-YEAR\n1001,1-2024\n"


def _make_index_csv(tmp_path: Path, rows: list[dict]) -> str:
    df = pd.DataFrame(rows)
    fpath = tmp_path / "index.txt"
    df.to_csv(fpath, index=False)
    return str(fpath)


# ---------------------------------------------------------------------------
# Task A-02: load_lease_index
# ---------------------------------------------------------------------------


class TestLoadLeaseIndex:
    def test_filters_year_and_invalid_components(self, tmp_path: Path) -> None:
        rows = [
            {"MONTH-YEAR": "1-2024", "URL": "https://example.com?f_lc=1"},
            {"MONTH-YEAR": "6-2023", "URL": "https://example.com?f_lc=2"},
            {"MONTH-YEAR": "3-2022", "URL": "https://example.com?f_lc=3"},
            {"MONTH-YEAR": "3-2025", "URL": "https://example.com?f_lc=4"},
            {"MONTH-YEAR": "-1-1965", "URL": "https://example.com?f_lc=5"},
            {"MONTH-YEAR": "0-1966", "URL": "https://example.com?f_lc=6"},
        ]
        fpath = _make_index_csv(tmp_path, rows)
        urls = load_lease_index(fpath, 2024)
        assert set(urls) == {
            "https://example.com?f_lc=1",
            "https://example.com?f_lc=4",
        }

    def test_deduplication(self, tmp_path: Path) -> None:
        rows = [
            {"MONTH-YEAR": "1-2024", "URL": "https://example.com?f_lc=42"},
            {"MONTH-YEAR": "2-2024", "URL": "https://example.com?f_lc=42"},
            {"MONTH-YEAR": "3-2024", "URL": "https://example.com?f_lc=42"},
        ]
        fpath = _make_index_csv(tmp_path, rows)
        urls = load_lease_index(fpath, 2024)
        assert urls == ["https://example.com?f_lc=42"]

    def test_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_lease_index("/nonexistent/path.txt", 2024)

    def test_missing_url_column(self, tmp_path: Path) -> None:
        rows = [{"MONTH-YEAR": "1-2024", "OTHER": "x"}]
        fpath = _make_index_csv(tmp_path, rows)
        with pytest.raises(KeyError, match="URL"):
            load_lease_index(fpath, 2024)

    def test_missing_month_year_column(self, tmp_path: Path) -> None:
        rows = [{"URL": "https://example.com", "OTHER": "x"}]
        fpath = _make_index_csv(tmp_path, rows)
        with pytest.raises(KeyError, match="MONTH-YEAR"):
            load_lease_index(fpath, 2024)


# ---------------------------------------------------------------------------
# Task A-03: extract_lease_id
# ---------------------------------------------------------------------------


class TestExtractLeaseId:
    def test_valid_url(self) -> None:
        url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        assert extract_lease_id(url) == "1001135839"

    def test_missing_f_lc(self) -> None:
        url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=123"
        with pytest.raises(ValueError):
            extract_lease_id(url)

    def test_empty_f_lc(self) -> None:
        url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="
        with pytest.raises(ValueError):
            extract_lease_id(url)


# ---------------------------------------------------------------------------
# Task A-04: parse_download_link
# ---------------------------------------------------------------------------


class TestParseDownloadLink:
    def test_valid_html(self) -> None:
        href = parse_download_link(MONTH_SAVE_HTML)
        assert "anon_blobber.download" in href
        assert "lp564.txt" in href

    def test_no_download_link(self) -> None:
        html = "<html><body><a href='https://example.com'>nope</a></body></html>"
        with pytest.raises(DownloadError):
            parse_download_link(html)

    def test_empty_string(self) -> None:
        with pytest.raises(DownloadError):
            parse_download_link("")


# ---------------------------------------------------------------------------
# Task A-05: download_lease_file
# ---------------------------------------------------------------------------


class TestDownloadLeaseFile:
    LEASE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"
    MONTH_SAVE_BASE = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"

    def _mock_responses(self) -> tuple[MagicMock, MagicMock]:
        ms_resp = MagicMock()
        ms_resp.text = MONTH_SAVE_HTML
        ms_resp.raise_for_status = MagicMock()

        dl_resp = MagicMock()
        dl_resp.content = FILE_CONTENT
        dl_resp.raise_for_status = MagicMock()
        return ms_resp, dl_resp

    def test_successful_download(self, tmp_path: Path) -> None:
        ms_resp, dl_resp = self._mock_responses()
        with patch("kgs_pipeline.acquire.requests.get", side_effect=[ms_resp, dl_resp]):
            result = download_lease_file(self.LEASE_URL, str(tmp_path), self.MONTH_SAVE_BASE, 0.0)
        assert result is not None
        assert result.exists()
        assert result.stat().st_size > 0

    def test_idempotency_skips_existing(self, tmp_path: Path) -> None:
        # Pre-create the file
        existing = tmp_path / "lp564.txt"
        existing.write_bytes(FILE_CONTENT)
        ms_resp = MagicMock()
        ms_resp.text = MONTH_SAVE_HTML
        ms_resp.raise_for_status = MagicMock()
        with patch("kgs_pipeline.acquire.requests.get", return_value=ms_resp) as mock_get:
            result = download_lease_file(self.LEASE_URL, str(tmp_path), self.MONTH_SAVE_BASE, 0.0)
            mock_get.assert_not_called()
        assert result == existing

    def test_network_error_returns_none(self, tmp_path: Path) -> None:
        import requests as req_lib

        with patch(
            "kgs_pipeline.acquire.requests.get",
            side_effect=req_lib.RequestException("timeout"),
        ):
            result = download_lease_file(self.LEASE_URL, str(tmp_path), self.MONTH_SAVE_BASE, 0.0)
        assert result is None
        assert list(tmp_path.glob("*.txt")) == []

    def test_download_error_returns_none(self, tmp_path: Path) -> None:
        ms_resp = MagicMock()
        ms_resp.text = "<html><body>no link here</body></html>"
        ms_resp.raise_for_status = MagicMock()
        with patch("kgs_pipeline.acquire.requests.get", return_value=ms_resp):
            result = download_lease_file(self.LEASE_URL, str(tmp_path), self.MONTH_SAVE_BASE, 0.0)
        assert result is None

    def test_zero_byte_file_deleted(self, tmp_path: Path) -> None:
        ms_resp, dl_resp = self._mock_responses()
        dl_resp.content = b""  # empty content
        with patch("kgs_pipeline.acquire.requests.get", side_effect=[ms_resp, dl_resp]):
            result = download_lease_file(self.LEASE_URL, str(tmp_path), self.MONTH_SAVE_BASE, 0.0)
        assert result is None
        assert not (tmp_path / "lp564.txt").exists()


# ---------------------------------------------------------------------------
# Task A-06: run_acquire
# ---------------------------------------------------------------------------


class TestRunAcquire:
    def _config(self, tmp_path: Path, index_path: str) -> dict:
        return {
            "lease_index": index_path,
            "raw_dir": str(tmp_path / "raw"),
            "month_save_url": "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave",
            "min_year": 2024,
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }

    def _make_index(self, tmp_path: Path, n: int = 3) -> str:
        rows = [
            {"MONTH-YEAR": "1-2024", "URL": f"https://example.com?f_lc={i}"}
            for i in range(1, n + 1)
        ]
        df = pd.DataFrame(rows)
        fpath = tmp_path / "index.txt"
        df.to_csv(fpath, index=False)
        return str(fpath)

    def test_returns_three_paths(self, tmp_path: Path) -> None:
        index_path = self._make_index(tmp_path)
        config = self._config(tmp_path, index_path)

        side_effects = [Path(config["raw_dir"]) / f"lp{i}.txt" for i in range(1, 4)]

        with patch("kgs_pipeline.acquire.download_lease_file", side_effect=side_effects):
            with patch(
                "kgs_pipeline.acquire.load_lease_index",
                return_value=[f"https://example.com?f_lc={i}" for i in range(1, 4)],
            ):
                result = run_acquire(config)

        assert len(result) == 3

    def test_filters_none_results(self, tmp_path: Path) -> None:
        index_path = self._make_index(tmp_path)
        config = self._config(tmp_path, index_path)
        urls = [f"https://example.com?f_lc={i}" for i in range(1, 4)]
        side_effects = [
            Path(config["raw_dir"]) / "lp1.txt",
            None,
            Path(config["raw_dir"]) / "lp3.txt",
        ]

        with patch("kgs_pipeline.acquire.download_lease_file", side_effect=side_effects):
            with patch("kgs_pipeline.acquire.load_lease_index", return_value=urls):
                result = run_acquire(config)

        assert len(result) == 2

    def test_creates_raw_dir(self, tmp_path: Path) -> None:
        index_path = self._make_index(tmp_path, n=1)
        config = self._config(tmp_path, index_path)
        raw_dir = Path(config["raw_dir"])
        assert not raw_dir.exists()

        with patch("kgs_pipeline.acquire.download_lease_file", return_value=None):
            with patch(
                "kgs_pipeline.acquire.load_lease_index", return_value=["https://example.com?f_lc=1"]
            ):
                run_acquire(config)

        assert raw_dir.exists()

    def test_uses_threaded_scheduler(self, tmp_path: Path) -> None:
        index_path = self._make_index(tmp_path, n=1)
        config = self._config(tmp_path, index_path)

        with patch(
            "kgs_pipeline.acquire.load_lease_index", return_value=["https://example.com?f_lc=1"]
        ):
            with patch("kgs_pipeline.acquire.dask.compute") as mock_compute:
                mock_compute.return_value = (None,)
                run_acquire(config)
                _, kwargs = mock_compute.call_args
                assert kwargs.get("scheduler") == "threads"


# ---------------------------------------------------------------------------
# Task A-07: Integration tests
# ---------------------------------------------------------------------------


class TestAcquireIntegration:
    MONTH_SAVE_BASE = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"

    def _setup(self, tmp_path: Path) -> tuple[dict, list[str]]:
        urls = [
            f"https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc={i}" for i in [564, 565, 566]
        ]
        rows = [{"MONTH-YEAR": "1-2024", "URL": u} for u in urls]
        df = pd.DataFrame(rows)
        idx_path = tmp_path / "index.txt"
        df.to_csv(idx_path, index=False)

        config = {
            "lease_index": str(idx_path),
            "raw_dir": str(tmp_path / "raw"),
            "month_save_url": self.MONTH_SAVE_BASE,
            "min_year": 2024,
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }
        return config, urls

    def _make_mock_get(self, lease_ids: list[str]) -> MagicMock:
        def side_effect(url: str, **kwargs) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "MonthSave" in url:
                for lid in lease_ids:
                    if f"f_lc={lid}" in url:
                        resp.text = (
                            f'<html><body><a href="https://chasm.kgs.ku.edu/ords/'
                            f'qualified.anon_blobber.download?p_file_name=lp{lid}.txt">'
                            f"Download</a></body></html>"
                        )
                        return resp
                resp.text = "<html></html>"
            else:
                resp.content = b"LEASE_KID,MONTH-YEAR\n1001,1-2024\nmore data\n"
            return resp

        return MagicMock(side_effect=side_effect)

    def test_idempotency_tr20(self, tmp_path: Path) -> None:
        config, _ = self._setup(tmp_path)
        mock_get = self._make_mock_get(["564", "565", "566"])
        with patch("kgs_pipeline.acquire.requests.get", mock_get):
            first = run_acquire(config)
        with patch("kgs_pipeline.acquire.requests.get", mock_get):
            second = run_acquire(config)

        raw_dir = Path(config["raw_dir"])
        files_first = set(f.name for f in raw_dir.glob("*.txt"))
        files_second = set(f.name for f in raw_dir.glob("*.txt"))
        assert files_first == files_second
        assert len(first) == len(second)

    def test_file_size_positive_tr21a(self, tmp_path: Path) -> None:
        config, _ = self._setup(tmp_path)
        mock_get = self._make_mock_get(["564", "565", "566"])
        with patch("kgs_pipeline.acquire.requests.get", mock_get):
            run_acquire(config)
        for f in Path(config["raw_dir"]).glob("*.txt"):
            assert f.stat().st_size > 0, f"{f.name} is zero bytes"

    def test_utf8_readability_tr21b(self, tmp_path: Path) -> None:
        config, _ = self._setup(tmp_path)
        mock_get = self._make_mock_get(["564", "565", "566"])
        with patch("kgs_pipeline.acquire.requests.get", mock_get):
            run_acquire(config)
        for f in Path(config["raw_dir"]).glob("*.txt"):
            f.read_text(encoding="utf-8")  # should not raise

    def test_minimum_row_count_tr21c(self, tmp_path: Path) -> None:
        config, _ = self._setup(tmp_path)
        mock_get = self._make_mock_get(["564", "565", "566"])
        with patch("kgs_pipeline.acquire.requests.get", mock_get):
            run_acquire(config)
        for f in Path(config["raw_dir"]).glob("*.txt"):
            lines = f.read_text(encoding="utf-8").splitlines()
            assert len(lines) > 1, f"{f.name} has only {len(lines)} line(s)"
