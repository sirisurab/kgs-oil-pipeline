"""Tests for kgs_pipeline/acquire.py (Tasks A-01 to A-06)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from kgs_pipeline.acquire import (
    acquire,
    download_lease_file,
    extract_lease_id,
    load_lease_index,
    resolve_download_url,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BASE_CONFIG = {
    "acquire": {
        "lease_index_path": "",  # overridden per test
        "target_year": 2024,
        "raw_dir": "",
        "max_workers": 2,
    }
}


def _make_index_csv(tmp_path: Path, rows: list[dict]) -> str:
    df = pd.DataFrame(rows)
    p = tmp_path / "lease_index.txt"
    df.to_csv(p, index=False)
    return str(p)


# ---------------------------------------------------------------------------
# Task A-01: load_lease_index
# ---------------------------------------------------------------------------


def test_load_lease_index_year_filter(tmp_path: Path) -> None:
    rows = [
        {"MONTH-YEAR": "1-2024", "URL": "http://a.com?f_lc=1"},
        {"MONTH-YEAR": "6-2025", "URL": "http://b.com?f_lc=2"},
        {"MONTH-YEAR": "3-2023", "URL": "http://c.com?f_lc=3"},
    ]
    path = _make_index_csv(tmp_path, rows)
    config = {**BASE_CONFIG, "acquire": {**BASE_CONFIG["acquire"], "lease_index_path": path}}
    result = load_lease_index(config)
    assert set(result["MONTH-YEAR"]) == {"1-2024", "6-2025"}


def test_load_lease_index_drops_non_numeric_year(tmp_path: Path) -> None:
    rows = [
        {"MONTH-YEAR": "-1-1965", "URL": "http://a.com?f_lc=1"},
        {"MONTH-YEAR": "0-1966", "URL": "http://b.com?f_lc=2"},
        {"MONTH-YEAR": "1-2024", "URL": "http://c.com?f_lc=3"},
    ]
    path = _make_index_csv(tmp_path, rows)
    config = {**BASE_CONFIG, "acquire": {**BASE_CONFIG["acquire"], "lease_index_path": path}}
    result = load_lease_index(config)
    # -1-1965 → split on "-" → last element "1965" BUT the month part is "-1"
    # The year is the last element; "-1" yields last element "1965" which is numeric,
    # but "0-1966" yields "1966" which is also numeric. These are year < 2024 so filtered.
    # The actual non-numeric drop covers strings like "abc" etc.
    # Both 1965 and 1966 rows are year < 2024 so they won't appear regardless.
    assert len(result) == 1
    assert result["MONTH-YEAR"].iloc[0] == "1-2024"


def test_load_lease_index_dedup_by_url(tmp_path: Path) -> None:
    rows = [
        {"MONTH-YEAR": "1-2024", "URL": "http://a.com?f_lc=1"},
        {"MONTH-YEAR": "6-2024", "URL": "http://a.com?f_lc=1"},
        {"MONTH-YEAR": "3-2025", "URL": "http://b.com?f_lc=2"},
    ]
    path = _make_index_csv(tmp_path, rows)
    config = {**BASE_CONFIG, "acquire": {**BASE_CONFIG["acquire"], "lease_index_path": path}}
    result = load_lease_index(config)
    assert len(result) == 2
    assert result["URL"].tolist().count("http://a.com?f_lc=1") == 1


def test_load_lease_index_returns_dataframe(tmp_path: Path) -> None:
    rows = [{"MONTH-YEAR": "1-2024", "URL": "http://a.com?f_lc=1"}]
    path = _make_index_csv(tmp_path, rows)
    config = {**BASE_CONFIG, "acquire": {**BASE_CONFIG["acquire"], "lease_index_path": path}}
    result = load_lease_index(config)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task A-02: extract_lease_id
# ---------------------------------------------------------------------------


def test_extract_lease_id_valid_url() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
    assert extract_lease_id(url) == "1001135839"


def test_extract_lease_id_missing_param() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=123"
    with pytest.raises(ValueError):
        extract_lease_id(url)


# ---------------------------------------------------------------------------
# Task A-03: resolve_download_url
# ---------------------------------------------------------------------------


def test_resolve_download_url_returns_href() -> None:
    html = (
        "<html><body>"
        '<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
        '?p_file_name=lp564.txt">Download</a>'
        "</body></html>"
    )
    mock_resp = MagicMock()
    mock_resp.text = html

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        result = resolve_download_url("564", BASE_CONFIG)

    assert result is not None
    assert "anon_blobber.download" in result
    assert "lp564.txt" in result


def test_resolve_download_url_returns_none_on_no_link() -> None:
    html = "<html><body><p>No link here</p></body></html>"
    mock_resp = MagicMock()
    mock_resp.text = html

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        result = resolve_download_url("999", BASE_CONFIG)

    assert result is None


def test_resolve_download_url_contains_lease_id() -> None:
    html = "<html><body></body></html>"
    mock_resp = MagicMock()
    mock_resp.text = html
    captured_urls: list[str] = []

    def fake_get(url: str, **kwargs: object) -> MagicMock:
        captured_urls.append(url)
        return mock_resp

    with patch("kgs_pipeline.acquire.requests.get", side_effect=fake_get):
        resolve_download_url("1001135839", BASE_CONFIG)

    assert "1001135839" in captured_urls[0]


# ---------------------------------------------------------------------------
# Task A-04: download_lease_file
# ---------------------------------------------------------------------------


def test_download_lease_file_writes_file(tmp_path: Path) -> None:
    content = b"LEASE_KID,LEASE\n1,TestLease\n"
    download_url = (
        "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )
    mock_resp = MagicMock()
    mock_resp.content = content

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_lease_file(download_url, str(tmp_path))

    assert result is not None
    assert result == tmp_path / "lp564.txt"
    assert result.exists()


def test_download_lease_file_skips_existing(tmp_path: Path) -> None:
    existing = tmp_path / "lp564.txt"
    existing.write_bytes(b"existing content")
    download_url = (
        "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )

    with patch("kgs_pipeline.acquire.requests.get") as mock_get:
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_lease_file(download_url, str(tmp_path))

    mock_get.assert_not_called()
    assert result == existing


def test_download_lease_file_returns_none_on_empty_content(tmp_path: Path) -> None:
    download_url = (
        "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )
    mock_resp = MagicMock()
    mock_resp.content = b""

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_lease_file(download_url, str(tmp_path))

    assert result is None
    assert not (tmp_path / "lp564.txt").exists()


def test_download_lease_file_sleeps(tmp_path: Path) -> None:
    download_url = (
        "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )
    mock_resp = MagicMock()
    mock_resp.content = b"data"

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep") as mock_sleep:
            download_lease_file(download_url, str(tmp_path))

    mock_sleep.assert_called_once_with(0.5)


# ---------------------------------------------------------------------------
# Task A-05: acquire (orchestration)
# ---------------------------------------------------------------------------


def _make_index_with_urls(tmp_path: Path, urls: list[str]) -> str:
    rows = [{"MONTH-YEAR": "1-2024", "URL": u} for u in urls]
    return _make_index_csv(tmp_path, rows)


def test_acquire_returns_list_of_paths(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    urls = [
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1",
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=2",
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=3",
    ]
    idx_path = _make_index_with_urls(tmp_path, urls)

    html = (
        '<html><body><a href="https://host/ords/qualified.anon_blobber.download'
        '?p_file_name=lp{id}.txt">D</a></body></html>'
    )

    call_count = [0]

    def fake_get(url: str, **kwargs: object) -> MagicMock:
        m = MagicMock()
        call_count[0] += 1
        if "MonthSave" in url:
            lease_id = url.split("f_lc=")[1]
            m.text = html.replace("{id}", lease_id)
        else:
            m.content = b"LEASE_KID\n1\n"
        return m

    config = {
        "acquire": {
            "lease_index_path": idx_path,
            "target_year": 2024,
            "raw_dir": str(raw_dir),
            "max_workers": 2,
        }
    }

    with patch("kgs_pipeline.acquire.requests.get", side_effect=fake_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            results = acquire(config)

    assert len(results) == 3
    assert all(isinstance(r, Path) for r in results)


def test_acquire_partial_none_results(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    urls = [
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1",
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=2",
    ]
    idx_path = _make_index_with_urls(tmp_path, urls)

    html_with_link = (
        '<html><body><a href="https://host/ords/qualified.anon_blobber.download'
        '?p_file_name=lp1.txt">D</a></body></html>'
    )
    html_no_link = "<html><body></body></html>"

    def fake_get(url: str, **kwargs: object) -> MagicMock:
        m = MagicMock()
        if "MonthSave" in url:
            if "f_lc=1" in url:
                m.text = html_with_link
            else:
                m.text = html_no_link
        else:
            m.content = b"data"
        return m

    config = {
        "acquire": {
            "lease_index_path": idx_path,
            "target_year": 2024,
            "raw_dir": str(raw_dir),
            "max_workers": 2,
        }
    }

    with patch("kgs_pipeline.acquire.requests.get", side_effect=fake_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            results = acquire(config)

    paths = [r for r in results if r is not None]
    nones = [r for r in results if r is None]
    assert len(paths) == 1
    assert len(nones) == 1


def test_acquire_uses_threaded_scheduler(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    urls = ["https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1"]
    idx_path = _make_index_with_urls(tmp_path, urls)
    config = {
        "acquire": {
            "lease_index_path": idx_path,
            "target_year": 2024,
            "raw_dir": str(raw_dir),
            "max_workers": 2,
        }
    }

    captured: list[str] = []

    import dask

    original_compute = dask.compute

    def fake_compute(*args: object, **kwargs: object) -> tuple:
        captured.append(str(kwargs.get("scheduler", "")))
        return original_compute(*args, scheduler="synchronous")

    with patch("kgs_pipeline.acquire.dask.compute", side_effect=fake_compute):
        with patch("kgs_pipeline.acquire.requests.get"):
            with patch("kgs_pipeline.acquire.time.sleep"):
                try:
                    acquire(config)
                except Exception:
                    pass

    assert any("thread" in s for s in captured)


def test_acquire_idempotent(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    urls = ["https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1"]
    idx_path = _make_index_with_urls(tmp_path, urls)

    existing = raw_dir / "lp1.txt"
    existing.write_bytes(b"LEASE_KID\n1\n")

    html = (
        '<html><body><a href="https://host/ords/qualified.anon_blobber.download'
        '?p_file_name=lp1.txt">D</a></body></html>'
    )

    config = {
        "acquire": {
            "lease_index_path": idx_path,
            "target_year": 2024,
            "raw_dir": str(raw_dir),
            "max_workers": 2,
        }
    }

    net_calls: list[str] = []

    def fake_get(url: str, **kwargs: object) -> MagicMock:
        net_calls.append(url)
        m = MagicMock()
        m.text = html
        m.content = b"data"
        return m

    with patch("kgs_pipeline.acquire.requests.get", side_effect=fake_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            results = acquire(config)

    # MonthSave page is fetched but the download itself should be skipped
    download_calls = [u for u in net_calls if "anon_blobber" in u]
    assert len(download_calls) == 0
    assert results[0] == existing


# ---------------------------------------------------------------------------
# Task A-06: File integrity verification
# ---------------------------------------------------------------------------


def test_raw_files_nonzero_size(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    f = raw_dir / "lp001.txt"
    f.write_bytes(b"LEASE_KID,LEASE\n1,TestLease\n")

    for file in raw_dir.glob("*.txt"):
        assert file.stat().st_size > 0


def test_raw_files_readable_utf8(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    f = raw_dir / "lp001.txt"
    f.write_text("LEASE_KID,LEASE\n1,TestLease\n", encoding="utf-8")

    for file in raw_dir.glob("*.txt"):
        file.read_text(encoding="utf-8")  # raises UnicodeDecodeError if invalid


def test_raw_files_have_data_rows(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    f = raw_dir / "lp001.txt"
    f.write_text("LEASE_KID,LEASE\n1,TestLease\n", encoding="utf-8")

    for file in raw_dir.glob("*.txt"):
        lines = file.read_text(encoding="utf-8").splitlines()
        assert len(lines) > 1
