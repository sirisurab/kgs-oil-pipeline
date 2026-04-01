"""Tests for kgs_pipeline/acquire.py."""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.acquire import (
    ScrapingError,
    download_file,
    download_lease,
    extract_lease_id,
    load_lease_index,
    run_acquire,
    scrape_download_url,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

HEADER = '"LEASE_KID","LEASE","DOR_CODE","API_NUMBER","FIELD","PRODUCING_ZONE","OPERATOR","COUNTY","TOWNSHIP","TWN_DIR","RANGE","RANGE_DIR","SECTION","SPOT","LATITUDE","LONGITUDE","MONTH-YEAR","PRODUCT","WELLS","PRODUCTION","URL"'


def _make_row(lease_id: str, month_year: str, url: str) -> str:
    return f'"{lease_id}","TEST","123","API","FIELD","ZONE","OP","COUNTY","1","S","1","E","1","NE","39.0","-95.0","{month_year}","O","1","100.0","{url}"'


@pytest.fixture()
def index_fixture(tmp_path: Path) -> Path:
    """CSV with 2 rows in 2024 and 1 in 2023."""
    p = tmp_path / "leases.txt"
    rows = [
        HEADER,
        _make_row("A", "1-2024", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=A"),
        _make_row("B", "6-2024", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=B"),
        _make_row("C", "3-2023", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=C"),
    ]
    p.write_text("\n".join(rows))
    return p


@pytest.fixture()
def dup_url_fixture(tmp_path: Path) -> Path:
    """CSV where 2 rows in 2024 share the same URL."""
    p = tmp_path / "leases_dup.txt"
    rows = [
        HEADER,
        _make_row("A", "1-2024", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=A"),
        _make_row("A", "2-2024", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=A"),
    ]
    p.write_text("\n".join(rows))
    return p


@pytest.fixture()
def invalid_year_fixture(tmp_path: Path) -> Path:
    """CSV with a row that has MONTH-YEAR '-1-1965'."""
    p = tmp_path / "leases_invalid.txt"
    rows = [
        HEADER,
        _make_row("A", "1-2024", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=A"),
        _make_row("B", "-1-1965", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=B"),
    ]
    p.write_text("\n".join(rows))
    return p


@pytest.fixture()
def all_pre2024_fixture(tmp_path: Path) -> Path:
    p = tmp_path / "leases_old.txt"
    rows = [
        HEADER,
        _make_row("A", "1-2020", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=A"),
        _make_row("B", "6-2023", "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=B"),
    ]
    p.write_text("\n".join(rows))
    return p


# ---------------------------------------------------------------------------
# Task 01: load_lease_index
# ---------------------------------------------------------------------------


def test_load_lease_index_filters_pre2024(index_fixture: Path) -> None:
    df = load_lease_index(str(index_fixture))
    assert len(df) == 2


def test_load_lease_index_deduplicates_urls(dup_url_fixture: Path) -> None:
    df = load_lease_index(str(dup_url_fixture))
    assert df["URL"].duplicated().sum() == 0


def test_load_lease_index_drops_invalid_year(invalid_year_fixture: Path) -> None:
    """Row with MONTH-YEAR '-1-1965' has last element '1965' < 2024, so it's filtered out.
    Only the 2024 row remains."""
    df = load_lease_index(str(invalid_year_fixture))
    assert len(df) == 1


def test_load_lease_index_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_lease_index("/nonexistent/path/leases.txt")


def test_load_lease_index_all_pre2024_raises(all_pre2024_fixture: Path) -> None:
    with pytest.raises(ValueError, match="No leases found"):
        load_lease_index(str(all_pre2024_fixture))


# ---------------------------------------------------------------------------
# Task 02: extract_lease_id
# ---------------------------------------------------------------------------


def test_extract_lease_id_valid() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
    assert extract_lease_id(url) == "1001135839"


def test_extract_lease_id_no_f_lc() -> None:
    with pytest.raises(ValueError):
        extract_lease_id("https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?other=123")


def test_extract_lease_id_empty_f_lc() -> None:
    with pytest.raises(ValueError):
        extract_lease_id("https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=")


def test_extract_lease_id_extra_params() -> None:
    url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=12345&other=abc"
    assert extract_lease_id(url) == "12345"


# ---------------------------------------------------------------------------
# Task 03: scrape_download_url
# ---------------------------------------------------------------------------


def _mock_response(html: str) -> MagicMock:
    resp = MagicMock()
    resp.text = html
    resp.raise_for_status = MagicMock()
    return resp


def test_scrape_download_url_success() -> None:
    html = textwrap.dedent("""
        <html><body>
        <a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt">Download</a>
        </body></html>
    """)
    session = MagicMock()
    session.get.return_value = _mock_response(html)
    result = scrape_download_url("1001135839", session)
    assert (
        result
        == "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    )


def test_scrape_download_url_no_link() -> None:
    html = "<html><body><p>No download link here</p></body></html>"
    session = MagicMock()
    session.get.return_value = _mock_response(html)
    with pytest.raises(ScrapingError):
        scrape_download_url("1001135839", session)


def test_scrape_download_url_empty_body() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response("")
    with pytest.raises(ScrapingError):
        scrape_download_url("1001135839", session)


# ---------------------------------------------------------------------------
# Task 04: download_file
# ---------------------------------------------------------------------------


def test_download_file_success(tmp_path: Path) -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.content = b"some data"
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    with patch("time.sleep"):
        result = download_file(url, str(tmp_path), session)

    assert result.name == "lp564.txt"
    assert result.exists()


def test_download_file_idempotent(tmp_path: Path) -> None:
    """Second call should not invoke session.get."""
    session = MagicMock()
    existing = tmp_path / "lp564.txt"
    existing.write_bytes(b"already here")

    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
    result = download_file(url, str(tmp_path), session)
    assert result == existing
    session.get.assert_not_called()


def test_download_file_empty_response(tmp_path: Path) -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.content = b""
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp999.txt"
    with pytest.raises(ValueError, match="empty"):
        download_file(url, str(tmp_path), session)


def test_download_file_missing_param(tmp_path: Path) -> None:
    session = MagicMock()
    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?other=foo"
    with pytest.raises(ValueError, match="p_file_name"):
        download_file(url, str(tmp_path), session)


def test_download_file_creates_output_dir(tmp_path: Path) -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.content = b"data"
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    new_dir = tmp_path / "new_subdir"
    url = "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp1.txt"
    with patch("time.sleep"):
        download_file(url, str(new_dir), session)
    assert new_dir.exists()


# ---------------------------------------------------------------------------
# Task 05: download_lease
# ---------------------------------------------------------------------------


def test_download_lease_success(tmp_path: Path) -> None:
    session = MagicMock()
    with (
        patch("kgs_pipeline.acquire.extract_lease_id", return_value="123"),
        patch(
            "kgs_pipeline.acquire.scrape_download_url",
            return_value="https://example.com?p_file_name=lp1.txt",
        ),
        patch("kgs_pipeline.acquire.download_file", return_value=tmp_path / "lp1.txt"),
    ):
        result = download_lease("https://example.com?f_lc=123", str(tmp_path), session)
    assert result == tmp_path / "lp1.txt"


def test_download_lease_scraping_error_returns_none(tmp_path: Path) -> None:
    session = MagicMock()
    with (
        patch("kgs_pipeline.acquire.extract_lease_id", return_value="123"),
        patch("kgs_pipeline.acquire.scrape_download_url", side_effect=ScrapingError("no link")),
    ):
        result = download_lease("https://example.com?f_lc=123", str(tmp_path), session)
    assert result is None


def test_download_lease_value_error_returns_none(tmp_path: Path) -> None:
    session = MagicMock()
    with patch("kgs_pipeline.acquire.extract_lease_id", side_effect=ValueError("bad url")):
        result = download_lease("https://example.com", str(tmp_path), session)
    assert result is None


# ---------------------------------------------------------------------------
# Task 06: run_acquire
# ---------------------------------------------------------------------------


def _make_df_with_urls(urls: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"URL": urls})


def test_run_acquire_all_success(tmp_path: Path, index_fixture: Path) -> None:
    p = tmp_path / "f.txt"
    compute_result = tuple([p, p, p])
    with (
        patch(
            "kgs_pipeline.acquire.load_lease_index",
            return_value=_make_df_with_urls(["u1", "u2", "u3"]),
        ),
        patch("kgs_pipeline.acquire.download_lease", side_effect=[p, p, p]),
        patch("dask.compute", return_value=compute_result),
    ):
        results = run_acquire(str(index_fixture), str(tmp_path))
    assert len(results) == 3


def test_run_acquire_all_fail(tmp_path: Path, index_fixture: Path) -> None:
    compute_result = (None, None)
    with (
        patch(
            "kgs_pipeline.acquire.load_lease_index", return_value=_make_df_with_urls(["u1", "u2"])
        ),
        patch("kgs_pipeline.acquire.download_lease", return_value=None),
        patch("dask.compute", return_value=compute_result),
    ):
        results = run_acquire(str(index_fixture), str(tmp_path))
    assert results == []


def test_run_acquire_partial_success(tmp_path: Path, index_fixture: Path) -> None:
    p1 = tmp_path / "f1.txt"
    p2 = tmp_path / "f2.txt"
    compute_result = (p1, None, p2)
    with (
        patch(
            "kgs_pipeline.acquire.load_lease_index",
            return_value=_make_df_with_urls(["u1", "u2", "u3"]),
        ),
        patch("kgs_pipeline.acquire.download_lease", side_effect=[p1, None, p2]),
        patch("dask.compute", return_value=compute_result),
    ):
        results = run_acquire(str(index_fixture), str(tmp_path))
    assert len(results) == 2


def test_run_acquire_respects_max_workers(tmp_path: Path, index_fixture: Path) -> None:
    p = tmp_path / "f.txt"
    with (
        patch("kgs_pipeline.acquire.load_lease_index", return_value=_make_df_with_urls(["u1"])),
        patch("kgs_pipeline.acquire.download_lease", return_value=p),
        patch("dask.compute", return_value=(p,)) as mock_compute,
    ):
        run_acquire(str(index_fixture), str(tmp_path), max_workers=3)
    call_kwargs = mock_compute.call_args
    assert call_kwargs.kwargs.get("num_workers") == 3
