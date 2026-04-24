"""Tests for the acquire stage (tasks A1–A4).

Test requirements: TR-20, TR-21, TR-27 (from test-requirements.xml).
Each test carries exactly one of: @pytest.mark.unit or @pytest.mark.integration
per ADR-008.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kgs_pipeline.acquire import (
    build_manifest,
    download_file,
    resolve_download_url,
    acquire,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def lease_index_csv(tmp_path: Path) -> Path:
    """Synthetic lease index with leases spanning 2020, 2023, 2024, 2025."""
    content = textwrap.dedent("""\
        LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL
        1001,ALPHA,111,,,,Op1,Nemaha,1,S,14,E,1,,39.9,-95.8,1-2020,O,2,100,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001
        1001,ALPHA,111,,,,Op1,Nemaha,1,S,14,E,1,,39.9,-95.8,3-2020,O,2,110,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001
        1002,BETA,222,,,,Op2,Douglas,2,S,15,W,2,,38.9,-95.2,5-2023,O,1,50,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1002
        1003,GAMMA,333,,,,Op3,Riley,3,S,16,W,3,,39.1,-96.1,1-2024,O,3,200,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1003
        1003,GAMMA,333,,,,Op3,Riley,3,S,16,W,3,,39.1,-96.1,6-2024,O,3,210,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1003
        1004,DELTA,444,,,,Op4,Ellis,4,S,17,E,4,,38.5,-99.1,3-2025,O,1,80,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1004
    """)
    p = tmp_path / "lease_index.txt"
    p.write_text(content)
    return p


@pytest.fixture()
def lease_index_with_bad_years(tmp_path: Path) -> Path:
    """Lease index including rows with non-numeric year components."""
    content = textwrap.dedent("""\
        LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL
        1005,EPSILON,555,,,,Op5,Barton,1,S,10,W,5,,38.0,-98.5,-1-1965,O,2,300,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1005
        1005,EPSILON,555,,,,Op5,Barton,1,S,10,W,5,,38.0,-98.5,0-1966,O,2,310,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1005
        1005,EPSILON,555,,,,Op5,Barton,1,S,10,W,5,,38.0,-98.5,1-2024,O,2,320,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1005
    """)
    p = tmp_path / "lease_index_bad.txt"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Task A1 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_manifest_filters_by_min_year(lease_index_csv: Path) -> None:
    """Only leases with at least one row at year >= 2024 appear in manifest."""
    df = build_manifest(lease_index_csv, min_year=2024)
    assert set(df["LEASE_KID"].astype(str)) == {"1003", "1004"}


@pytest.mark.unit
def test_manifest_deduplicates_by_url(lease_index_csv: Path) -> None:
    """One entry per URL — index has many rows per lease."""
    df = build_manifest(lease_index_csv, min_year=2024)
    assert df["URL"].nunique() == len(df)
    assert len(df) == 2  # leases 1003 and 1004


@pytest.mark.unit
def test_manifest_excludes_non_numeric_years(lease_index_with_bad_years: Path) -> None:
    """Rows with non-numeric year components (e.g. -1-1965, 0-1966) are excluded."""
    df = build_manifest(lease_index_with_bad_years, min_year=2024)
    # Lease 1005 has a valid 2024 row, so it qualifies
    assert "1005" in df["LEASE_KID"].astype(str).values
    assert len(df) == 1


@pytest.mark.unit
def test_manifest_missing_file_raises(tmp_path: Path) -> None:
    """Missing index file raises a clear error naming the expected path."""
    missing = tmp_path / "does_not_exist.txt"
    with pytest.raises(FileNotFoundError, match=str(missing)):
        build_manifest(missing, min_year=2024)


# ---------------------------------------------------------------------------
# Task A2 tests
# ---------------------------------------------------------------------------

_MONTHSAVE_TEMPLATE = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"

_MONTHSAVE_HTML_WITH_ANCHOR = """\
<html><body>
<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt">
Download
</a>
</body></html>
"""

_MONTHSAVE_HTML_NO_ANCHOR = "<html><body><p>No download available.</p></body></html>"


@pytest.mark.unit
def test_resolve_download_url_returns_url() -> None:
    """Given an HTML page with an anon_blobber.download anchor, returns the URL."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.text = _MONTHSAVE_HTML_WITH_ANCHOR

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        result = resolve_download_url(
            lease_id="1001135839",
            monthsave_template=_MONTHSAVE_TEMPLATE,
            retry_attempts=3,
            retry_backoff_base=2.0,
            timeout=30,
        )

    assert result is not None
    assert "anon_blobber.download" in result
    assert "p_file_name=lp564.txt" in result


@pytest.mark.unit
def test_resolve_download_url_no_anchor_returns_none() -> None:
    """No matching anchor → returns None without raising."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.text = _MONTHSAVE_HTML_NO_ANCHOR

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        result = resolve_download_url(
            lease_id="9999",
            monthsave_template=_MONTHSAVE_TEMPLATE,
            retry_attempts=3,
            retry_backoff_base=2.0,
            timeout=30,
        )

    assert result is None


@pytest.mark.unit
def test_resolve_download_url_http_error_returns_none() -> None:
    """Persistent HTTP errors → returns None and logs a warning (no exception raised)."""
    import requests as req

    with patch("kgs_pipeline.acquire.requests.get", side_effect=req.ConnectionError("fail")):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = resolve_download_url(
                lease_id="8888",
                monthsave_template=_MONTHSAVE_TEMPLATE,
                retry_attempts=2,
                retry_backoff_base=1.0,
                timeout=5,
            )

    assert result is None


# ---------------------------------------------------------------------------
# Task A3 tests
# ---------------------------------------------------------------------------

_DOWNLOAD_URL = (
    "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
)


@pytest.mark.unit
def test_download_file_writes_to_target_dir(tmp_path: Path) -> None:
    """Valid text content → file written to target_dir with p_file_name as filename."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.content = b"LEASE_KID,MONTH-YEAR\n1001,1-2024\n"

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_file(
                download_url=_DOWNLOAD_URL,
                target_dir=tmp_path,
                sleep_seconds=0.0,
                retry_attempts=3,
                retry_backoff_base=2.0,
                timeout=30,
            )

    assert result is not None
    assert result.name == "lp564.txt"
    assert result.exists()
    assert result.stat().st_size > 0


@pytest.mark.unit
def test_download_file_idempotent(tmp_path: Path) -> None:
    """Existing non-empty file → not re-downloaded, existing content unchanged (TR-20)."""
    target = tmp_path / "lp564.txt"
    original_content = b"LEASE_KID,MONTH-YEAR\n1001,1-2024\n"
    target.write_bytes(original_content)

    mock_get = MagicMock()
    with patch("kgs_pipeline.acquire.requests.get", mock_get):
        result = download_file(
            download_url=_DOWNLOAD_URL,
            target_dir=tmp_path,
            sleep_seconds=0.0,
            retry_attempts=3,
            retry_backoff_base=2.0,
            timeout=30,
        )

    mock_get.assert_not_called()
    assert result == target
    assert target.read_bytes() == original_content


@pytest.mark.unit
def test_download_file_empty_content_no_file_left(tmp_path: Path) -> None:
    """Empty response → no file left behind, sentinel returned."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.content = b""

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_file(
                download_url=_DOWNLOAD_URL,
                target_dir=tmp_path,
                sleep_seconds=0.0,
                retry_attempts=1,
                retry_backoff_base=2.0,
                timeout=30,
            )

    assert result is None
    assert not (tmp_path / "lp564.txt").exists()


@pytest.mark.unit
def test_download_file_non_utf8_no_file_left(tmp_path: Path) -> None:
    """Non-UTF-8 bytes that fail text validation → no file left behind (H2)."""
    # Create a mock bytes object that raises UnicodeDecodeError on decode attempts
    mock_content = MagicMock()
    mock_content.decode.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "simulated")

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.content = mock_content

    with patch("kgs_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("kgs_pipeline.acquire.time.sleep"):
            result = download_file(
                download_url=_DOWNLOAD_URL,
                target_dir=tmp_path,
                sleep_seconds=0.0,
                retry_attempts=1,
                retry_backoff_base=2.0,
                timeout=30,
            )

    assert result is None
    assert not (tmp_path / "lp564.txt").exists()


# ---------------------------------------------------------------------------
# Task A4 tests
# ---------------------------------------------------------------------------


def _make_acquire_config(tmp_path: Path, index_path: str) -> dict:
    return {
        "acquire": {
            "lease_index_path": index_path,
            "raw_dir": str(tmp_path / "raw"),
            "min_year": 2024,
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "retry_attempts": 1,
            "retry_backoff_base": 1.0,
            "monthsave_url_template": "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}",
            "download_timeout": 5,
        }
    }


@pytest.fixture()
def minimal_index(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL
        1003,GAMMA,333,,,,Op3,Riley,3,S,16,W,3,,39.1,-96.1,1-2024,O,3,200,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1003
        1004,DELTA,444,,,,Op4,Ellis,4,S,17,E,4,,38.5,-99.1,3-2025,O,1,80,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1004
        1005,EPSILON,555,,,,Op5,Barton,1,S,10,W,5,,38.0,-98.5,2-2024,O,2,300,https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1005
    """)
    p = tmp_path / "lease_index.txt"
    p.write_text(content)
    return p


@pytest.mark.unit
def test_acquire_all_succeed(tmp_path: Path, minimal_index: Path) -> None:
    """All 3 leases succeed → 3 files in target directory."""
    cfg = _make_acquire_config(tmp_path, str(minimal_index))

    html_template = (
        '<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
        '?p_file_name=lp{lease_id}.txt">Download</a>'
    )

    call_counter = {"n": 0}

    def mock_get(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "MonthSave" in url:
            lease_id = url.split("f_lc=")[-1]
            resp.text = html_template.format(lease_id=lease_id)
        else:
            resp.content = b"LEASE_KID,MONTH-YEAR\n1003,1-2024\n"
        call_counter["n"] += 1
        return resp

    with patch("kgs_pipeline.acquire.requests.get", side_effect=mock_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            acquire(cfg)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    files = list(raw_dir.glob("*.txt"))
    assert len(files) == 3


@pytest.mark.unit
def test_acquire_partial_failure_returns_cleanly(tmp_path: Path, minimal_index: Path) -> None:
    """1 resolver failure + 1 download failure → 1 file written; function returns cleanly."""
    cfg = _make_acquire_config(tmp_path, str(minimal_index))

    lease_ids = ["1003", "1004", "1005"]
    fail_resolve = "1004"
    fail_download = "1005"

    def mock_get(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "MonthSave" in url:
            lease_id = url.split("f_lc=")[-1]
            if lease_id == fail_resolve:
                resp.text = "<html><body>No download.</body></html>"
            else:
                resp.text = (
                    f'<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
                    f'?p_file_name=lp{lease_id}.txt">Download</a>'
                )
        else:
            # download request
            for lid in lease_ids:
                if f"lp{lid}.txt" in url:
                    if lid == fail_download:
                        resp.content = b""
                    else:
                        resp.content = b"LEASE_KID,MONTH-YEAR\n1003,1-2024\n"
                    return resp
            resp.content = b"LEASE_KID,MONTH-YEAR\n1003,1-2024\n"
        return resp

    with patch("kgs_pipeline.acquire.requests.get", side_effect=mock_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            acquire(cfg)  # must not raise

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    files = list(raw_dir.glob("*.txt"))
    assert len(files) == 1


@pytest.mark.integration
def test_acquire_idempotent(tmp_path: Path, minimal_index: Path) -> None:
    """Run acquire twice; file count identical and contents unchanged (TR-20)."""
    cfg = _make_acquire_config(tmp_path, str(minimal_index))

    file_content = b"LEASE_KID,MONTH-YEAR\n1003,1-2024\n"
    lease_ids_seen: set[str] = set()

    def mock_get(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "MonthSave" in url:
            lease_id = url.split("f_lc=")[-1]
            lease_ids_seen.add(lease_id)
            resp.text = (
                f'<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
                f'?p_file_name=lp{lease_id}.txt">Download</a>'
            )
        else:
            resp.content = file_content
        return resp

    with patch("kgs_pipeline.acquire.requests.get", side_effect=mock_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            acquire(cfg)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    first_run_files = {f.name: f.read_bytes() for f in raw_dir.glob("*.txt")}
    first_run_count = len(first_run_files)

    with patch("kgs_pipeline.acquire.requests.get", side_effect=mock_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            acquire(cfg)

    second_run_files = {f.name: f.read_bytes() for f in raw_dir.glob("*.txt")}
    assert len(second_run_files) == first_run_count
    for name, content in first_run_files.items():
        assert second_run_files[name] == content


@pytest.mark.integration
def test_acquire_output_files_valid(tmp_path: Path, minimal_index: Path) -> None:
    """Every file in raw dir: size > 0, text-decodable, ≥2 lines (TR-21)."""
    cfg = _make_acquire_config(tmp_path, str(minimal_index))
    file_content = b"LEASE_KID,MONTH-YEAR\n1003,1-2024\n1003,2-2024\n"

    def mock_get(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "MonthSave" in url:
            lease_id = url.split("f_lc=")[-1]
            resp.text = (
                f'<a href="https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download'
                f'?p_file_name=lp{lease_id}.txt">Download</a>'
            )
        else:
            resp.content = file_content
        return resp

    with patch("kgs_pipeline.acquire.requests.get", side_effect=mock_get):
        with patch("kgs_pipeline.acquire.time.sleep"):
            acquire(cfg)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    files = list(raw_dir.glob("*.txt"))
    assert len(files) > 0
    for f in files:
        assert f.stat().st_size > 0
        text = f.read_text(encoding="utf-8")
        assert len(text.splitlines()) >= 2
