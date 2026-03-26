"""Tests for kgs_pipeline/acquire.py (Tasks 01–05) and config/utils."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from kgs_pipeline.config import CONFIG, PipelineConfig
from kgs_pipeline.utils import (
    compute_file_hash,
    ensure_dir,
    is_valid_raw_file,
    retry,
    timer,
)


# ---------------------------------------------------------------------------
# Task 01: PipelineConfig tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_config_defaults():
    """PipelineConfig() with no args has correct defaults."""
    cfg = PipelineConfig()
    assert cfg.raw_dir == Path("data/raw")
    assert cfg.interim_dir == Path("data/interim")
    assert cfg.processed_dir == Path("data/processed")
    assert cfg.features_dir == Path("data/processed/features")
    assert cfg.year_start == 2020
    assert cfg.year_end == 2025
    assert cfg.max_concurrent_requests == 5
    assert cfg.oil_max_bbl_per_month == 50000.0
    assert cfg.dask_n_workers == 4


@pytest.mark.unit
def test_config_singleton_is_pipeline_config():
    """The module-level CONFIG singleton is a PipelineConfig instance."""
    assert isinstance(CONFIG, PipelineConfig)


@pytest.mark.unit
def test_config_year_start_too_low():
    with pytest.raises(ValidationError):
        PipelineConfig(year_start=1990)


@pytest.mark.unit
def test_config_year_start_greater_than_end():
    with pytest.raises(ValidationError):
        PipelineConfig(year_start=2025, year_end=2020)


@pytest.mark.unit
def test_config_max_concurrent_requests_zero():
    with pytest.raises(ValidationError):
        PipelineConfig(max_concurrent_requests=0)


@pytest.mark.unit
def test_config_path_fields_are_paths():
    cfg = PipelineConfig()
    assert isinstance(cfg.raw_dir, Path)
    assert isinstance(cfg.interim_dir, Path)
    assert isinstance(cfg.processed_dir, Path)
    assert isinstance(cfg.features_dir, Path)


# ---------------------------------------------------------------------------
# Task 02: Utils tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ensure_dir_creates_directory(tmp_path: Path):
    new_dir = tmp_path / "subdir" / "nested"
    result = ensure_dir(new_dir)
    assert new_dir.exists()
    assert result == new_dir
    # Idempotent
    ensure_dir(new_dir)


@pytest.mark.unit
def test_compute_file_hash_hex_string(tmp_path: Path):
    f = tmp_path / "test.txt"
    f.write_text("hello world", encoding="utf-8")
    digest = compute_file_hash(f)
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


@pytest.mark.unit
def test_compute_file_hash_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        compute_file_hash(tmp_path / "nonexistent.txt")


@pytest.mark.unit
def test_retry_exhausts_all_attempts():
    call_count = 0

    @retry(max_attempts=3, backoff_s=0.001, exceptions=(ValueError,))
    def always_fail():
        nonlocal call_count
        call_count += 1
        raise ValueError("always fails")

    with pytest.raises(ValueError):
        always_fail()
    assert call_count == 3


@pytest.mark.unit
def test_retry_succeeds_on_second_attempt():
    call_count = 0

    @retry(max_attempts=3, backoff_s=0.001, exceptions=(ValueError,))
    def fail_once():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("first attempt fails")
        return "ok"

    result = fail_once()
    assert result == "ok"
    assert call_count == 2


@pytest.mark.unit
def test_is_valid_raw_file_zero_bytes(tmp_path: Path):
    f = tmp_path / "empty.txt"
    f.write_bytes(b"")
    assert is_valid_raw_file(f) is False


@pytest.mark.unit
def test_is_valid_raw_file_only_header(tmp_path: Path):
    f = tmp_path / "header_only.txt"
    f.write_text("HEADER_LINE\n", encoding="utf-8")
    assert is_valid_raw_file(f) is False


@pytest.mark.unit
def test_is_valid_raw_file_valid(tmp_path: Path):
    f = tmp_path / "valid.txt"
    f.write_text("header\ndata row\n", encoding="utf-8")
    assert is_valid_raw_file(f) is True


@pytest.mark.unit
def test_is_valid_raw_file_invalid_utf8(tmp_path: Path):
    f = tmp_path / "bad_encoding.txt"
    f.write_bytes(b"\xff\xfe invalid bytes \x80\x81")
    assert is_valid_raw_file(f) is False


@pytest.mark.unit
def test_timer_decorator_returns_value():
    @timer()
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5


# ---------------------------------------------------------------------------
# Task 03: load_lease_urls tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_lease_urls_basic(tmp_path: Path):
    from kgs_pipeline.acquire import load_lease_urls

    f = tmp_path / "leases.txt"
    f.write_text("URL\nhttps://example.com/1\nhttps://example.com/2\n", encoding="utf-8")
    urls = load_lease_urls(f)
    assert len(urls) == 2
    assert all(isinstance(u, str) and u for u in urls)


@pytest.mark.unit
def test_load_lease_urls_deduplication(tmp_path: Path):
    from kgs_pipeline.acquire import load_lease_urls

    f = tmp_path / "leases.txt"
    f.write_text(
        "URL\nhttps://example.com/1\nhttps://example.com/1\nhttps://example.com/2\n",
        encoding="utf-8",
    )
    urls = load_lease_urls(f)
    assert len(urls) == 2


@pytest.mark.unit
def test_load_lease_urls_nulls_excluded(tmp_path: Path):
    from kgs_pipeline.acquire import load_lease_urls

    f = tmp_path / "leases.txt"
    f.write_text("URL\nhttps://example.com/1\n\n", encoding="utf-8")
    urls = load_lease_urls(f)
    assert all(u for u in urls)
    assert len(urls) == 1


@pytest.mark.unit
def test_load_lease_urls_missing_file(tmp_path: Path):
    from kgs_pipeline.acquire import load_lease_urls

    with pytest.raises(FileNotFoundError):
        load_lease_urls(tmp_path / "nonexistent.txt")


@pytest.mark.unit
def test_load_lease_urls_no_url_column(tmp_path: Path):
    from kgs_pipeline.acquire import load_lease_urls

    f = tmp_path / "leases.txt"
    f.write_text("LEASE_KID,LEASE\n1001,Test\n", encoding="utf-8")
    with pytest.raises(KeyError):
        load_lease_urls(f)


# ---------------------------------------------------------------------------
# Task 04: scrape_lease_page tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scrape_idempotency_skips_existing(tmp_path: Path):
    """If the expected output file already exists and is valid, skip scraping."""
    from kgs_pipeline.acquire import scrape_lease_page

    # Create a pre-existing valid file
    existing = tmp_path / "lp564.txt"
    existing.write_text("header\ndata row\n", encoding="utf-8")

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"

    mock_browser = MagicMock()
    semaphore = asyncio.Semaphore(1)

    result = asyncio.run(
        scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
    )
    assert result == existing
    mock_browser.new_page.assert_not_called()


@pytest.mark.unit
def test_scrape_returns_none_on_missing_button(tmp_path: Path):
    """Returns None when the 'Save Monthly Data to File' button is not found."""
    from kgs_pipeline.acquire import scrape_lease_page

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=999"

    mock_page = AsyncMock()
    mock_page.query_selector = AsyncMock(return_value=None)
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(1)
    result = asyncio.run(
        scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
    )
    assert result is None


@pytest.mark.unit
def test_scrape_returns_none_on_timeout(tmp_path: Path):
    """Returns None when navigation raises TimeoutError."""
    from kgs_pipeline.acquire import scrape_lease_page

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=888"

    mock_page = AsyncMock()
    mock_page.goto = AsyncMock(side_effect=TimeoutError("timeout"))
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(1)
    result = asyncio.run(
        scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
    )
    assert result is None


@pytest.mark.unit
def test_scrape_returns_none_on_missing_txt_link(tmp_path: Path):
    """Returns None when MonthSave page has no .txt link."""
    from kgs_pipeline.acquire import scrape_lease_page

    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=777"

    # First query_selector returns a button (save btn), second returns None (no txt link)
    mock_save_btn = AsyncMock()
    mock_save_btn.get_attribute = AsyncMock(
        return_value="https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=777"
    )

    mock_page = AsyncMock()
    # First call → save btn found, second call → txt link not found
    mock_page.query_selector = AsyncMock(side_effect=[mock_save_btn, None])
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(1)
    result = asyncio.run(
        scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
    )
    assert result is None


# ---------------------------------------------------------------------------
# Task 05: run_acquire_pipeline tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_pipeline_success(tmp_path: Path):
    """Returns list of 3 Paths when all leases succeed."""
    from kgs_pipeline.acquire import run_acquire_pipeline

    fake_paths = [tmp_path / f"lp{i}.txt" for i in range(3)]
    for p in fake_paths:
        p.write_text("header\ndata\n", encoding="utf-8")

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["u1", "u2", "u3"]),
        patch("kgs_pipeline.acquire._run_single_lease", side_effect=fake_paths),
    ):
        result = run_acquire_pipeline(
            lease_index_path=tmp_path / "fake_index.txt",
            output_dir=tmp_path,
        )
    assert len(result) == 3
    assert all(isinstance(p, Path) for p in result)


@pytest.mark.unit
def test_run_acquire_pipeline_all_none(tmp_path: Path):
    """Returns empty list when all leases return None."""
    from kgs_pipeline.acquire import run_acquire_pipeline

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["u1", "u2"]),
        patch("kgs_pipeline.acquire._run_single_lease", return_value=None),
    ):
        result = run_acquire_pipeline(
            lease_index_path=tmp_path / "fake_index.txt",
            output_dir=tmp_path,
        )
    assert result == []


@pytest.mark.unit
def test_run_acquire_pipeline_partial_success(tmp_path: Path):
    """Returns only the 2 successful paths when one returns None."""
    from kgs_pipeline.acquire import run_acquire_pipeline

    p1 = tmp_path / "lp1.txt"
    p1.write_text("header\ndata\n", encoding="utf-8")
    p2 = tmp_path / "lp2.txt"
    p2.write_text("header\ndata\n", encoding="utf-8")

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["u1", "u2", "u3"]),
        patch(
            "kgs_pipeline.acquire._run_single_lease",
            side_effect=[p1, p2, None],
        ),
    ):
        result = run_acquire_pipeline(
            lease_index_path=tmp_path / "fake_index.txt",
            output_dir=tmp_path,
        )
    assert len(result) == 2


@pytest.mark.unit
def test_run_acquire_pipeline_propagates_file_not_found(tmp_path: Path):
    """Propagates FileNotFoundError from load_lease_urls."""
    from kgs_pipeline.acquire import run_acquire_pipeline

    with patch(
        "kgs_pipeline.acquire.load_lease_urls",
        side_effect=FileNotFoundError("not found"),
    ):
        with pytest.raises(FileNotFoundError):
            run_acquire_pipeline(
                lease_index_path=tmp_path / "missing.txt",
                output_dir=tmp_path,
            )


@pytest.mark.unit
def test_run_acquire_pipeline_idempotent(tmp_path: Path):
    """Second run with same output_dir produces same file count, not doubled."""
    from kgs_pipeline.acquire import run_acquire_pipeline

    p1 = tmp_path / "lp1.txt"
    p1.write_text("header\ndata\n", encoding="utf-8")

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["u1"]),
        patch("kgs_pipeline.acquire._run_single_lease", return_value=p1),
    ):
        run_acquire_pipeline(
            lease_index_path=tmp_path / "fake_index.txt",
            output_dir=tmp_path,
        )
        result2 = run_acquire_pipeline(
            lease_index_path=tmp_path / "fake_index.txt",
            output_dir=tmp_path,
        )

    assert len(result2) == 1
    assert p1.read_text() == "header\ndata\n"
