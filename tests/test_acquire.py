"""Tests for kgs_pipeline/acquire.py — Tasks 01–05."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import kgs_pipeline.config as config
from kgs_pipeline.acquire import ScrapingError, load_lease_urls, run_acquire_pipeline


# ---------------------------------------------------------------------------
# Task 01: Config constants
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_config_dirs_are_paths():
    assert isinstance(config.RAW_DATA_DIR, Path)
    assert isinstance(config.INTERIM_DATA_DIR, Path)
    assert isinstance(config.PROCESSED_DATA_DIR, Path)
    assert isinstance(config.FEATURES_DATA_DIR, Path)
    assert isinstance(config.EXTERNAL_DATA_DIR, Path)


@pytest.mark.unit
def test_max_concurrent_requests_equals_5():
    assert config.MAX_CONCURRENT_REQUESTS == 5


@pytest.mark.unit
def test_kgs_base_url_starts_with_https():
    assert config.KGS_BASE_URL.startswith("https://")


@pytest.mark.unit
def test_lease_index_file_ends_with_txt():
    assert str(config.LEASE_INDEX_FILE).endswith(".txt")


# ---------------------------------------------------------------------------
# Task 02: ScrapingError
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scraping_error_message():
    err = ScrapingError("test message")
    assert "test message" in str(err)


@pytest.mark.unit
def test_scraping_error_with_lease_id():
    err = ScrapingError("something went wrong", lease_id="1001135839")
    assert "1001135839" in str(err)


@pytest.mark.unit
def test_scraping_error_is_exception():
    assert isinstance(ScrapingError("x"), Exception)


@pytest.mark.unit
def test_scraping_error_catchable():
    try:
        raise ScrapingError("x")
    except Exception:
        pass
    else:
        pytest.fail("ScrapingError was not caught by 'except Exception'")


# ---------------------------------------------------------------------------
# Task 03: load_lease_urls
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_lease_urls_deduplication(tmp_path: Path):
    csv = tmp_path / "leases.txt"
    csv.write_text(
        "LEASE KID,URL\n"
        "1,https://example.com/lease1\n"
        "2,https://example.com/lease2\n"
        "3,https://example.com/lease1\n"  # duplicate
        "4,\n"  # null URL
    )
    urls = load_lease_urls(csv)
    assert len(urls) == 2
    assert all(isinstance(u, str) for u in urls)


@pytest.mark.unit
def test_load_lease_urls_missing_url_column(tmp_path: Path):
    csv = tmp_path / "leases.txt"
    csv.write_text("LEASE KID,OPERATOR\n1,Acme\n")
    with pytest.raises(KeyError):
        load_lease_urls(csv)


@pytest.mark.unit
def test_load_lease_urls_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_lease_urls(Path("/nonexistent/path/leases.txt"))


@pytest.mark.unit
def test_load_lease_urls_returns_list(tmp_path: Path):
    csv = tmp_path / "leases.txt"
    csv.write_text("LEASE KID,URL\n1,https://example.com/a\n2,https://example.com/b\n")
    result = load_lease_urls(csv)
    assert isinstance(result, list)
    assert all(isinstance(u, str) for u in result)


@pytest.mark.integration
def test_load_lease_urls_real_file():
    if not config.LEASE_INDEX_FILE.exists():
        pytest.skip("Lease index file not available")
    urls = load_lease_urls(config.LEASE_INDEX_FILE)
    assert len(urls) > 0
    assert all(u.startswith("https://") for u in urls)


# ---------------------------------------------------------------------------
# Task 04: scrape_lease_page (unit tests using mocks)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scrape_lease_page_returns_path(tmp_path: Path):
    """When page has save button and txt link, returns a Path ending with .txt."""
    from kgs_pipeline.acquire import scrape_lease_page

    # Mock download object
    mock_download = AsyncMock()
    mock_download.save_as = AsyncMock()

    # Mock page
    mock_page = AsyncMock()
    save_btn_locator = AsyncMock()
    save_btn_locator.count = AsyncMock(return_value=1)
    save_btn_locator.first = AsyncMock()
    save_btn_locator.first.click = AsyncMock()
    mock_page.locator = MagicMock(return_value=save_btn_locator)

    txt_locator = AsyncMock()
    txt_locator.count = AsyncMock(return_value=1)
    txt_locator.first = AsyncMock()
    txt_locator.first.get_attribute = AsyncMock(return_value="lp564.txt")
    txt_locator.first.click = AsyncMock()

    def _locator_side_effect(selector: str):
        if "Save Monthly" in selector or "text=" in selector:
            return save_btn_locator
        return txt_locator

    mock_page.locator = MagicMock(side_effect=_locator_side_effect)
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    # expect_download context manager
    download_ctx = AsyncMock()
    download_ctx.__aenter__ = AsyncMock(return_value=download_ctx)
    download_ctx.__aexit__ = AsyncMock(return_value=False)
    download_ctx.value = mock_download
    mock_page.expect_download = MagicMock(return_value=download_ctx)

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(5)
    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564"

    result = asyncio.get_event_loop().run_until_complete(
        scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
    )
    assert result is not None
    assert str(result).endswith(".txt")


@pytest.mark.unit
def test_scrape_lease_page_raises_when_no_save_button(tmp_path: Path):
    """When save button is absent, ScrapingError is raised."""
    from kgs_pipeline.acquire import scrape_lease_page

    mock_page = AsyncMock()
    save_locator = AsyncMock()
    save_locator.count = AsyncMock(return_value=0)
    mock_page.locator = MagicMock(return_value=save_locator)
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(5)
    lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=999"

    with pytest.raises(ScrapingError):
        asyncio.get_event_loop().run_until_complete(
            scrape_lease_page(lease_url, tmp_path, semaphore, mock_browser)
        )


@pytest.mark.unit
def test_scrape_lease_page_returns_none_when_no_txt_link(tmp_path: Path):
    """When MonthSave page has no .txt link, returns None."""
    from kgs_pipeline.acquire import scrape_lease_page

    mock_page = AsyncMock()

    save_locator = AsyncMock()
    save_locator.count = AsyncMock(return_value=1)
    save_locator.first = AsyncMock()
    save_locator.first.click = AsyncMock()

    txt_locator = AsyncMock()
    txt_locator.count = AsyncMock(return_value=0)

    download_ctx = AsyncMock()
    download_ctx.__aenter__ = AsyncMock(return_value=download_ctx)
    download_ctx.__aexit__ = AsyncMock(return_value=False)
    download_ctx.value = AsyncMock()
    mock_page.expect_download = MagicMock(return_value=download_ctx)

    call_count = [0]

    def _locator_se(selector: str):
        call_count[0] += 1
        if call_count[0] == 1:
            return save_locator
        return txt_locator

    mock_page.locator = MagicMock(side_effect=_locator_se)
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(5)
    result = asyncio.get_event_loop().run_until_complete(
        scrape_lease_page(
            "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1",
            tmp_path,
            semaphore,
            mock_browser,
        )
    )
    assert result is None


@pytest.mark.unit
def test_scrape_lease_page_skips_existing_file(tmp_path: Path):
    """When the target file already exists, returns it without calling save_as."""
    from kgs_pipeline.acquire import scrape_lease_page

    existing = tmp_path / "lp564.txt"
    existing.write_text("existing")

    mock_page = AsyncMock()
    save_locator = AsyncMock()
    save_locator.count = AsyncMock(return_value=1)
    save_locator.first = AsyncMock()
    save_locator.first.click = AsyncMock()

    txt_locator = AsyncMock()
    txt_locator.count = AsyncMock(return_value=1)
    txt_locator.first = AsyncMock()
    txt_locator.first.get_attribute = AsyncMock(return_value="lp564.txt")

    download_ctx = AsyncMock()
    download_ctx.__aenter__ = AsyncMock(return_value=download_ctx)
    download_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_download = AsyncMock()
    mock_download.save_as = AsyncMock()
    download_ctx.value = mock_download
    mock_page.expect_download = MagicMock(return_value=download_ctx)

    call_count = [0]

    def _locator_se(selector: str):
        call_count[0] += 1
        if call_count[0] == 1:
            return save_locator
        return txt_locator

    mock_page.locator = MagicMock(side_effect=_locator_se)
    mock_page.goto = AsyncMock()
    mock_page.close = AsyncMock()

    mock_browser = AsyncMock()
    mock_browser.new_page = AsyncMock(return_value=mock_page)

    semaphore = asyncio.Semaphore(5)
    result = asyncio.get_event_loop().run_until_complete(
        scrape_lease_page(
            "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=564",
            tmp_path,
            semaphore,
            mock_browser,
        )
    )
    assert result == existing
    mock_download.save_as.assert_not_called()


# ---------------------------------------------------------------------------
# Task 05: run_acquire_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_pipeline_returns_paths(tmp_path: Path):
    dummy_path = tmp_path / "lp1.txt"
    dummy_path.write_text("data")

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["url1", "url2", "url3"]),
        patch("kgs_pipeline.acquire._run_chunk_sync", return_value=[dummy_path, dummy_path, dummy_path]),
        patch("dask.compute", return_value=([dummy_path, dummy_path, dummy_path],)),
    ):
        result = run_acquire_pipeline()
    assert isinstance(result, list)


@pytest.mark.unit
def test_run_acquire_pipeline_handles_scraping_errors(tmp_path: Path):
    dummy_path = tmp_path / "lp1.txt"
    dummy_path.write_text("data")

    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["url1", "url2"]),
        patch("dask.compute", return_value=([dummy_path],)),
    ):
        result = run_acquire_pipeline()
    # Should return list without raising
    assert isinstance(result, list)


@pytest.mark.unit
def test_run_acquire_pipeline_none_results(tmp_path: Path):
    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=["url1"]),
        patch("dask.compute", return_value=([None],)),
    ):
        result = run_acquire_pipeline()
    assert result == []


@pytest.mark.unit
def test_run_acquire_pipeline_return_type(tmp_path: Path):
    with (
        patch("kgs_pipeline.acquire.load_lease_urls", return_value=[]),
        patch("dask.compute", return_value=([], )),
    ):
        result = run_acquire_pipeline()
    assert isinstance(result, list)
