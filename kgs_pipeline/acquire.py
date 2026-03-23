"""Acquire component — scrapes KGS lease pages and downloads monthly production files."""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

import kgs_pipeline.config as config

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task 02: Custom exception
# ---------------------------------------------------------------------------


class ScrapingError(Exception):
    """Raised when the Playwright scraper encounters a fatal per-lease error."""

    def __init__(self, message: str, lease_id: str = "") -> None:
        self.lease_id = lease_id
        full_message = f"{message} (lease_id={lease_id})" if lease_id else message
        super().__init__(full_message)


# ---------------------------------------------------------------------------
# Task 03: Lease index reader
# ---------------------------------------------------------------------------


def load_lease_urls(index_file: Path) -> list[str]:
    """Read deduplicated lease URLs from the KGS archive index file.

    Args:
        index_file: Path to the tab- or comma-delimited lease index file.

    Returns:
        Deduplicated list of URL strings.

    Raises:
        FileNotFoundError: If the index file does not exist.
        KeyError: If the URL column is absent.
    """
    if not index_file.exists():
        raise FileNotFoundError(f"Lease index file not found: {index_file}")

    df = pd.read_csv(index_file, sep=",", dtype=str, low_memory=False)
    if df.shape[1] == 1:
        df = pd.read_csv(index_file, sep="\t", dtype=str, low_memory=False)

    df.columns = [c.strip() for c in df.columns]

    if "URL" not in df.columns:
        raise KeyError(
            f"'URL' column not found in {index_file}. Available columns: {list(df.columns)}"
        )

    urls: list[str] = (
        df["URL"]
        .dropna()
        .loc[lambda s: s.str.strip() != ""]
        .drop_duplicates()
        .tolist()
    )

    logger.info("Loaded %d unique lease URLs from %s", len(urls), index_file)
    return urls


# ---------------------------------------------------------------------------
# Task 04: Per-lease Playwright scraper
# ---------------------------------------------------------------------------


async def scrape_lease_page(
    lease_url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    browser: "playwright.async_api.Browser",  # type: ignore[name-defined]  # noqa: F821
) -> Path | None:
    """Scrape a single KGS lease page and download the monthly production .txt file.

    Args:
        lease_url: Full URL to the lease page.
        output_dir: Directory where the downloaded file is saved.
        semaphore: Asyncio semaphore limiting concurrent page contexts.
        browser: Playwright Browser instance.

    Returns:
        Path to the downloaded file, or None if no .txt link was found.

    Raises:
        ScrapingError: If the "Save Monthly Data to File" button is absent,
                       or on any Playwright navigation error.
    """
    from playwright.async_api import Error as PlaywrightError  # noqa: PLC0415

    parsed = urllib.parse.urlparse(lease_url)
    params = urllib.parse.parse_qs(parsed.query)
    lease_id = params.get("f_lc", [""])[0]

    async with semaphore:
        page = await browser.new_page()
        try:
            await page.goto(lease_url, timeout=30_000)

            save_btn = page.locator("text=Save Monthly Data to File")
            count = await save_btn.count()
            if count == 0:
                raise ScrapingError(
                    "No 'Save Monthly Data to File' button found", lease_id=lease_id
                )

            async with page.expect_download() as download_info:
                await save_btn.first.click()

            download = await download_info.value

            # Locate .txt link on MonthSave page
            txt_link = page.locator("a[href$='.txt']")
            txt_count = await txt_link.count()
            if txt_count == 0:
                logger.warning("No .txt download link found for lease URL: %s", lease_url)
                return None

            href = await txt_link.first.get_attribute("href")
            if not href:
                logger.warning("Empty href on .txt link for lease URL: %s", lease_url)
                return None

            filename = Path(href).name
            dest = output_dir / filename

            if dest.exists():
                logger.debug("Skipping — already downloaded: %s", dest)
                return dest

            await download.save_as(str(dest))
            logger.info("Downloaded: %s", dest)
            return dest

        except PlaywrightError as exc:
            logger.error("Playwright error for %s: %s", lease_url, exc)
            raise ScrapingError(str(exc), lease_id=lease_id) from exc
        finally:
            await page.close()


# ---------------------------------------------------------------------------
# Task 05: Acquire pipeline orchestrator
# ---------------------------------------------------------------------------


async def _scrape_chunk(
    chunk: list[str],
    output_dir: Path,
    max_concurrent: int,
) -> list[Path | None]:
    """Run Playwright scraping for a chunk of lease URLs.

    Launches one browser per chunk and uses a shared semaphore for concurrency.
    """
    from playwright.async_api import async_playwright  # noqa: PLC0415

    semaphore = asyncio.Semaphore(max_concurrent)
    results: list[Path | None] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            tasks = [
                scrape_lease_page(url, output_dir, semaphore, browser)
                for url in chunk
            ]
            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
            for url, result in zip(chunk, raw_results):
                if isinstance(result, ScrapingError):
                    logger.error("ScrapingError for %s: %s", url, result)
                elif isinstance(result, Exception):
                    logger.error("Unexpected error for %s: %s", url, result)
                else:
                    results.append(result)  # type: ignore[arg-type]
        finally:
            await browser.close()

    return results


def _run_chunk_sync(args: tuple[list[str], Path, int]) -> list[Path | None]:
    """Synchronous wrapper for _scrape_chunk — used as a Dask delayed target."""
    chunk, output_dir, max_concurrent = args
    return asyncio.run(_scrape_chunk(chunk, output_dir, max_concurrent))


def run_acquire_pipeline() -> list[Path]:
    """Orchestrate the full acquisition workflow end-to-end.

    Reads the lease index, partitions URLs into chunks, and processes each chunk
    via Dask delayed tasks backed by Playwright async scraping.

    Returns:
        Flat list of successfully downloaded file Paths.
    """
    import dask  # noqa: PLC0415

    lease_index = config.LEASE_INDEX_FILE
    output_dir = config.RAW_DATA_DIR
    max_concurrent = config.MAX_CONCURRENT_REQUESTS
    n_workers = config.DASK_N_WORKERS

    urls = load_lease_urls(lease_index)
    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_size = max_concurrent * 4
    chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]

    delayed_tasks = [
        dask.delayed(_run_chunk_sync)((chunk, output_dir, max_concurrent))
        for chunk in chunks
    ]

    try:
        from dask.distributed import LocalCluster, Client  # noqa: PLC0415

        with LocalCluster(n_workers=n_workers, threads_per_worker=1) as cluster:
            with Client(cluster):
                nested = dask.compute(*delayed_tasks)
    except Exception:
        nested = dask.compute(*delayed_tasks, scheduler="synchronous")

    flat: list[Path | None] = [item for sublist in nested for item in sublist]
    downloaded = [p for p in flat if p is not None]
    skipped = len(flat) - len(downloaded)

    logger.info(
        "Acquire complete — attempted: %d, downloaded: %d, skipped/failed: %d",
        len(urls),
        len(downloaded),
        skipped,
    )
    return downloaded
