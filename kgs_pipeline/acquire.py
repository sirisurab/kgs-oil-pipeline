"""Acquire component: download raw KGS oil production data via web scraping."""

import asyncio
import logging
from pathlib import Path

import dask
import pandas as pd
import playwright.async_api  # type: ignore[import]

from kgs_pipeline.config import (
    KGS_BASE_URL,
    KGS_MONTH_SAVE_URL,
    LEASE_INDEX_FILE,
    RAW_DATA_DIR,
    SCRAPE_CONCURRENCY,
    SCRAPE_TIMEOUT_MS,
)

logger = logging.getLogger(__name__)


class ScrapingError(Exception):
    """Raised when a critical scraping operation fails."""

    pass


def load_lease_urls(lease_index_path: Path) -> list[dict]:
    """
    Read the lease index file and extract unique lease URLs.

    Parameters
    ----------
    lease_index_path : Path
        Path to the lease index CSV file (e.g., oil_leases_2020_present.txt).

    Returns
    -------
    list[dict]
        List of dicts with keys 'lease_kid' and 'url', one per unique lease.

    Raises
    ------
    FileNotFoundError
        If lease_index_path does not exist.
    KeyError
        If required columns 'LEASE_KID' or 'URL' are missing.
    """
    if not lease_index_path.exists():
        raise FileNotFoundError(f"Lease index file not found: {lease_index_path}")

    df = pd.read_csv(lease_index_path)

    # Check for required columns
    if "LEASE_KID" not in df.columns:
        raise KeyError("Column 'LEASE_KID' not found in lease index file")
    if "URL" not in df.columns:
        raise KeyError("Column 'URL' not found in lease index file")

    # Deduplicate on LEASE_KID and drop null URLs
    df = df.dropna(subset=["URL"])
    df = df.drop_duplicates(subset=["LEASE_KID"], keep="first")

    # Return as list of dicts
    result = [
        {"lease_kid": row["LEASE_KID"], "url": row["URL"]}
        for _, row in df.iterrows()
    ]
    return result


async def scrape_lease_page(
    lease_kid: str,
    url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    browser: "playwright.async_api.Browser",  # type: ignore[name-defined]
) -> Path | None:
    """
    Scrape monthly data for a single lease and download the .txt file.

    Parameters
    ----------
    lease_kid : str
        Unique lease identifier.
    url : str
        Main lease page URL.
    output_dir : Path
        Directory to save downloaded files.
    semaphore : asyncio.Semaphore
        Rate-limiting semaphore (max 5 concurrent).
    browser : playwright.async_api.Browser
        Playwright browser instance.

    Returns
    -------
    Path | None
        Path to downloaded file on success, None if download link missing or error.

    Raises
    ------
    ScrapingError
        If the "Save Monthly Data to File" button is not found.
    """
    async with semaphore:
        page = None
        try:
            # Check if file already exists (idempotent)
            # We'll check after finding the filename, so proceed with scraping
            page = await browser.new_page()
            await page.goto(url, timeout=SCRAPE_TIMEOUT_MS)

            # Locate "Save Monthly Data to File" button/link
            save_button = await page.query_selector(
                'a:has-text("Save Monthly Data to File"), button:has-text("Save Monthly Data to File")'
            )
            if not save_button:
                raise ScrapingError(
                    f"No 'Save Monthly Data to File' button found for lease {lease_kid}"
                )

            # Click to navigate to MonthSave page
            await save_button.click()

            # Wait for navigation and locate .txt download link
            await page.wait_for_load_state("networkidle", timeout=SCRAPE_TIMEOUT_MS)

            # Look for an anchor with href ending in .txt
            download_link = await page.query_selector('a[href$=".txt"]')
            if not download_link:
                logger.warning(f"No .txt download link found for lease {lease_kid}")
                return None

            # Get href and extract filename
            href = await download_link.get_attribute("href")
            if not href:
                logger.warning(f"Could not get href for lease {lease_kid}")
                return None

            filename = href.split("/")[-1]
            output_path = output_dir / filename

            # Check if already exists (idempotent)
            if output_path.exists():
                logger.info(f"File already exists for lease {lease_kid}: {output_path}")
                return output_path

            # Download file
            async with page.expect_download() as download_info:
                await download_link.click()
            download = await download_info.value
            await download.save_as(output_path)

            logger.info(f"Downloaded {filename} for lease {lease_kid}")
            return output_path

        except playwright.async_api.TimeoutError:  # type: ignore[attr-defined]
            logger.error(f"Timeout scraping lease {lease_kid} at {url}")
            return None
        except ScrapingError:
            raise
        except Exception as e:
            logger.error(f"Error scraping lease {lease_kid}: {e}")
            return None
        finally:
            if page:
                await page.close()


def _scrape_one_lease(
    lease_kid: str, url: str, output_dir: Path
) -> Path | None:
    """
    Wrapper to run async scraping in a sync context (for Dask delayed).

    Parameters
    ----------
    lease_kid : str
        Unique lease identifier.
    url : str
        Main lease page URL.
    output_dir : Path
        Directory to save downloaded files.

    Returns
    -------
    Path | None
        Path to downloaded file, or None on error.
    """
    try:

        async def _async_scrape() -> Path | None:
            async with playwright.async_api.async_playwright() as pw:  # type: ignore[attr-defined]
                browser = await pw.chromium.launch()
                semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)
                result = await scrape_lease_page(
                    lease_kid, url, output_dir, semaphore, browser
                )
                await browser.close()
                return result

        return asyncio.run(_async_scrape())
    except Exception as e:
        logger.error(f"Error in _scrape_one_lease for lease {lease_kid}: {e}")
        return None


def run_acquire_pipeline(
    lease_index_path: Path = LEASE_INDEX_FILE, output_dir: Path = RAW_DATA_DIR
) -> list[Path]:
    """
    Orchestrate the full async scraping workflow using Dask for task scheduling.

    Parameters
    ----------
    lease_index_path : Path
        Path to lease index file.
    output_dir : Path
        Directory to save raw .txt files.

    Returns
    -------
    list[Path]
        List of successfully downloaded file paths.
    """
    logger.info("Starting acquire pipeline")

    # Load lease URLs
    leases = load_lease_urls(lease_index_path)
    logger.info(f"Loaded {len(leases)} unique leases")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build Dask delayed tasks
    delayed_tasks = [
        dask.delayed(_scrape_one_lease)(lease["lease_kid"], lease["url"], output_dir)
        for lease in leases
    ]

    # Compute all tasks
    logger.info(f"Starting scraping of {len(delayed_tasks)} leases")
    results = dask.compute(*delayed_tasks)

    # Filter out None results and count stats
    successful = [p for p in results if p is not None]
    failed = len(results) - len(successful)

    logger.info(
        f"Acquire complete: {len(successful)} successful, {failed} failed/skipped"
    )

    return successful
