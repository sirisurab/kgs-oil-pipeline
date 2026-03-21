"""
Acquire component: Web-scraping workflow to download per-lease monthly data files from KGS.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore

try:
    from playwright.async_api import async_playwright  # type: ignore
except ImportError:
    async_playwright = None  # type: ignore

import dask  # type: ignore
import dask.delayed  # type: ignore

from kgs_pipeline.config import MAX_CONCURRENT_SCRAPES, RAW_DATA_DIR

logger = logging.getLogger(__name__)


class ScrapeError(Exception):
    """Custom exception for unrecoverable scraping failures."""
    pass


def extract_lease_urls(leases_file: Path) -> pd.DataFrame:
    """
    Read the archives file and return a deduplicated DataFrame of unique lease IDs and URLs.

    Args:
        leases_file: Path to the oil_leases_2020_present.txt file.

    Returns:
        DataFrame with columns ['lease_kid', 'url'].

    Raises:
        FileNotFoundError: If leases_file does not exist.
        KeyError: If the URL column is missing from the file.
    """
    if not leases_file.exists():
        raise FileNotFoundError(f"Leases file not found: {leases_file}")

    df = pd.read_csv(leases_file, dtype=str, quotechar='"', sep=",")

    if "URL" not in df.columns:
        raise KeyError(f"URL column not found in {leases_file}")

    # Drop rows with null or empty URL
    df = df[df["URL"].notna() & (df["URL"].str.strip() != "")]

    # Select and rename columns
    df = df[["LEASE_KID", "URL"]].copy()
    df.columns = ["lease_kid", "url"]

    # Deduplicate on lease_kid (keep first occurrence)
    df = df.drop_duplicates(subset=["lease_kid"], keep="first")

    logger.info(f"Extracted {len(df)} unique leases from {leases_file}")
    return df


async def scrape_lease_page(
    lease_url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    playwright_instance: object,
) -> Optional[Path]:
    """
    Asynchronously scrape one KGS lease page and download the monthly data file.

    Args:
        lease_url: Full URL to the lease page.
        output_dir: Directory to write the downloaded file.
        semaphore: Semaphore to limit concurrent requests.
        playwright_instance: The async_playwright context manager instance.

    Returns:
        Path to the downloaded file, or None if download failed or file was skipped.

    Raises:
        ScrapeError: If the "Save Monthly Data to File" button is not found.
    """
    if async_playwright is None:
        raise ImportError("playwright is not installed. Run: pip install playwright")

    # Extract lease_kid from URL query parameter
    lease_kid = None
    if "f_lc=" in lease_url:
        lease_kid = lease_url.split("f_lc=")[-1].split("&")[0]

    async with semaphore:
        browser = None
        try:
            browser = await playwright_instance.chromium.launch()  # type: ignore
            context = await browser.new_context()
            page = await context.new_page()

            logger.debug(f"Navigating to lease page: {lease_url}")
            await page.goto(lease_url, wait_until="networkidle")

            # Find and click the "Save Monthly Data to File" button
            logger.debug(f"Looking for 'Save Monthly Data to File' button on {lease_kid}")
            save_button = None
            buttons = await page.query_selector_all("button, a")
            for button in buttons:
                text = await button.text_content()
                if text and "Save Monthly Data to File" in text:
                    save_button = button
                    break

            if not save_button:
                await context.close()
                await browser.close()
                raise ScrapeError(f"'Save Monthly Data to File' button not found for {lease_url}")

            logger.debug(f"Clicking 'Save Monthly Data to File' button for {lease_kid}")
            await save_button.click()
            await page.wait_for_load_state("networkidle")

            # Find the download link (.txt file)
            logger.debug(f"Looking for download link on MonthSave page for {lease_kid}")
            download_link = None
            filename = None
            links = await page.query_selector_all("a")
            for link in links:
                text = await link.text_content()
                if text and text.strip().endswith(".txt"):
                    filename = text.strip()
                    href = await link.get_attribute("href")
                    if href:
                        # Construct full URL if relative
                        if href.startswith("http"):
                            download_link = href
                        else:
                            download_link = "https://chasm.kgs.ku.edu" + href
                        break

            if not download_link or not filename:
                logger.warning(
                    f"No download link found for lease {lease_kid}. URL: {lease_url}"
                )
                await context.close()
                await browser.close()
                return None

            # Check if file already exists (idempotency)
            output_path = output_dir / filename
            if output_path.exists():
                logger.info(f"File already exists, skipping download: {output_path}")
                await context.close()
                await browser.close()
                return output_path

            # Download the file
            logger.debug(f"Downloading file from {download_link} for lease {lease_kid}")
            response = await page.goto(download_link)
            if response:
                content = await response.body()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(content)
                logger.info(f"Downloaded file for lease {lease_kid}: {output_path}")
            else:
                logger.warning(f"Failed to download file for lease {lease_kid}")
                await context.close()
                await browser.close()
                return None

            await context.close()
            await browser.close()
            return output_path

        except ScrapeError:
            raise
        except Exception as e:
            logger.error(f"Error scraping lease {lease_kid} at {lease_url}: {e}")
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            return None


async def _scrape_chunk(
    chunk: list[tuple[str, str]],
    output_dir: Path,
) -> dict:
    """
    Internal async function to scrape a chunk of leases within a single event loop.

    Args:
        chunk: List of (lease_kid, url) tuples.
        output_dir: Output directory for downloaded files.

    Returns:
        Dict with 'downloaded', 'failed', and 'skipped' lists.
    """
    if async_playwright is None:
        raise ImportError("playwright is not installed. Run: pip install playwright")

    downloaded: list[Path] = []
    failed: list[str] = []
    skipped: list[str] = []

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCRAPES)

    async with async_playwright() as p:
        tasks = [
            scrape_lease_page(url, output_dir, semaphore, p)
            for _, url in chunk
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for (lease_kid, url), result in zip(chunk, results):
        if isinstance(result, Exception):
            logger.error(f"Exception for lease {lease_kid}: {result}")
            failed.append(url)
        elif result is None:
            failed.append(url)
        else:
            if isinstance(result, Path) and result.exists():
                if result.stat().st_size > 0:
                    downloaded.append(result)
                else:
                    failed.append(url)
            else:
                failed.append(url)

    return {
        "downloaded": downloaded,
        "failed": failed,
        "skipped": skipped,
    }


def run_scrape_pipeline(leases_file: Path, output_dir: Path = RAW_DATA_DIR) -> dict:
    """
    Orchestrate the full parallel scraping run for all leases.

    Args:
        leases_file: Path to the oil_leases_2020_present.txt file.
        output_dir: Output directory for downloaded files.

    Returns:
        Summary dict with keys 'downloaded', 'failed', 'skipped', 'total_leases'.

    Raises:
        FileNotFoundError: If leases_file does not exist.
    """
    logger.info(f"Starting scrape pipeline from {leases_file}")

    # Extract lease URLs
    df = extract_lease_urls(leases_file)
    total_leases = len(df)

    # Convert to list of (lease_kid, url) tuples
    lease_tuples = list(df[["lease_kid", "url"]].itertuples(index=False, name=None))

    # Partition into chunks (e.g., 10 leases per chunk)
    chunk_size = 10
    chunks = [lease_tuples[i : i + chunk_size] for i in range(0, len(lease_tuples), chunk_size)]

    # Create delayed tasks for each chunk
    delayed_tasks = [
        dask.delayed(_scrape_chunk)(chunk, output_dir)
        for chunk in chunks
    ]

    # Compute all tasks using threads scheduler (I/O-bound)
    results = dask.compute(*delayed_tasks, scheduler="threads")

    # Aggregate results - results is a tuple of dicts
    all_downloaded: list[Path] = []
    all_failed: list[str] = []
    all_skipped: list[str] = []

    for result in results:
        if isinstance(result, dict):
            all_downloaded.extend(result.get("downloaded", []))
            all_failed.extend(result.get("failed", []))
            all_skipped.extend(result.get("skipped", []))

    summary = {
        "downloaded": all_downloaded,
        "failed": all_failed,
        "skipped": all_skipped,
        "total_leases": total_leases,
    }

    logger.info(
        f"Scrape pipeline complete. Downloaded: {len(all_downloaded)}, "
        f"Failed: {len(all_failed)}, Skipped: {len(all_skipped)}, "
        f"Total: {total_leases}"
    )

    return summary
