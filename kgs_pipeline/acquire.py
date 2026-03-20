import logging
import asyncio
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore[import-untyped]
from dask import delayed
import dask
from kgs_pipeline.config import MAX_CONCURRENT_SCRAPES

logger = logging.getLogger(__name__)


class ScrapeError(Exception):
    """Raised when a lease page scraping operation fails unrecoverably."""
    pass


def extract_lease_urls(leases_file: Path) -> pd.DataFrame:
    """
    Read the archives file and return a deduplicated DataFrame of unique lease
    identifiers and their URLs.

    Args:
        leases_file: Path to the oil leases archive file.

    Returns:
        DataFrame with columns ["lease_kid", "url"], deduplicated on lease_kid.

    Raises:
        FileNotFoundError: If the leases file does not exist.
        KeyError: If the URL column is missing from the file.
    """
    if not leases_file.exists():
        raise FileNotFoundError(f"Leases file not found: {leases_file}")

    df = pd.read_csv(leases_file, dtype=str, quotechar='"', sep=",")

    if "URL" not in df.columns:
        raise KeyError(f"URL column not found in {leases_file}")

    # Drop rows where URL is null or empty
    df = df[df["URL"].notna() & (df["URL"] != "")]

    # Select and rename columns
    df = df[["LEASE_KID", "URL"]].rename(
        columns={"LEASE_KID": "lease_kid", "URL": "url"}
    )

    # Deduplicate on lease_kid (keep first)
    df = df.drop_duplicates(subset=["lease_kid"], keep="first")

    logger.info(f"Found {len(df)} unique leases in {leases_file}")

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
        lease_url: The URL of the lease page to scrape.
        output_dir: Directory to write the downloaded file to.
        semaphore: Asyncio semaphore to limit concurrent requests.
        playwright_instance: Playwright async browser instance.

    Returns:
        Path to the downloaded file, or None if the download could not be completed.

    Raises:
        ScrapeError: If the "Save Monthly Data to File" button is not found.
    """
    async with semaphore:
        # Extract lease_kid from URL query parameter
        try:
            lease_kid = lease_url.split("f_lc=")[-1]
        except Exception:
            lease_kid = "unknown"

        page = None
        try:
            page = await playwright_instance.new_page()
            logger.debug(f"Navigating to lease page: {lease_url}")
            await page.goto(lease_url, wait_until="networkidle")

            # Find and click "Save Monthly Data to File" button
            logger.debug(f"Searching for 'Save Monthly Data to File' button on {lease_kid}")
            try:
                button = page.locator("button, a", has_text="Save Monthly Data to File")
                await button.first.click()
                logger.debug(f"Clicked 'Save Monthly Data to File' for {lease_kid}")
            except Exception:
                raise ScrapeError(
                    f"'Save Monthly Data to File' button not found on {lease_url}"
                )

            # Wait for navigation to MonthSave page
            await page.wait_for_load_state("networkidle")

            # Find the download link (should be a .txt file)
            logger.debug(f"Searching for .txt download link on MonthSave page for {lease_kid}")
            try:
                download_link = page.locator('a[href*=".txt"]').first
                href = await download_link.get_attribute("href")
                filename = href.split("/")[-1] if href else None

                if not filename or not filename.endswith(".txt"):
                    logger.warning(
                        f"Could not extract .txt filename from download link for {lease_kid}"
                    )
                    return None
            except Exception:
                logger.warning(
                    f"No .txt download link found on MonthSave page for {lease_kid}"
                )
                return None

            # Construct full URL if relative
            if href.startswith("http"):
                download_url = href
            else:
                download_url = f"https://chasm.kgs.ku.edu{href}"

            # Check if file already exists (idempotency)
            output_file = output_dir / filename
            if output_file.exists():
                logger.info(f"File already exists for {lease_kid}: {output_file}")
                return output_file

            # Download the file
            logger.debug(f"Downloading {filename} for {lease_kid} from {download_url}")
            async with playwright_instance.api_request.get(download_url) as response:
                if response.ok:
                    content = await response.body()
                    output_file.write_bytes(content)
                    logger.info(f"Downloaded {filename} for lease {lease_kid}")
                    return output_file
                else:
                    logger.error(
                        f"Failed to download {download_url} (status {response.status})"
                    )
                    return None

        except ScrapeError:
            raise
        except Exception as e:
            logger.error(f"Error scraping {lease_url}: {type(e).__name__}: {e}")
            return None
        finally:
            if page:
                await page.close()


async def _scrape_batch(
    urls: list[str], output_dir: Path
) -> tuple[list[Path], list[str], list[Path]]:
    """
    Scrape a batch of lease URLs concurrently within a single async context.

    Args:
        urls: List of lease URLs to scrape.
        output_dir: Output directory for downloaded files.

    Returns:
        Tuple of (downloaded_paths, failed_urls, skipped_paths).
    """
    from playwright.async_api import async_playwright  # type: ignore[import-not-found]

    output_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCRAPES)
    downloaded: list[Path] = []
    failed: list[str] = []
    skipped: list[Path] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        context = await browser.new_context()

        tasks = []
        for url in urls:
            tasks.append(scrape_lease_page(url, output_dir, semaphore, context))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                logger.error(f"Exception while scraping {url}: {result}")
                failed.append(url)
            elif result is None:
                failed.append(url)
            elif isinstance(result, Path) and result.exists():
                if result.stat().st_size > 0:
                    downloaded.append(result)
                else:
                    failed.append(url)
            else:
                failed.append(url)

        await context.close()
        await browser.close()

    return downloaded, failed, skipped


def _scrape_batch_wrapper(urls: list[str], output_dir: Path) -> dict:
    """
    Wrapper to run async batch scraping in a sync context (for Dask).

    Args:
        urls: List of lease URLs.
        output_dir: Output directory for downloaded files.

    Returns:
        Dict with keys "downloaded", "failed", "skipped".
    """
    try:
        downloaded, failed, skipped = asyncio.run(_scrape_batch(urls, output_dir))
        return {
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
        }
    except Exception as e:
        logger.error(f"Error in batch scraping: {type(e).__name__}: {e}")
        return {
            "downloaded": [],
            "failed": urls,
            "skipped": [],
        }


def run_scrape_pipeline(leases_file: Path, output_dir: Path) -> dict:
    """
    Orchestrate the full parallel scraping run for all leases.

    Args:
        leases_file: Path to the oil leases archive file.
        output_dir: Directory to write downloaded files.

    Returns:
        Dict with keys:
        - "downloaded": list of successfully downloaded file Paths.
        - "failed": list of failed lease URLs.
        - "skipped": list of skipped file Paths (already existed).
        - "total_leases": total number of leases attempted.
    """
    if not leases_file.exists():
        raise FileNotFoundError(f"Leases file not found: {leases_file}")

    output_dir.mkdir(parents=True, exist_ok=True)
    leases_df = extract_lease_urls(leases_file)
    total_leases = len(leases_df)

    # Partition leases into batches for Dask
    batch_size = max(1, total_leases // 4)  # Use 4 batches, at least 1 lease per batch
    batches = [
        leases_df.iloc[i : i + batch_size]["url"].tolist()
        for i in range(0, len(leases_df), batch_size)
    ]

    # Create Dask delayed tasks for each batch
    tasks = [
        delayed(_scrape_batch_wrapper)(batch, output_dir)
        for batch in batches
    ]

    # Compute all tasks using threads scheduler
    try:
        results = dask.compute(*tasks, scheduler="threads")
    except Exception as e:
        logger.error(f"Error during Dask computation: {type(e).__name__}: {e}")
        results = []

    # Aggregate results
    downloaded_all: list[Path] = []
    failed_all: list[str] = []
    skipped_all: list[Path] = []

    for result in results:
        if isinstance(result, dict):
            downloaded_all.extend(result.get("downloaded", []))
            failed_all.extend(result.get("failed", []))
            skipped_all.extend(result.get("skipped", []))

    # Log summary
    logger.info(
        f"Scrape pipeline complete: {len(downloaded_all)} downloaded, "
        f"{len(failed_all)} failed, {len(skipped_all)} skipped out of {total_leases} total"
    )

    return {
        "downloaded": downloaded_all,
        "failed": failed_all,
        "skipped": skipped_all,
        "total_leases": total_leases,
    }
