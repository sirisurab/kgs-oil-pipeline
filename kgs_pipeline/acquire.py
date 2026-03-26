"""Acquire component for the KGS oil production data pipeline.

Responsibilities:
- Load lease URLs from the lease index file.
- Scrape per-lease monthly production .txt files from the KGS web portal
  using Playwright async API with asyncio.Semaphore(5) rate-limiting.
- Save raw files to data/raw/.
- Orchestrate parallel acquisition via Dask delayed tasks.
"""

import asyncio
from pathlib import Path

import dask
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import CONFIG
from kgs_pipeline.utils import ensure_dir, is_valid_raw_file, setup_logging

logger = setup_logging("acquire", CONFIG.logs_dir, CONFIG.log_level)


class ScrapingError(Exception):
    """Raised when Playwright scraping fails due to missing page elements."""


def load_lease_urls(lease_index_path: Path) -> list[str]:
    """Read the lease index file and return a deduplicated list of lease URLs.

    Tries comma delimiter first, then pipe delimiter.

    Args:
        lease_index_path: Path to the lease index .txt file.

    Returns:
        Deduplicated list of non-empty URL strings.

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If no URL column is found in the file.
    """
    if not lease_index_path.exists():
        raise FileNotFoundError(f"Lease index file not found: {lease_index_path}")

    df: pd.DataFrame | None = None
    for sep in [",", "|"]:
        try:
            candidate = pd.read_csv(lease_index_path, sep=sep, dtype=str)
            if "URL" in candidate.columns:
                df = candidate
                break
        except Exception:
            continue

    if df is None:
        # Try with quoting
        try:
            df = pd.read_csv(lease_index_path, dtype=str)
        except Exception as exc:
            raise KeyError(f"Could not parse lease index file: {exc}") from exc

    if "URL" not in df.columns:
        raise KeyError(
            f"'URL' column not found in lease index file. Available columns: {list(df.columns)}"
        )

    urls = df["URL"].dropna().str.strip()
    urls = urls[urls != ""].drop_duplicates().tolist()
    logger.info("Loaded %d unique lease URLs from %s", len(urls), lease_index_path)
    return urls


async def scrape_lease_page(
    lease_url: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    playwright_browser: object,
) -> Path | None:
    """Scrape a single lease page and download the monthly production .txt file.

    Args:
        lease_url: URL of the lease main page.
        output_dir: Directory to save the downloaded file.
        semaphore: asyncio.Semaphore limiting concurrent page contexts.
        playwright_browser: Playwright async browser instance.

    Returns:
        Path to the saved file on success, or None on failure.
    """
    from playwright.async_api import Browser

    browser: Browser = playwright_browser  # type: ignore[assignment]

    async with semaphore:
        page = None
        try:
            # Determine expected filename from URL
            lease_kid = lease_url.split("f_lc=")[-1] if "f_lc=" in lease_url else None
            expected_name = f"lp{lease_kid}.txt" if lease_kid else None

            # Idempotency: skip if already downloaded
            if expected_name:
                existing = output_dir / expected_name
                if is_valid_raw_file(existing):
                    logger.debug("Skipping already-downloaded file: %s", existing)
                    return existing

            page = await browser.new_page()
            await page.goto(lease_url, timeout=CONFIG.scrape_timeout_ms)

            # Find "Save Monthly Data to File" button/link
            save_btn = await page.query_selector('a:has-text("Save Monthly Data to File")')
            if save_btn is None:
                raise ScrapingError(
                    f"'Save Monthly Data to File' button not found on page: {lease_url}"
                )

            month_save_href = await save_btn.get_attribute("href")
            if month_save_href is None:
                # Try clicking and waiting for navigation
                async with page.expect_navigation(timeout=CONFIG.scrape_timeout_ms):
                    await save_btn.click()
                month_save_url = page.url
            else:
                if not month_save_href.startswith("http"):
                    month_save_url = f"{CONFIG.kgs_base_url}/{month_save_href.lstrip('/')}"
                else:
                    month_save_url = month_save_href
                await page.goto(month_save_url, timeout=CONFIG.scrape_timeout_ms)

            # Find .txt download link on MonthSave page
            txt_link = await page.query_selector('a[href$=".txt"]')
            if txt_link is None:
                raise ScrapingError(
                    f"No .txt download link found on MonthSave page for: {lease_url}"
                )

            href = await txt_link.get_attribute("href")
            if not href:
                raise ScrapingError(f"Empty href on .txt link for: {lease_url}")

            filename = href.split("/")[-1]
            if not filename.endswith(".txt"):
                filename = href.rsplit("/", 1)[-1]

            download_url = (
                href if href.startswith("http") else f"{CONFIG.kgs_base_url}/{href.lstrip('/')}"
            )

            # Download file content via the page's fetch
            content = await page.evaluate(
                f"""async () => {{
                    const r = await fetch("{download_url}");
                    return await r.text();
                }}"""
            )

            output_path = output_dir / filename
            output_path.write_text(content, encoding="utf-8")
            logger.info("Downloaded: %s", output_path)
            return output_path

        except Exception as exc:
            logger.error("Failed to scrape lease URL %s: %s", lease_url, exc)
            return None
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass


def _run_single_lease(lease_url: str, output_dir: Path) -> Path | None:
    """Synchronous wrapper to run async scrape for a single lease.

    Args:
        lease_url: The lease page URL.
        output_dir: Directory for downloaded files.

    Returns:
        Path to the downloaded file, or None on failure.
    """
    import asyncio as _asyncio

    from playwright.async_api import async_playwright

    async def _scrape() -> Path | None:
        semaphore = _asyncio.Semaphore(1)
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                return await scrape_lease_page(lease_url, output_dir, semaphore, browser)
            finally:
                await browser.close()

    return _asyncio.run(_scrape())


def run_acquire_pipeline(
    lease_index_path: Path | None = None,
    output_dir: Path | None = None,
    max_workers: int | None = None,
) -> list[Path]:
    """Orchestrate the full acquisition workflow.

    Downloads raw per-lease monthly .txt files for all leases in the index.
    Uses Dask delayed tasks scheduled with thread concurrency.

    Args:
        lease_index_path: Path to the lease index file (default: CONFIG.lease_index_file).
        output_dir: Directory to write raw files (default: CONFIG.raw_dir).
        max_workers: Number of Dask workers (default: CONFIG.dask_n_workers).

    Returns:
        List of Path objects for successfully written files.

    Raises:
        FileNotFoundError: If lease_index_path does not exist (propagated from load_lease_urls).
    """
    lease_index_path = lease_index_path or CONFIG.lease_index_file
    output_dir = output_dir or CONFIG.raw_dir
    max_workers = max_workers or CONFIG.dask_n_workers

    ensure_dir(output_dir)
    lease_urls = load_lease_urls(lease_index_path)
    total = len(lease_urls)
    logger.info("Starting acquire pipeline: %d leases to process.", total)

    tasks = [
        dask.delayed(_run_single_lease)(url, output_dir)  # type: ignore[attr-defined]
        for url in lease_urls
    ]

    results = dask.compute(*tasks, scheduler="threads", num_workers=max_workers)  # type: ignore[attr-defined]

    succeeded = [r for r in results if r is not None]
    failed = total - len(succeeded)
    logger.info(
        "Acquire complete: %d attempted, %d succeeded, %d failed.",
        total,
        len(succeeded),
        failed,
    )
    return succeeded


if __name__ == "__main__":
    paths = run_acquire_pipeline()
    print(f"Acquired {len(paths)} files.")
