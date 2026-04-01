"""KGS lease production data acquisition module.

Downloads per-lease production files from the KGS CHASM server in parallel using Dask.
"""

import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import dask  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup

from kgs_pipeline.utils import retry, setup_logging

logger = setup_logging(__name__)


class ScrapingError(Exception):
    """Raised when the expected download link cannot be found on a KGS page."""


# ---------------------------------------------------------------------------
# Task 01: Lease index loader
# ---------------------------------------------------------------------------


def load_lease_index(index_path: str) -> pd.DataFrame:
    """Load and filter the lease index CSV to deduplicated rows with year >= 2024.

    Args:
        index_path: Path to the lease index text/CSV file.

    Returns:
        DataFrame with deduplicated rows filtered to year >= 2024.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or no leases survive the year filter.
    """
    path = Path(index_path)
    if not path.exists():
        raise FileNotFoundError(f"Lease index file not found: {index_path}")
    if path.stat().st_size == 0:
        raise ValueError("Lease index file is empty")

    df = pd.read_csv(index_path, dtype=str)
    if df.empty:
        raise ValueError("Lease index file is empty")

    # Extract year from MONTH-YEAR by splitting on "-" and taking the last element
    def _extract_year(month_year: str) -> str | None:
        if not isinstance(month_year, str):
            return None
        parts = month_year.split("-")
        return parts[-1] if parts else None

    df["_year_str"] = df["MONTH-YEAR"].apply(_extract_year)
    # Drop rows where year component is not numeric
    df = df[df["_year_str"].apply(lambda y: isinstance(y, str) and y.isdigit())]
    df["_year"] = df["_year_str"].astype(int)
    df = df[df["_year"] >= 2024]
    df = df.drop(columns=["_year_str", "_year"])

    if df.empty:
        raise ValueError("No leases found for year >= 2024")

    df = df.drop_duplicates(subset=["URL"], keep="first")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Task 02: Lease ID extractor
# ---------------------------------------------------------------------------


def extract_lease_id(url: str) -> str:
    """Parse the f_lc query parameter from a KGS MainLease URL.

    Args:
        url: Full URL with f_lc query parameter.

    Returns:
        Lease ID string.

    Raises:
        ValueError: If f_lc is absent or empty.
    """
    parsed = urlparse(url)
    if not parsed.query:
        raise ValueError(f"URL has no query string: {url}")
    params = parse_qs(parsed.query)
    if "f_lc" not in params:
        raise ValueError(f"URL missing 'f_lc' parameter: {url}")
    value = params["f_lc"][0]
    if not value:
        raise ValueError(f"URL has empty 'f_lc' parameter: {url}")
    return value


# ---------------------------------------------------------------------------
# Task 03: MonthSave page scraper
# ---------------------------------------------------------------------------


@retry(max_retries=3, base_delay=1.0, factor=2.0)
def _get_with_retry(session: requests.Session, url: str) -> requests.Response:
    """Make a GET request with retry logic."""
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response


def scrape_download_url(lease_id: str, session: requests.Session) -> str:
    """Scrape the data file download URL from the KGS MonthSave page.

    Args:
        lease_id: KGS lease identifier string.
        session: Shared requests Session.

    Returns:
        Absolute URL containing 'anon_blobber.download'.

    Raises:
        ScrapingError: If no download link is found on the page.
    """
    month_save_url = f"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"
    response = _get_with_retry(session, month_save_url)
    soup = BeautifulSoup(response.text, "html.parser")
    anchor = soup.find("a", href=lambda h: h and "anon_blobber.download" in h)
    if anchor is None:
        raise ScrapingError(f"No 'anon_blobber.download' link found for lease_id={lease_id}")
    return str(anchor["href"])


# ---------------------------------------------------------------------------
# Task 04: File downloader
# ---------------------------------------------------------------------------


@retry(max_retries=3, base_delay=1.0, factor=2.0)
def _download_with_retry(session: requests.Session, url: str) -> requests.Response:
    """Make a GET request for file download with retry logic."""
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response


def download_file(download_url: str, output_dir: str, session: requests.Session) -> Path:
    """Download a single KGS production file to output_dir.

    Args:
        download_url: Full download URL with p_file_name query parameter.
        output_dir: Directory where the file will be saved.
        session: Shared requests Session.

    Returns:
        Path to the saved (or pre-existing) file.

    Raises:
        ValueError: If p_file_name parameter is missing or response body is empty.
    """
    parsed = urlparse(download_url)
    params = parse_qs(parsed.query)
    if "p_file_name" not in params:
        raise ValueError(f"Download URL missing 'p_file_name' parameter: {download_url}")
    filename = params["p_file_name"][0]

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / filename

    # Idempotency: skip if already downloaded
    if output_path.exists():
        logger.debug("File already exists, skipping: %s", output_path)
        return output_path

    response = _download_with_retry(session, download_url)
    if not response.content:
        raise ValueError(f"Downloaded file is empty: {filename}")

    output_path.write_bytes(response.content)
    time.sleep(0.5)
    logger.info("Downloaded: %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Task 05: Single-lease download worker
# ---------------------------------------------------------------------------


def download_lease(url: str, output_dir: str, session: requests.Session) -> Path | None:
    """Orchestrate the full download workflow for a single lease.

    Args:
        url: Lease main-page URL from the index.
        output_dir: Target directory for raw files.
        session: Shared requests Session.

    Returns:
        Path on success, None on failure.
    """
    try:
        lease_id = extract_lease_id(url)
        download_url = scrape_download_url(lease_id, session)
        return download_file(download_url, output_dir, session)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to download lease %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Task 06: Parallel acquire orchestrator
# ---------------------------------------------------------------------------


def run_acquire(
    index_path: str,
    output_dir: str,
    max_workers: int = 5,
) -> list[Path]:
    """Main entry point for the acquire stage.

    Args:
        index_path: Path to the lease index file.
        output_dir: Directory for raw downloaded files.
        max_workers: Dask thread concurrency limit.

    Returns:
        List of paths to successfully downloaded (or pre-existing) files.
    """
    df = load_lease_index(index_path)
    urls = df["URL"].tolist()
    logger.info("Acquire stage: %d unique lease URLs to process", len(urls))

    with requests.Session() as session:
        tasks = [dask.delayed(download_lease)(url, output_dir, session) for url in urls]
        results = dask.compute(*tasks, scheduler="threads", num_workers=max_workers)

    successful: list[Path] = [r for r in results if r is not None]
    failures = len(results) - len(successful)
    logger.info(
        "Acquire complete: %d attempted, %d succeeded, %d failed",
        len(urls),
        len(successful),
        failures,
    )
    return successful


if __name__ == "__main__":
    import argparse

    from kgs_pipeline.config import LEASE_INDEX_FILE, RAW_DIR

    parser = argparse.ArgumentParser(description="KGS acquire stage")
    parser.add_argument(
        "--index",
        default=str(LEASE_INDEX_FILE),
        help="Path to lease index file",
    )
    parser.add_argument(
        "--output-dir",
        default=str(RAW_DIR),
        help="Output directory for raw files",
    )
    parser.add_argument("--max-workers", type=int, default=5)
    args = parser.parse_args()

    paths = run_acquire(args.index, args.output_dir, args.max_workers)
    print(f"Downloaded {len(paths)} files to {args.output_dir}")
