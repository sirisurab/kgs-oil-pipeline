"""Acquire component for the KGS oil production data pipeline.

Responsibilities:
- Load lease URLs from the lease index file.
- Download per-lease monthly production .txt files from the KGS web portal
  using a two-step HTTP approach with rate-limiting.
- Save raw files to data/raw/.
- Orchestrate parallel acquisition via Dask delayed tasks.
"""

import time
from pathlib import Path

import dask
import pandas as pd  # type: ignore[import-untyped]
import requests
from bs4 import BeautifulSoup

from kgs_pipeline.config import CONFIG
from kgs_pipeline.utils import ensure_dir, is_valid_raw_file, setup_logging

logger = setup_logging("acquire", CONFIG.logs_dir, CONFIG.log_level)

KGS_MONTH_SAVE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"
REQUEST_TIMEOUT = 30


class ScrapingError(Exception):
    """Raised when download fails due to missing page elements."""


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
        try:
            df = pd.read_csv(lease_index_path, dtype=str)
        except Exception as exc:
            raise KeyError(f"Could not parse lease index file: {exc}") from exc

    if "URL" not in df.columns:
        raise KeyError(
            f"'URL' column not found in lease index file. "
            f"Available columns: {list(df.columns)}"
        )

    urls = df["URL"].dropna().str.strip()
    urls = urls[urls != ""].drop_duplicates().tolist()
    logger.info("Loaded %d unique lease URLs from %s", len(urls), lease_index_path)
    return urls


def download_lease_file(
    lease_url: str,
    output_dir: Path,
) -> Path | None:
    """Download monthly production data for a single lease.

    Uses a two-step HTTP approach:
    1. GET MonthSave page for the lease to retrieve the download link.
    2. GET the .txt file directly.

    Args:
        lease_url: URL of the lease main page (contains f_lc= parameter).
        output_dir: Directory to save the downloaded file.

    Returns:
        Path to the saved file on success, or None on failure.
    """
    try:
        lease_kid = lease_url.split("f_lc=")[-1] if "f_lc=" in lease_url else None
        if not lease_kid:
            raise ScrapingError(f"Could not extract lease ID from URL: {lease_url}")

        # Idempotency: skip if already downloaded and valid
        # Note: actual filename uses lp<internal_id>.txt, not lp<lease_kid>.txt
        # so we check output_dir for any matching file after download

        # Step 1 — request MonthSave intermediate page
        month_save_url = f"{KGS_MONTH_SAVE_URL}?f_lc={lease_kid}"
        r1 = requests.get(month_save_url, timeout=REQUEST_TIMEOUT)
        r1.raise_for_status()

        soup = BeautifulSoup(r1.text, "html.parser")
        download_href = None
        for a in soup.find_all("a", href=True):
            if "anon_blobber.download" in a["href"]:
                download_href = a["href"]
                break

        if not download_href:
            raise ScrapingError(
                f"No download link found on MonthSave page for lease {lease_kid}"
            )

        # Extract filename from href
        filename = download_href.split("p_file_name=")[-1]
        output_path = output_dir / filename

        # Idempotency: skip if already downloaded
        if is_valid_raw_file(output_path):
            logger.debug("Skipping already-downloaded file: %s", output_path)
            return output_path

        # Step 2 — download the file
        r2 = requests.get(download_href, timeout=REQUEST_TIMEOUT)
        r2.raise_for_status()

        output_path.write_bytes(r2.content)
        logger.info("Downloaded: %s", output_path)
        return output_path

    except Exception as exc:
        logger.error("Failed to download lease URL %s: %s", lease_url, exc)
        return None


def _run_single_lease(lease_url: str, output_dir: Path) -> Path | None:
    """Download a single lease file with a small delay for rate limiting.

    Args:
        lease_url: The lease page URL.
        output_dir: Directory for downloaded files.

    Returns:
        Path to the downloaded file, or None on failure.
    """
    time.sleep(0.5)  # simple rate limiting — 2 requests/sec max
    return download_lease_file(lease_url, output_dir)


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
        FileNotFoundError: If lease_index_path does not exist.
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
