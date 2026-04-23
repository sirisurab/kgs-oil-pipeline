"""Acquire stage: load lease index, extract IDs, fetch download links, download files."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_MONTHSAVE_BASE = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds


# ---------------------------------------------------------------------------
# Task A-01: Load and filter the lease index
# ---------------------------------------------------------------------------


def load_lease_index(index_path: str | Path) -> pd.DataFrame:
    """Read the lease index, filter to years >= 2024, and deduplicate by URL.

    Args:
        index_path: Path to oil_leases_2020_present.txt.

    Returns:
        DataFrame filtered to rows where year >= 2024, deduplicated by URL.

    Raises:
        FileNotFoundError: If index_path does not exist.
        ValueError: If URL or MONTH-YEAR columns are absent.
    """
    path = Path(index_path)
    if not path.exists():
        raise FileNotFoundError(f"Lease index not found: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    if "URL" not in df.columns:
        raise ValueError(f"'URL' column is absent from {path}")
    if "MONTH-YEAR" not in df.columns:
        raise ValueError(f"'MONTH-YEAR' column is absent from {path}")

    def _parse_year(my: str) -> int | None:
        parts = my.split("-")
        year_str = parts[-1]
        if not year_str.isdigit():
            return None
        return int(year_str)

    years = df["MONTH-YEAR"].map(_parse_year)
    mask = years >= 2024
    df = df[mask.fillna(False)].copy()

    df = df.drop_duplicates(subset=["URL"])
    logger.debug("Lease index loaded: %d unique URLs after filtering to year >= 2024", len(df))
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Task A-02: Extract lease ID from URL
# ---------------------------------------------------------------------------


def extract_lease_id(url: str) -> str:
    """Extract the f_lc query parameter (lease ID) from a KGS lease URL.

    Args:
        url: KGS MainLease URL containing f_lc parameter.

    Returns:
        The lease ID string.

    Raises:
        ValueError: If the f_lc parameter is absent.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if "f_lc" not in params or not params["f_lc"]:
        raise ValueError(f"'f_lc' parameter is absent from URL: {url}")
    return params["f_lc"][0]


# ---------------------------------------------------------------------------
# Task A-03: Fetch the MonthSave download link
# ---------------------------------------------------------------------------


def fetch_download_link(lease_id: str, session: requests.Session) -> str:
    """Fetch the CSV download link from the MonthSave page for a lease.

    Args:
        lease_id: KGS lease ID (value of f_lc parameter).
        session: requests.Session to use for the HTTP call.

    Returns:
        Full URL of the CSV download link.

    Raises:
        requests.HTTPError: If the HTTP response is non-2xx.
        ValueError: If no download anchor tag is found on the page.
    """
    url = f"{_MONTHSAVE_BASE}?f_lc={lease_id}"
    response = session.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    anchor = soup.find("a", href=lambda h: h and "anon_blobber.download" in h)
    if anchor is None:
        raise ValueError(f"No download link found on MonthSave page for lease_id={lease_id}")
    if not isinstance(anchor, Tag):
        raise ValueError(f"Download link is not a Tag element for lease_id={lease_id}")
    href_val = anchor["href"]
    href: str = href_val[0] if isinstance(href_val, list) else href_val
    return href


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def _download_with_retry(
    url: str,
    dest_path: Path,
    session: requests.Session,
) -> Path | None:
    """Download url to dest_path with exponential backoff retries.

    Returns dest_path on success, None if all retries fail.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            with session.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with dest_path.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        fh.write(chunk)
            return dest_path
        except requests.RequestException as exc:
            wait = _BACKOFF_BASE * (2**attempt)
            logger.warning(
                "Download attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                attempt + 1,
                _MAX_RETRIES,
                url,
                exc,
                wait,
            )
            if attempt < _MAX_RETRIES - 1:
                time.sleep(wait)
    logger.error("All %d download attempts failed for %s", _MAX_RETRIES, url)
    return None


def acquire(config: dict[str, Any]) -> list[Path]:
    """Orchestrate the full acquire stage using Dask threaded scheduler.

    Reads the lease index, filters by year, fetches download links, and
    downloads all files in parallel (threaded, I/O-bound per ADR-001).

    Args:
        config: Pipeline configuration dict (acquire section used).

    Returns:
        List of Paths to successfully downloaded files.
    """
    import dask.bag as db

    acq_cfg = config["acquire"]
    index_path: str = acq_cfg["index_path"]
    raw_dir = Path(acq_cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    lease_df = load_lease_index(index_path)
    urls: list[str] = lease_df["URL"].tolist()
    logger.info("Acquire stage: %d unique lease URLs to process", len(urls))

    def _process_lease(lease_url: str) -> Path | None:
        session = requests.Session()
        try:
            lease_id = extract_lease_id(lease_url)
        except ValueError as exc:
            logger.warning("Skipping URL %s: %s", lease_url, exc)
            return None

        dest_path = raw_dir / f"{lease_id}.txt"
        if dest_path.exists() and dest_path.stat().st_size > 0:
            logger.debug("Cache hit — skipping download for lease %s", lease_id)
            return dest_path

        try:
            download_url = fetch_download_link(lease_id, session)
        except (requests.HTTPError, ValueError) as exc:
            logger.warning("Could not fetch download link for lease %s: %s", lease_id, exc)
            return None

        return _download_with_retry(download_url, dest_path, session)

    # Use Dask bag with threaded scheduler (I/O-bound — ADR-001 / H1)
    bag = db.from_sequence(urls, npartitions=min(len(urls), 8))
    results: list[Path | None] = bag.map(_process_lease).compute(scheduler="threads")

    downloaded = [p for p in results if p is not None]
    logger.info("Acquire stage complete: %d files downloaded/cached", len(downloaded))
    return downloaded
