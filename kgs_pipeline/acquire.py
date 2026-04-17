"""Acquire stage: download raw KGS lease production files to data/raw/."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import dask
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when a download step fails and cannot be recovered from."""


# ---------------------------------------------------------------------------
# Task A-02: Lease index loader
# ---------------------------------------------------------------------------


def load_lease_index(index_path: str, min_year: int) -> list[str]:
    """Load the KGS lease index, filter to year >= min_year, deduplicate by URL.

    Returns a list of unique lease URLs.
    """
    path = Path(index_path)
    if not path.exists():
        raise FileNotFoundError(f"Lease index not found: {index_path}")

    df = pd.read_csv(index_path, dtype=str)

    if "MONTH-YEAR" not in df.columns:
        raise KeyError(f"'MONTH-YEAR' column absent from {index_path}")
    if "URL" not in df.columns:
        raise KeyError(f"'URL' column absent from {index_path}")

    logger.info("Lease index raw rows: %d", len(df))

    def _year_valid(my_val: str) -> bool:
        parts = str(my_val).split("-")
        year_str = parts[-1]
        return year_str.isdigit() and int(year_str) >= min_year

    mask = df["MONTH-YEAR"].apply(_year_valid)
    filtered = df[mask]
    logger.info("Rows after year filter (>= %d): %d", min_year, len(filtered))

    unique_urls = filtered["URL"].dropna().drop_duplicates().tolist()
    logger.info("Unique lease URLs after deduplication: %d", len(unique_urls))
    return unique_urls


# ---------------------------------------------------------------------------
# Task A-03: Lease ID extractor
# ---------------------------------------------------------------------------


def extract_lease_id(url: str) -> str:
    """Parse a KGS lease URL and return the f_lc query parameter value."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    values = params.get("f_lc", [])
    if not values or not values[0]:
        raise ValueError(f"'f_lc' parameter absent or empty in URL: {url}")
    return values[0]


# ---------------------------------------------------------------------------
# Task A-04: MonthSave page parser
# ---------------------------------------------------------------------------


def parse_download_link(html_content: str) -> str:
    """Parse the MonthSave HTML response and return the download URL.

    Raises DownloadError if no valid download link is found.
    """
    if not html_content or not html_content.strip():
        raise DownloadError("Empty or blank HTML content — cannot parse download link")

    soup = BeautifulSoup(html_content, "html.parser")
    anchors = [
        tag for tag in soup.find_all("a", href=True) if "anon_blobber.download" in tag["href"]
    ]
    if not anchors:
        raise DownloadError("No anchor tag with 'anon_blobber.download' found in HTML response")
    return str(anchors[0]["href"])


# ---------------------------------------------------------------------------
# Task A-05: Single-file downloader
# ---------------------------------------------------------------------------


def download_lease_file(
    lease_url: str,
    raw_dir: str,
    month_save_base: str,
    sleep_seconds: float,
) -> Path | None:
    """Execute the three-step download workflow for one lease URL.

    Returns the Path to the saved file, or None on skip/failure.
    """
    try:
        lease_id = extract_lease_id(lease_url)
    except ValueError as exc:
        logger.warning("Cannot extract lease ID from %s: %s", lease_url, exc)
        return None

    # Idempotency check: check if output file already exists before any HTTP calls
    # Use standard naming pattern lp{lease_id}.txt
    p_file_name = f"lp{lease_id}.txt"
    output_path = Path(raw_dir) / p_file_name
    if output_path.exists() and output_path.stat().st_size > 0:
        logger.info("Skipping %s — already downloaded", p_file_name)
        return output_path

    month_save_url = f"{month_save_base}?f_lc={lease_id}"

    try:
        ms_response = requests.get(month_save_url, timeout=30)
        ms_response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("MonthSave GET failed for %s: %s", lease_url, exc)
        return None

    try:
        download_url = parse_download_link(ms_response.text)
    except DownloadError as exc:
        logger.warning("parse_download_link failed for %s: %s", lease_url, exc)
        return None

    # Extract p_file_name from the download URL
    parsed_dl = urlparse(download_url)
    dl_params = parse_qs(parsed_dl.query)
    file_name_list = dl_params.get("p_file_name", [])
    if not file_name_list or not file_name_list[0]:
        logger.warning("No p_file_name in download URL %s", download_url)
        return None

    p_file_name = file_name_list[0]
    output_path = Path(raw_dir) / p_file_name

    try:
        dl_response = requests.get(download_url, timeout=60, stream=True)
        dl_response.raise_for_status()
        output_path.write_bytes(dl_response.content)
    except requests.RequestException as exc:
        logger.warning("File download failed for %s: %s", lease_url, exc)
        return None

    time.sleep(sleep_seconds)

    if output_path.stat().st_size == 0:
        output_path.unlink()
        logger.warning("Downloaded file %s is 0 bytes — deleted", p_file_name)
        return None

    return output_path


# ---------------------------------------------------------------------------
# Task A-06: Parallel acquire runner
# ---------------------------------------------------------------------------


def run_acquire(config: dict) -> list[Path]:
    """Orchestrate the full acquire stage using Dask threaded scheduler.

    Returns a list of successfully downloaded file paths.
    """
    lease_urls = load_lease_index(config["lease_index"], config["min_year"])
    logger.info("Found %d unique lease URLs to process", len(lease_urls))

    raw_dir = config["raw_dir"]
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    month_save_url: str = config["month_save_url"]
    sleep_seconds: float = float(config["sleep_seconds"])
    max_workers: int = int(config["max_workers"])

    tasks = [
        dask.delayed(download_lease_file)(url, raw_dir, month_save_url, sleep_seconds)
        for url in lease_urls
    ]

    results = dask.compute(*tasks, scheduler="threads", num_workers=max_workers)

    successful: list[Path] = [r for r in results if r is not None]
    logger.info(
        "Downloaded %d of %d lease files successfully",
        len(successful),
        len(lease_urls),
    )
    return successful
