"""Acquire stage: download raw lease production files from KGS."""

import logging
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import dask
import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


def load_lease_index(config: dict) -> pd.DataFrame:
    """Load and filter the lease index to target_year and later, deduped by URL."""
    path = config["acquire"]["lease_index_path"]
    target_year: int = config["acquire"]["target_year"]

    df = pd.read_csv(path, dtype=str)

    # Extract year from "M-YYYY" format using vectorized str accessor
    year_series = df["MONTH-YEAR"].str.split("-").str[-1]

    # Keep only rows with numeric year strings
    numeric_mask = year_series.str.match(r"^\d+$")
    df = df[numeric_mask].copy()
    year_series = df["MONTH-YEAR"].str.split("-").str[-1]

    # Apply year filter
    df = df[year_series.astype(int) >= target_year].copy()

    # Deduplicate by URL column
    df = df.drop_duplicates(subset=["URL"], keep="first")

    return df.reset_index(drop=True)


def extract_lease_id(url: str) -> str:
    """Extract the f_lc query parameter value from a KGS lease URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if "f_lc" not in params:
        raise ValueError(f"URL does not contain 'f_lc' parameter: {url}")
    return params["f_lc"][0]


def resolve_download_url(lease_id: str, config: dict) -> str | None:
    """Fetch the MonthSave page for a lease and return the download link, or None."""
    month_save_url = f"https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"
    response = requests.get(month_save_url)
    soup = BeautifulSoup(response.text, "html.parser")

    anchor = soup.find("a", href=lambda h: h and "anon_blobber.download" in h)
    if anchor is None:
        logger.warning("No download link found for lease_id=%s at %s", lease_id, month_save_url)
        return None

    if not isinstance(anchor, Tag):
        logger.warning("Anchor is not a Tag for lease_id=%s", lease_id)
        return None

    href = anchor.get("href")
    if not isinstance(href, str):
        logger.warning("Anchor href is not a string for lease_id=%s", lease_id)
        return None
    return href


def download_lease_file(download_url: str, output_dir: str) -> Path | None:
    """Download a lease file to output_dir. Skip if already present and non-empty."""
    parsed = urlparse(download_url)
    params = parse_qs(parsed.query)
    file_name = params.get("p_file_name", [None])[0]
    if file_name is None:
        logger.warning("No p_file_name in download URL: %s", download_url)
        time.sleep(0.5)
        return None

    out_path = Path(output_dir) / file_name

    # Idempotency: skip if file already exists and is non-empty
    if out_path.exists() and out_path.stat().st_size > 0:
        time.sleep(0.5)
        return out_path

    response = requests.get(download_url)
    content = response.content

    if len(content) == 0:
        if out_path.exists():
            out_path.unlink()
        time.sleep(0.5)
        return None

    out_path.write_bytes(content)
    time.sleep(0.5)
    return out_path


def acquire(config: dict) -> list[Path | None]:
    """Orchestrate parallel acquisition of all lease files via Dask threaded scheduler."""
    output_dir = config["acquire"]["raw_dir"]
    max_workers: int = min(config["acquire"]["max_workers"], 5)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    lease_df = load_lease_index(config)
    logger.info("Loaded %d unique lease URLs to process", len(lease_df))

    delayed_tasks = []
    for url in lease_df["URL"]:
        task = dask.delayed(_acquire_one)(url, output_dir, config)
        delayed_tasks.append(task)

    results: list[Path | None] = list(
        dask.compute(*delayed_tasks, scheduler="threads", num_workers=max_workers)
    )
    logger.info("Acquire complete: %d results", len(results))
    return results


def _acquire_one(url: str, output_dir: str, config: dict) -> Path | None:
    """Download one lease file end-to-end."""
    try:
        lease_id = extract_lease_id(url)
        download_url = resolve_download_url(lease_id, config)
        if download_url is None:
            return None
        return download_lease_file(download_url, output_dir)
    except Exception as exc:
        logger.error("Failed to acquire %s: %s", url, exc)
        return None
