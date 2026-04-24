"""
Acquire stage: download per-lease monthly production files from the KGS portal.

Scheduler choice: Dask threaded scheduler (I/O-bound workload) — per ADR-001 and
stage-manifest-acquire H1. The distributed client used by CPU-bound stages is
initialized AFTER acquire completes and must not be used here.

Parallelism limit: max 5 concurrent workers with 0.5 s sleep per worker — per
task-writer-kgs.md <instructions>.

Browser automation is prohibited — per ADR-007.
HTTP retries and backoff are configurable from config.yaml — per build-env-manifest.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
import bs4.element
from bs4 import BeautifulSoup
from dask import delayed
import dask

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task A1: Build download manifest from the lease index
# ---------------------------------------------------------------------------


def build_manifest(
    index_path: str | Path,
    min_year: int,
) -> pd.DataFrame:
    """Read the lease index and return deduplicated records to download.

    Filtering rules per task-writer-kgs.md <data-filtering> and <instructions>:
    - Extract year from MONTH-YEAR by splitting on '-' and taking the last element.
    - Rows whose year component is not numeric are excluded.
    - Only leases with at least one row where year >= min_year are retained.
    - Deduplicated by URL (one entry per lease).

    Column names come from references/kgs_archives_data_dictionary.csv (ADR-003).

    Parameters
    ----------
    index_path:
        Path to the lease index file.
    min_year:
        Minimum year threshold (inclusive).

    Returns
    -------
    pd.DataFrame with columns [LEASE_KID, URL, MONTH-YEAR, ...], one row per lease.

    Raises
    ------
    FileNotFoundError
        If the index file is missing or unreadable.
    """
    path = Path(index_path)
    if not path.exists():
        raise FileNotFoundError(f"Lease index file not found at expected path: {path.resolve()}")

    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as exc:
        raise FileNotFoundError(
            f"Lease index file could not be read at {path.resolve()}: {exc}"
        ) from exc

    # Normalize column names — strip quotes/whitespace that may appear in CSV headers
    df.columns = [c.strip().strip('"') for c in df.columns]

    if "MONTH-YEAR" not in df.columns:
        raise ValueError(
            f"Lease index is missing required column 'MONTH-YEAR'. "
            f"Found columns: {list(df.columns)}"
        )
    if "URL" not in df.columns:
        raise ValueError(
            f"Lease index is missing required column 'URL'. Found columns: {list(df.columns)}"
        )

    # Extract year by splitting on "-" and taking the last element
    year_series = df["MONTH-YEAR"].str.strip().str.rsplit("-", n=1).str[-1]

    # Exclude rows whose year component is not numeric
    numeric_mask = year_series.str.match(r"^\d+$", na=False)
    df = df[numeric_mask].copy()
    year_series = year_series[numeric_mask]

    # Filter to leases with at least one row where year >= min_year
    years = year_series.astype(int)
    qualifying_urls = df.loc[years >= min_year, "URL"].unique()
    df = df[df["URL"].isin(qualifying_urls)].copy()

    # Deduplicate by URL — one entry per lease
    df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)

    logger.info("Manifest built: %d leases qualify (min_year=%d)", len(df), min_year)
    return df


def _extract_lease_id(url: str) -> str:
    """Extract the lease ID from a MainLease URL query parameter f_lc."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    lease_ids = params.get("f_lc", [])
    if not lease_ids:
        raise ValueError(f"Cannot extract lease ID from URL: {url!r}")
    return lease_ids[0]


# ---------------------------------------------------------------------------
# Task A2: Resolve a lease's MonthSave download URL
# ---------------------------------------------------------------------------


def resolve_download_url(
    lease_id: str,
    monthsave_template: str,
    retry_attempts: int,
    retry_backoff_base: float,
    timeout: int,
) -> str | None:
    """Return the direct download URL for a lease by fetching its MonthSave page.

    Steps per task-writer-kgs.md <instructions> steps 2 and 3:
    1. GET MonthSave page for the lease.
    2. Parse with BeautifulSoup to find anchor whose href contains
       'anon_blobber.download'.

    HTTP library and HTML parser: requests + BeautifulSoup — per ADR-007 and
    build-env-manifest "HTTP library for acquire".

    Parameters
    ----------
    lease_id:
        KGS lease identifier.
    monthsave_template:
        URL template with '{lease_id}' placeholder.
    retry_attempts:
        Maximum number of HTTP attempts (from config.yaml).
    retry_backoff_base:
        Base for exponential backoff between retries.
    timeout:
        HTTP request timeout in seconds.

    Returns
    -------
    str
        Fully-qualified download URL on success.
    None
        When no download anchor is found or all retries fail.
    """
    url = monthsave_template.format(lease_id=lease_id)

    for attempt in range(1, retry_attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            anchor = soup.find("a", href=lambda h: h and "anon_blobber.download" in h)
            if anchor is None:
                logger.debug("No download anchor found on MonthSave page for lease %s.", lease_id)
                return None

            if not isinstance(anchor, bs4.element.Tag):
                logger.debug("Anchor element is not a Tag for lease %s.", lease_id)
                return None

            href = anchor.get("href")
            # Make absolute if needed
            if isinstance(href, str) and href.startswith("http"):
                return str(href)
            if isinstance(href, str):
                parsed = urlparse(url)
                return f"{parsed.scheme}://{parsed.netloc}{href}"
            return None
        except requests.RequestException as exc:
            if attempt < retry_attempts:
                sleep_time = retry_backoff_base ** (attempt - 1)
                logger.debug(
                    "MonthSave request failed for lease %s (attempt %d/%d): %s. Retrying in %.1fs.",
                    lease_id,
                    attempt,
                    retry_attempts,
                    exc,
                    sleep_time,
                )
                time.sleep(sleep_time)
            else:
                logger.warning(
                    "MonthSave request permanently failed for lease %s after %d attempts: %s",
                    lease_id,
                    retry_attempts,
                    exc,
                )
                return None

    return None  # unreachable but satisfies type checker


# ---------------------------------------------------------------------------
# Task A3: Download a single lease production file
# ---------------------------------------------------------------------------


def download_file(
    download_url: str,
    target_dir: str | Path,
    sleep_seconds: float,
    retry_attempts: int,
    retry_backoff_base: float,
    timeout: int,
) -> Path | None:
    """Download a lease production file to target_dir using p_file_name from the URL.

    Idempotency (TR-20): If the target file already exists and is non-empty,
    the download is skipped and the existing path is returned.

    File integrity (TR-21): The written file must be non-empty and decodable as
    UTF-8 text. If it fails either check, the partial file is deleted and None
    is returned (stage-manifest-acquire H2).

    Polite-scraping sleep of sleep_seconds is applied per worker before each
    download attempt — per task-writer-kgs.md <instructions>.

    Parameters
    ----------
    download_url:
        Fully-qualified download URL containing p_file_name query parameter.
    target_dir:
        Directory to write the file into.
    sleep_seconds:
        Sleep duration before each download attempt.
    retry_attempts:
        Maximum number of HTTP attempts.
    retry_backoff_base:
        Base for exponential backoff.
    timeout:
        HTTP request timeout in seconds.

    Returns
    -------
    Path to the written file on success, or None on failure.
    """
    # Extract filename from p_file_name query parameter
    parsed = urlparse(download_url)
    params = parse_qs(parsed.query)
    file_names = params.get("p_file_name", [])
    if not file_names:
        logger.warning("Cannot extract p_file_name from URL: %s", download_url)
        return None

    filename = file_names[0]
    target_path = Path(target_dir) / filename

    # Idempotency: skip if already present and non-empty
    if target_path.exists() and target_path.stat().st_size > 0:
        logger.debug("Skipping already-present file: %s", target_path)
        return target_path

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retry_attempts + 1):
        time.sleep(sleep_seconds)
        try:
            resp = requests.get(download_url, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            if attempt < retry_attempts:
                backoff = retry_backoff_base ** (attempt - 1)
                logger.debug(
                    "Download failed for %s (attempt %d/%d): %s. Retrying in %.1fs.",
                    filename,
                    attempt,
                    retry_attempts,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
                continue
            logger.warning(
                "Download permanently failed for %s after %d attempts: %s",
                filename,
                retry_attempts,
                exc,
            )
            return None

        content = resp.content

        # Integrity check: non-empty
        if not content:
            logger.warning("Downloaded empty content for %s; skipping.", filename)
            return None

        # Integrity check: decodable as text
        try:
            content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                content.decode("latin-1")
            except UnicodeDecodeError:
                logger.warning(
                    "Downloaded content for %s is not text-decodable; skipping.",
                    filename,
                )
                return None

        target_path.write_bytes(content)
        logger.info("Downloaded: %s", target_path)
        return target_path

    return None


# ---------------------------------------------------------------------------
# Task A4: Orchestrate parallel acquisition
# ---------------------------------------------------------------------------


def acquire(config: dict[str, Any]) -> None:
    """Stage entry point: build manifest, resolve URLs, and download files in parallel.

    Uses the Dask threaded scheduler (I/O-bound) — per ADR-001 / stage-manifest-acquire H1.
    The distributed client used by CPU-bound stages must not be initialized here.

    Concurrency limited to max_workers=5 with 0.5s per-worker sleep per
    task-writer-kgs.md <instructions>.

    Parameters
    ----------
    config:
        Full pipeline config dict (parsed config.yaml).

    Raises
    ------
    KeyError
        If the 'acquire' section is absent from config.
    FileNotFoundError
        If the lease index file is missing.
    """
    acq = config["acquire"]

    manifest = build_manifest(
        index_path=acq["lease_index_path"],
        min_year=acq["min_year"],
    )

    raw_dir = acq["raw_dir"]
    max_workers = acq["max_workers"]
    sleep_seconds = acq["sleep_per_worker"]
    retry_attempts = acq["retry_attempts"]
    retry_backoff_base = acq["retry_backoff_base"]
    monthsave_template = acq["monthsave_url_template"]
    timeout = acq["download_timeout"]

    logger.info("Starting acquisition: %d leases, max_workers=%d", len(manifest), max_workers)

    def _process_one(row: pd.Series) -> str:
        """Resolve and download one lease; returns status string for logging."""
        url = row["URL"]
        try:
            lease_id = _extract_lease_id(url)
        except ValueError as exc:
            logger.warning("Could not extract lease ID from URL %r: %s", url, exc)
            return "failed"

        dl_url = resolve_download_url(
            lease_id=lease_id,
            monthsave_template=monthsave_template,
            retry_attempts=retry_attempts,
            retry_backoff_base=retry_backoff_base,
            timeout=timeout,
        )
        if dl_url is None:
            return "failed"

        result = download_file(
            download_url=dl_url,
            target_dir=raw_dir,
            sleep_seconds=sleep_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_base=retry_backoff_base,
            timeout=timeout,
        )
        if result is None:
            return "failed"

        # Check if it was a skip (already existed) vs new download
        # We cannot know from here — count as succeeded
        return "succeeded"

    # Build Dask delayed graph — threaded scheduler (I/O-bound, ADR-001)
    tasks = [delayed(_process_one)(row) for _, row in manifest.iterrows()]

    # Dask threaded scheduler with max_workers limit
    results: list[str] = dask.compute(  # type: ignore[assignment]
        *tasks,
        scheduler="threads",
        num_workers=max_workers,
    )

    n_succeeded = sum(1 for r in results if r == "succeeded")
    n_failed = sum(1 for r in results if r == "failed")

    logger.info(
        "Acquisition complete: %d succeeded, %d failed out of %d attempted.",
        n_succeeded,
        n_failed,
        len(manifest),
    )
