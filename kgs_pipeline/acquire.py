"""Acquire stage — download raw KGS lease production files."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import dask
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ACQ-01: Lease index loader
# ---------------------------------------------------------------------------

def load_lease_index(index_path: str) -> pd.DataFrame:
    """Load and filter the KGS lease index CSV.

    Filters to rows with year >= 2024 and deduplicates on the URL column.

    Args:
        index_path: Path to the lease index file.

    Returns:
        Filtered, deduplicated DataFrame with the URL column intact.

    Raises:
        FileNotFoundError: If ``index_path`` does not exist.
        ValueError: If required columns are absent.
    """
    p = Path(index_path)
    if not p.exists():
        raise FileNotFoundError(f"Lease index not found: {index_path}")

    df = pd.read_csv(index_path, dtype=str)

    required = {"URL", "MONTH-YEAR"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Lease index missing columns: {missing}")

    def _parse_year(val: str) -> int | None:
        try:
            return int(str(val).split("-")[-1].strip())
        except (ValueError, AttributeError):
            return None

    years = df["MONTH-YEAR"].map(_parse_year)
    df = df[years.notna() & (years >= 2024)].copy()
    df = df.drop_duplicates(subset=["URL"])
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# ACQ-02: Lease ID extractor
# ---------------------------------------------------------------------------

def extract_lease_id(url: str) -> str:
    """Extract the ``f_lc`` lease ID from a KGS MainLease URL.

    Args:
        url: A KGS MainLease URL.

    Returns:
        The lease ID string.

    Raises:
        ValueError: If ``f_lc`` is not present in the URL.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if "f_lc" not in params:
        raise ValueError(f"No f_lc parameter in URL: {url}")
    return params["f_lc"][0]


# ---------------------------------------------------------------------------
# ACQ-03: MonthSave page parser
# ---------------------------------------------------------------------------

def parse_download_link(html: str, base_url: str) -> str:
    """Find the ``anon_blobber.download`` link in a KGS MonthSave page.

    Args:
        html: Raw HTML content of the MonthSave page.
        base_url: Base URL used to resolve relative hrefs.

    Returns:
        Absolute download URL string.

    Raises:
        ValueError: If no matching anchor is found.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        if "anon_blobber.download" in href:
            if href.startswith("http"):
                return href
            return urljoin(base_url, href)
    raise ValueError(f"No anon_blobber.download link found in HTML (base_url={base_url})")


# ---------------------------------------------------------------------------
# ACQ-04: Single-lease downloader
# ---------------------------------------------------------------------------

_KGS_BASE = "https://chasm.kgs.ku.edu"
_MONTH_SAVE_TMPL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"


def download_lease(
    lease_url: str,
    output_dir: Path,
    session: requests.Session,
) -> Path | None:
    """Execute the two-step KGS download for a single lease.

    Args:
        lease_url: The MainLease URL for this lease.
        output_dir: Directory where downloaded files are saved.
        session: A ``requests.Session`` for connection reuse.

    Returns:
        ``Path`` to the saved file, or ``None`` on any failure.
    """
    try:
        lease_id = extract_lease_id(lease_url)
    except ValueError as exc:
        logger.warning("Cannot extract lease ID from %s: %s", lease_url, exc)
        return None

    month_save_url = _MONTH_SAVE_TMPL.format(lease_id=lease_id)

    try:
        resp = session.get(month_save_url, timeout=30)
        if resp.status_code != 200:
            logger.warning("MonthSave returned %s for lease %s", resp.status_code, lease_id)
            return None
        download_url = parse_download_link(resp.text, _KGS_BASE)
    except requests.exceptions.RequestException as exc:
        logger.warning("Network error fetching MonthSave for lease %s: %s", lease_id, exc)
        return None
    except ValueError as exc:
        logger.warning("No download link for lease %s: %s", lease_id, exc)
        return None

    # Extract filename from p_file_name query param
    dl_params = parse_qs(urlparse(download_url).query)
    filename = dl_params.get("p_file_name", [f"lp{lease_id}.txt"])[0]
    dest = output_dir / filename

    # Idempotency — skip if already downloaded
    if dest.exists():
        return dest

    try:
        data_resp = session.get(download_url, timeout=60)
        if data_resp.status_code != 200:
            logger.warning("Data download returned %s for lease %s", data_resp.status_code, lease_id)
            return None
        if not data_resp.content:
            logger.warning("Empty content for lease %s", lease_id)
            return None
        data_resp.content.decode("utf-8")  # validate decodable
    except requests.exceptions.RequestException as exc:
        logger.warning("Network error downloading data for lease %s: %s", lease_id, exc)
        return None
    except UnicodeDecodeError:
        logger.warning("Non-UTF-8 content for lease %s", lease_id)
        return None

    dest.write_bytes(data_resp.content)
    time.sleep(0.5)
    return dest


# ---------------------------------------------------------------------------
# ACQ-05: Retry decorator
# ---------------------------------------------------------------------------

def with_retry(max_attempts: int, backoff_base: float):
    """Decorator factory with exponential backoff retry logic.

    Args:
        max_attempts: Maximum number of call attempts.
        backoff_base: Base seconds for exponential backoff.

    Returns:
        Decorator that wraps callables with retry logic.
    """
    def decorator(fn):  # type: ignore[no-untyped-def]
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
            last_exc: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    wait = backoff_base * (2 ** attempt)
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                        attempt + 1,
                        max_attempts,
                        fn.__name__,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# ACQ-06: Parallel acquire orchestrator
# ---------------------------------------------------------------------------

def run_acquire(config: dict) -> list[Path]:
    """Orchestrate parallel download of all lease production files.

    Uses the Dask threaded scheduler (I/O-bound — ADR-001).

    Args:
        config: Pipeline configuration dict.

    Returns:
        List of ``Path`` objects for successfully downloaded files.
    """
    acq_cfg = config["acquire"]
    index_df = load_lease_index(acq_cfg["lease_index"])
    urls: list[str] = index_df["URL"].tolist()

    output_dir = Path(acq_cfg["raw_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    max_attempts: int = acq_cfg.get("max_attempts", 3)
    backoff_base: float = float(acq_cfg.get("backoff_base", 1.0))

    session = requests.Session()

    @with_retry(max_attempts=max_attempts, backoff_base=backoff_base)
    def _download(url: str) -> Path | None:
        return download_lease(url, output_dir, session)

    delayed_tasks = [dask.delayed(_download)(url) for url in urls]
    results = dask.compute(*delayed_tasks, scheduler="threads", num_workers=5)

    paths = [r for r in results if r is not None]
    failed = len(results) - len(paths)
    logger.info(
        "Acquire complete: %d attempted, %d succeeded, %d failed",
        len(results),
        len(paths),
        failed,
    )
    return paths


# ---------------------------------------------------------------------------
# ACQ-07: File integrity validator
# ---------------------------------------------------------------------------

def validate_raw_files(raw_dir: Path) -> list[str]:
    """Scan raw files and return error messages for failed integrity checks.

    Args:
        raw_dir: Directory containing raw downloaded files.

    Returns:
        List of error strings (empty if all files pass).

    Raises:
        FileNotFoundError: If ``raw_dir`` does not exist.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"raw_dir does not exist: {raw_dir}")

    errors: list[str] = []
    for filepath in raw_dir.iterdir():
        if not filepath.is_file():
            continue
        try:
            size = filepath.stat().st_size
            if size == 0:
                errors.append(f"{filepath.name}: zero-byte file")
                continue
            content = filepath.read_bytes()
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                errors.append(f"{filepath.name}: non-UTF-8 content")
                continue
            non_empty_lines = [ln for ln in text.splitlines() if ln.strip()]
            if len(non_empty_lines) < 2:
                errors.append(f"{filepath.name}: fewer than 2 non-empty lines (header-only or empty)")
        except OSError as exc:
            errors.append(f"{filepath.name}: OS error — {exc}")

    return errors


# ---------------------------------------------------------------------------
# ACQ-08: Stage entry point
# ---------------------------------------------------------------------------

def acquire(config: dict) -> None:
    """Top-level entry point for the acquire stage.

    Args:
        config: Pipeline configuration dict.
    """
    paths = run_acquire(config)
    raw_dir = Path(config["acquire"]["raw_dir"])
    errors = validate_raw_files(raw_dir)
    for err in errors:
        logger.warning("File integrity error: %s", err)
    logger.info("Acquire stage complete: %d files downloaded", len(paths))
