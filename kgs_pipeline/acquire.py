"""Acquire component: download KGS lease production data files."""

from __future__ import annotations

import argparse
import sys
import time
import urllib.parse
from pathlib import Path

import dask
import pandas as pd  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.util.retry import Retry

from kgs_pipeline.config import config as _cfg
from kgs_pipeline.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Task 03: Lease index loader
# ---------------------------------------------------------------------------


def load_lease_index(index_path: str, min_year: int = 2024) -> pd.DataFrame:
    """Load and filter the KGS lease index file.

    Returns one row per unique lease URL active from *min_year* onward.

    Raises:
        FileNotFoundError: if *index_path* does not exist.
    """
    path = Path(index_path)
    if not path.exists():
        raise FileNotFoundError(f"Lease index not found: {index_path}")

    df: pd.DataFrame | None = None
    for sep in (",", "\t", "|"):
        try:
            candidate = pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False)
            if len(candidate.columns) > 1:
                df = candidate
                break
        except UnicodeDecodeError:
            candidate = pd.read_csv(path, sep=sep, encoding="latin-1", low_memory=False)
            if len(candidate.columns) > 1:
                df = candidate
                break
        except Exception:
            continue

    if df is None:
        raise ValueError(f"Could not parse lease index file: {index_path}")

    df.columns = [c.strip() for c in df.columns]
    logger.info("Lease index loaded", extra={"rows_raw": len(df), "path": index_path})

    # Filter by MONTH-YEAR year component >= min_year
    month_year_col: str | None = "MONTH-YEAR"
    if month_year_col not in df.columns:
        # Try normalised name
        month_year_col = next(
            (c for c in df.columns if "MONTH" in c.upper() and "YEAR" in c.upper()),
            None,
        )

    if month_year_col:

        def _extract_year(val: object) -> int | None:
            s = str(val).strip()
            parts = s.split("-")
            last = parts[-1]
            try:
                return int(last)
            except ValueError:
                return None

        years = df[month_year_col].apply(_extract_year)
        df = df[years.notna() & (years >= min_year)].copy()

    # Identify URL column
    url_col = next((c for c in df.columns if c.upper() == "URL"), None)
    if url_col is None:
        raise KeyError("No URL column found in lease index")

    df = df[df[url_col].notna() & (df[url_col].astype(str).str.strip() != "")]
    df = df.drop_duplicates(subset=[url_col], keep="first")

    logger.info(
        "Lease index filtered",
        extra={"rows_filtered": len(df), "min_year": min_year},
    )
    return df


# ---------------------------------------------------------------------------
# Task 04: Lease ID extractor
# ---------------------------------------------------------------------------


def extract_lease_id(url: str) -> str:
    """Extract the ``f_lc`` query parameter value from a KGS lease URL.

    Raises:
        ValueError: if ``f_lc`` is absent or empty.
    """
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    values = params.get("f_lc", [])
    if not values or not values[0].strip():
        raise ValueError(f"No f_lc parameter found in URL: {url}")
    return values[0].strip()


# ---------------------------------------------------------------------------
# Task 05: MonthSave page fetcher and download-link parser
# ---------------------------------------------------------------------------


def fetch_month_save_page(
    lease_id: str,
    session: requests.Session,
    max_retries: int = 3,
    backoff_base: float = 1.0,
) -> str:
    """Fetch the MonthSave HTML page for *lease_id*.

    Retries up to *max_retries* times with exponential backoff.

    Raises:
        RuntimeError: after all retries are exhausted.
    """
    url = _cfg.MONTH_SAVE_URL_TEMPLATE.format(lease_id=lease_id)
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            logger.info(
                "Fetched MonthSave page",
                extra={"lease_id": lease_id, "status_code": resp.status_code},
            )
            return resp.text
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            sleep_time = backoff_base * (2**attempt)
            logger.warning(
                "MonthSave fetch failed, retrying",
                extra={"lease_id": lease_id, "attempt": attempt, "sleep": sleep_time},
            )
            time.sleep(sleep_time)

    raise RuntimeError(
        f"Failed to fetch MonthSave page for lease {lease_id} after {max_retries} retries"
    ) from last_exc


def parse_download_link(html: str, base_url: str = "https://chasm.kgs.ku.edu") -> str:
    """Parse the data-file download URL from a MonthSave HTML page.

    Raises:
        ValueError: if no ``anon_blobber.download`` link is found.
    """
    soup = BeautifulSoup(html, "html.parser")
    anchor = soup.find("a", href=lambda h: h and "anon_blobber.download" in h)
    if anchor is None:
        raise ValueError("No anon_blobber.download link found in HTML")
    href: str = anchor["href"]  # type: ignore[assignment]
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # Relative URL — prepend base_url
    return base_url.rstrip("/") + "/" + href.lstrip("/")


# ---------------------------------------------------------------------------
# Task 06: Data file downloader
# ---------------------------------------------------------------------------


def download_file(
    download_url: str,
    output_dir: str,
    session: requests.Session,
    sleep_seconds: float = 0.5,
) -> Path | None:
    """Download a single KGS data file to *output_dir*.

    Returns the output ``Path`` on success, or ``None`` on empty/failed download.
    Raises ``RuntimeError`` on HTTP status >= 400.
    """
    parsed = urllib.parse.urlparse(download_url)
    params = urllib.parse.parse_qs(parsed.query)
    filenames = params.get("p_file_name", [])
    if filenames and filenames[0].strip():
        filename = filenames[0].strip()
    else:
        filename = parsed.path.split("/")[-1] or "unknown.txt"

    output_path = Path(output_dir) / filename

    if output_path.exists() and output_path.stat().st_size > 0:
        logger.info(
            "Skipped (already downloaded)",
            extra={"file": str(output_path), "size": output_path.stat().st_size},
        )
        return output_path

    time.sleep(sleep_seconds)

    try:
        resp = session.get(download_url, timeout=60)
    except requests.exceptions.RequestException as exc:
        logger.warning("Download request failed", extra={"url": download_url, "error": str(exc)})
        return None

    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} downloading {download_url}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(resp.content)

    if output_path.stat().st_size == 0:
        logger.warning("Downloaded file is empty, removing", extra={"file": str(output_path)})
        output_path.unlink()
        return None

    logger.info(
        "Downloaded",
        extra={"file": str(output_path), "size": output_path.stat().st_size},
    )
    return output_path


# ---------------------------------------------------------------------------
# Task 07: Parallel download orchestrator
# ---------------------------------------------------------------------------


def _download_one_lease(
    row: dict,
    output_dir: str,
    sleep_seconds: float,
) -> Path | None:
    """Download a single lease — used as a Dask delayed task."""
    session = _make_session()
    try:
        lease_id = extract_lease_id(row["URL"])
        html = fetch_month_save_page(lease_id, session)
        dl_url = parse_download_link(html)
        return download_file(dl_url, output_dir, session, sleep_seconds)
    except Exception as exc:
        logger.warning(
            "Lease download failed",
            extra={"url": row.get("URL"), "error": str(exc)},
        )
        return None
    finally:
        session.close()


def _make_session() -> requests.Session:
    """Create a requests.Session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def run_acquire(
    index_path: str,
    output_dir: str,
    min_year: int = 2024,
    max_workers: int = 5,
) -> list[Path]:
    """Orchestrate full acquisition: load index → parallel download all leases."""
    df = load_lease_index(index_path, min_year)
    records = df.to_dict(orient="records")

    sleep_seconds = _cfg.SLEEP_SECONDS

    delayed_tasks = [
        dask.delayed(_download_one_lease)(row, output_dir, sleep_seconds) for row in records
    ]

    results = dask.compute(
        *delayed_tasks,
        scheduler="threads",
        num_workers=max_workers,
    )

    downloaded: list[Path] = [r for r in results if r is not None]
    failed = sum(1 for r in results if r is None)

    logger.info(
        "Acquire complete",
        extra={
            "attempted": len(records),
            "downloaded": len(downloaded),
            "failed": failed,
        },
    )
    return downloaded


# ---------------------------------------------------------------------------
# Task 09: CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for the acquire stage."""
    parser = argparse.ArgumentParser(description="KGS acquire: download lease data files")
    parser.add_argument(
        "--index-path",
        default=_cfg.LEASE_INDEX_PATH,
        help="Path to lease index file",
    )
    parser.add_argument(
        "--output-dir",
        default=_cfg.RAW_DATA_DIR,
        help="Directory to write downloaded files",
    )
    parser.add_argument("--min-year", type=int, default=_cfg.MIN_YEAR)
    parser.add_argument("--workers", type=int, default=_cfg.MAX_WORKERS)
    args = parser.parse_args()

    try:
        paths = run_acquire(
            index_path=args.index_path,
            output_dir=args.output_dir,
            min_year=args.min_year,
            max_workers=args.workers,
        )
        print(f"Acquired {len(paths)} files.")
        sys.exit(0)
    except Exception as exc:
        logger.error("Acquire failed", extra={"error": str(exc)})
        sys.exit(1)


if __name__ == "__main__":
    main()
