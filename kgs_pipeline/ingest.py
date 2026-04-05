"""Ingest component: load raw KGS lease files into Dask DataFrames."""

from __future__ import annotations

import argparse
from pathlib import Path

import dask
import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import config as _cfg
from kgs_pipeline.logging_utils import get_logger

logger = get_logger(__name__)

# Canonical column names after normalisation
EXPECTED_COLUMNS: list[str] = [
    "LEASE_KID",
    "LEASE",
    "DOR_CODE",
    "API_NUMBER",
    "FIELD",
    "PRODUCING_ZONE",
    "OPERATOR",
    "COUNTY",
    "TOWNSHIP",
    "TWN_DIR",
    "RANGE",
    "RANGE_DIR",
    "SECTION",
    "SPOT",
    "LATITUDE",
    "LONGITUDE",
    "MONTH_YEAR",
    "PRODUCT",
    "WELLS",
    "PRODUCTION",
]

# Columns that should be float64
_FLOAT_COLS = {"LATITUDE", "LONGITUDE", "PRODUCTION", "WELLS"}
# All non-float columns are string
_STRING_COLS = set(EXPECTED_COLUMNS) - _FLOAT_COLS

# Meta dtype mapping for Dask
_META_DTYPES: dict[str, object] = {col: pd.StringDtype() for col in _STRING_COLS}
_META_DTYPES.update({col: "float64" for col in _FLOAT_COLS})
_META_DTYPES["source_file"] = pd.StringDtype()


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, replace spaces/hyphens with underscores, uppercase."""
    df.columns = [c.strip().replace(" ", "_").replace("-", "_").upper() for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Task 01: Raw file discovery
# ---------------------------------------------------------------------------


def discover_raw_files(raw_dir: str, pattern: str = "*.txt") -> list[Path]:
    """Scan *raw_dir* for files matching *pattern*; skip hidden files.

    Raises:
        FileNotFoundError: if *raw_dir* does not exist.
    """
    directory = Path(raw_dir)
    if not directory.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    files = sorted(p for p in directory.glob(pattern) if not p.name.startswith("."))
    logger.info("Discovered raw files", extra={"count": len(files), "dir": raw_dir})
    return files


# ---------------------------------------------------------------------------
# Task 02: Single-file reader with schema detection
# ---------------------------------------------------------------------------


def read_raw_file(file_path: Path) -> pd.DataFrame:
    """Read a single KGS raw lease file into a pandas DataFrame.

    Never raises — returns an empty DataFrame with expected columns on failure.
    """
    empty_df = pd.DataFrame({col: pd.array([], dtype=pd.StringDtype()) for col in EXPECTED_COLUMNS})
    empty_df["source_file"] = pd.array([], dtype=pd.StringDtype())
    for col in _FLOAT_COLS:
        empty_df[col] = pd.array([], dtype="float64")

    if not file_path.exists() or file_path.stat().st_size == 0:
        logger.warning("Empty or missing file", extra={"file": str(file_path)})
        return empty_df

    df: pd.DataFrame | None = None
    for enc in ("utf-8", "latin-1"):
        try:
            candidate = pd.read_csv(file_path, encoding=enc, low_memory=False)
            df = candidate
            break
        except UnicodeDecodeError:
            continue
        except pd.errors.ParserError as exc:
            logger.warning(
                "CSV parse error",
                extra={"file": str(file_path), "error": str(exc)},
            )
            return empty_df

    if df is None or df.empty:
        logger.warning("File produced no rows", extra={"file": str(file_path)})
        return empty_df

    df = _normalise_columns(df)
    df["source_file"] = file_path.name

    # Ensure all expected columns are present
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Cast float columns
    for col in _FLOAT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Task 03: Year-range row filter
# ---------------------------------------------------------------------------


def _filter_partition(pdf: pd.DataFrame, min_year: int) -> pd.DataFrame:
    """Filter partition to rows where MONTH_YEAR year component >= min_year."""
    if "MONTH_YEAR" not in pdf.columns or pdf.empty:
        return pdf

    mask = pdf["MONTH_YEAR"].notna()
    pdf = pdf[mask].copy()
    if pdf.empty:
        return pdf

    def _year(val: object) -> int | None:
        s = str(val).strip()
        parts = s.split("-")
        try:
            return int(parts[-1])
        except (ValueError, IndexError):
            return None

    years = pdf["MONTH_YEAR"].apply(_year)
    return pdf[years.notna() & (years >= min_year)].copy()


def filter_by_year(ddf: dd.DataFrame, min_year: int = 2024) -> dd.DataFrame:
    """Filter Dask DataFrame to rows with MONTH_YEAR year >= *min_year*."""
    return ddf.map_partitions(_filter_partition, min_year, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Task 04: Multi-file Dask ingestion
# ---------------------------------------------------------------------------


def _build_meta() -> pd.DataFrame:
    """Return the meta DataFrame for dd.from_delayed."""
    meta: dict[str, object] = {
        col: pd.array([], dtype=pd.StringDtype()) for col in EXPECTED_COLUMNS
    }
    for col in _FLOAT_COLS:
        meta[col] = pd.array([], dtype="float64")
    meta["source_file"] = pd.array([], dtype=pd.StringDtype())
    return pd.DataFrame(meta)


def build_dask_dataframe(
    file_paths: list[Path],
    min_year: int = 2024,
) -> dd.DataFrame:
    """Build a Dask DataFrame from multiple raw files.

    Raises:
        ValueError: if *file_paths* is empty.
    """
    if not file_paths:
        raise ValueError("file_paths must not be empty")

    delayed_dfs = [dask.delayed(read_raw_file)(fp) for fp in file_paths]
    meta = _build_meta()
    ddf = dd.from_delayed(delayed_dfs, meta=meta)
    n = min(len(file_paths), 50)
    ddf = ddf.repartition(npartitions=n)
    ddf = filter_by_year(ddf, min_year)

    logger.info(
        "Built Dask DataFrame",
        extra={"files": len(file_paths), "partitions": ddf.npartitions},
    )
    return ddf


# ---------------------------------------------------------------------------
# Task 05: Interim Parquet writer
# ---------------------------------------------------------------------------


def write_interim_parquet(ddf: dd.DataFrame, output_dir: str) -> int:
    """Write Dask DataFrame to *output_dir* as consolidated Parquet files.

    Returns:
        Number of Parquet files written.

    Raises:
        ValueError: if *output_dir* is an existing non-directory path.
    """
    out = Path(output_dir)
    if out.exists() and not out.is_dir():
        raise ValueError(f"{output_dir} exists but is not a directory")
    out.mkdir(parents=True, exist_ok=True)

    estimated_rows = ddf.npartitions * 10_000
    n_partitions = max(1, estimated_rows // 500_000)

    ddf.repartition(npartitions=n_partitions).to_parquet(
        str(out), write_index=False, overwrite=True
    )

    file_count = len(list(out.glob("*.parquet")))
    logger.info(
        "Wrote interim Parquet",
        extra={"files": file_count, "output_dir": str(out)},
    )
    return file_count


# ---------------------------------------------------------------------------
# Task 06: Ingest orchestrator and CLI
# ---------------------------------------------------------------------------


def run_ingest(raw_dir: str, output_dir: str, min_year: int = 2024) -> int:
    """Chain discover → build → write for the ingest stage."""
    paths = discover_raw_files(raw_dir)
    ddf = build_dask_dataframe(paths, min_year)
    return write_interim_parquet(ddf, output_dir)


def main() -> None:
    """CLI entry point for the ingest stage."""
    parser = argparse.ArgumentParser(description="KGS ingest: load raw files to interim Parquet")
    parser.add_argument("--raw-dir", default=_cfg.RAW_DATA_DIR)
    parser.add_argument("--output-dir", default=_cfg.INTERIM_DATA_DIR)
    parser.add_argument("--min-year", type=int, default=2024)
    args = parser.parse_args()

    count = run_ingest(
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
        min_year=args.min_year,
    )
    print(f"Ingest wrote {count} Parquet file(s).")


if __name__ == "__main__":
    main()
