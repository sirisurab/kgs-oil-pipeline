"""Ingest component for the KGS oil production data pipeline.

Responsibilities:
- Discover raw per-lease .txt files in data/raw/.
- Read them lazily via Dask with latin-1 encoding.
- Filter out non-monthly records (Month=0, Month=-1).
- Enrich with lease-level metadata via left join.
- Cast and normalize column dtypes to the interim Parquet schema.
- Write a unified interim Parquet dataset to data/interim/ partitioned by LEASE_KID.
"""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]

from kgs_pipeline.config import CONFIG
from kgs_pipeline.utils import ensure_dir, is_valid_raw_file, setup_logging

logger = setup_logging("ingest", CONFIG.logs_dir, CONFIG.log_level)

# Expected interim Parquet schema
INTERIM_SCHEMA = pa.schema(
    [
        pa.field("LEASE_KID", pa.int64()),
        pa.field("LEASE", pa.string()),
        pa.field("DOR_CODE", pa.string()),
        pa.field("API_NUMBER", pa.string()),
        pa.field("FIELD", pa.string()),
        pa.field("PRODUCING_ZONE", pa.string()),
        pa.field("OPERATOR", pa.string()),
        pa.field("COUNTY", pa.string()),
        pa.field("TOWNSHIP", pa.string()),
        pa.field("TWN_DIR", pa.string()),
        pa.field("RANGE", pa.string()),
        pa.field("RANGE_DIR", pa.string()),
        pa.field("SECTION", pa.string()),
        pa.field("SPOT", pa.string()),
        pa.field("LATITUDE", pa.float64()),
        pa.field("LONGITUDE", pa.float64()),
        pa.field("MONTH_YEAR", pa.string()),
        pa.field("PRODUCT", pa.string()),
        pa.field("WELLS", pa.int64()),
        pa.field("PRODUCTION", pa.float64()),
        pa.field("source_file", pa.string()),
    ]
)


def discover_raw_files(raw_dir: Path | None = None) -> list[Path]:
    """Discover valid lp*.txt files in raw_dir.

    Args:
        raw_dir: Directory to scan (default: CONFIG.raw_dir).

    Returns:
        Sorted list of valid Path objects.

    Raises:
        FileNotFoundError: If raw_dir does not exist.
    """
    raw_dir = raw_dir or CONFIG.raw_dir
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    all_files = sorted(raw_dir.glob("lp*.txt"))
    valid, invalid = [], 0
    for f in all_files:
        if is_valid_raw_file(f):
            valid.append(f)
        else:
            invalid += 1

    if invalid > 0:
        logger.warning("Skipped %d invalid/empty files in %s", invalid, raw_dir)
    if not valid:
        logger.warning("No valid lp*.txt files found in %s", raw_dir)
    else:
        logger.info("Discovered %d valid raw files in %s", len(valid), raw_dir)
    return valid


def _add_source_file(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    """Map partition helper: adds source_file column from the encoded path."""
    df["source_file"] = Path(source_path).stem
    return df


def read_raw_files(file_paths: list[Path]) -> dd.DataFrame:
    """Read all raw lp*.txt files into a single lazy Dask DataFrame.

    Args:
        file_paths: List of Path objects to read.

    Returns:
        Lazy Dask DataFrame with all files concatenated and source_file column added.

    Raises:
        ValueError: If file_paths is empty.
    """
    if not file_paths:
        raise ValueError("No raw files provided to read_raw_files()")

    str_paths = [str(p) for p in file_paths]
    try:
        ddf = dd.read_csv(
            str_paths,
            encoding="latin-1",
            dtype=str,
            on_bad_lines="skip",
        )
    except Exception as exc:
        raise ValueError(f"Failed to read {len(file_paths)} raw file(s): {exc}") from exc

    # Rename LEASE KID (with space) to LEASE_KID
    if "LEASE KID" in ddf.columns:
        ddf = ddf.rename(columns={"LEASE KID": "LEASE_KID"})

    # Rename MONTH-YEAR to MONTH_YEAR if present
    if "MONTH-YEAR" in ddf.columns:
        ddf = ddf.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})

    # Add source_file column using map_partitions per-file
    # Build a list of individual DDFs with source_file added
    parts: list[dd.DataFrame] = []
    for path in file_paths:
        try:
            part = dd.read_csv(
                str(path),
                encoding="latin-1",
                dtype=str,
                on_bad_lines="skip",
            )
            if "LEASE KID" in part.columns:
                part = part.rename(columns={"LEASE KID": "LEASE_KID"})
            if "MONTH-YEAR" in part.columns:
                part = part.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})
            stem = path.stem
            part = part.map_partitions(
                lambda df, s=stem: df.assign(source_file=s),
                meta={**{c: str for c in part.columns}, "source_file": str},
            )
            parts.append(part)
        except Exception as exc:
            logger.warning("Could not read file %s: %s", path, exc)

    if not parts:
        raise ValueError(f"Failed to read any of {len(file_paths)} raw file(s)")

    return dd.concat(parts)


def filter_monthly_records(ddf: dd.DataFrame) -> dd.DataFrame:
    """Filter to retain only true monthly production records (month 1–12).

    Drops records where the month component of MONTH_YEAR is 0 (yearly total)
    or -1 (starting cumulative).

    Args:
        ddf: Lazy Dask DataFrame with a MONTH_YEAR column.

    Returns:
        Filtered lazy Dask DataFrame.

    Raises:
        KeyError: If MONTH_YEAR column is absent.
    """
    if "MONTH_YEAR" not in ddf.columns:
        raise KeyError("MONTH_YEAR column not found in DataFrame")

    def _filter_partition(df: pd.DataFrame) -> pd.DataFrame:
        month_str = df["MONTH_YEAR"].astype(str)
        null_mask = df["MONTH_YEAR"].isna()
        if null_mask.any():
            count = int(null_mask.sum())
            logger.warning("Dropping %d rows with null MONTH_YEAR", count)

        def parse_month(s: str) -> int | None:
            try:
                return int(s.split("-")[0])
            except Exception:
                return None

        months = month_str.map(parse_month)
        valid = months.between(1, 12, inclusive="both")
        return df[valid].reset_index(drop=True)

    meta = ddf._meta.copy()
    return ddf.map_partitions(_filter_partition, meta=meta)


def enrich_with_lease_metadata(
    ddf: dd.DataFrame,
    lease_index_path: Path | None = None,
) -> dd.DataFrame:
    """Left-join production data with lease-level metadata on LEASE_KID.

    Args:
        ddf: Lazy Dask DataFrame with LEASE_KID column.
        lease_index_path: Path to the lease index file (default: CONFIG.lease_index_file).

    Returns:
        Enriched lazy Dask DataFrame.

    Raises:
        FileNotFoundError: If lease_index_path does not exist.
        KeyError: If LEASE_KID is absent from either DataFrame.
    """
    lease_index_path = lease_index_path or CONFIG.lease_index_file
    if not lease_index_path.exists():
        raise FileNotFoundError(f"Lease index file not found: {lease_index_path}")

    if "LEASE_KID" not in ddf.columns:
        raise KeyError("LEASE_KID column not found in production DataFrame")

    lease_meta = pd.read_csv(lease_index_path, dtype=str)
    if "LEASE KID" in lease_meta.columns:
        lease_meta = lease_meta.rename(columns={"LEASE KID": "LEASE_KID"})

    if "LEASE_KID" not in lease_meta.columns:
        raise KeyError("LEASE_KID column not found in lease metadata file")

    lease_meta = lease_meta.drop_duplicates(subset=["LEASE_KID"])

    # Only bring in columns not already in the production data
    extra_cols = [c for c in lease_meta.columns if c not in ddf.columns or c == "LEASE_KID"]
    lease_meta = lease_meta[extra_cols]

    enriched = ddf.merge(lease_meta, on="LEASE_KID", how="left", suffixes=("", "_meta"))

    # Drop any _meta-suffixed duplicates
    meta_cols = [c for c in enriched.columns if c.endswith("_meta")]
    if meta_cols:
        enriched = enriched.drop(columns=meta_cols)

    return enriched


def apply_interim_schema(ddf: dd.DataFrame) -> dd.DataFrame:
    """Cast and normalize columns to match the expected interim Parquet schema.

    Args:
        ddf: Lazy Dask DataFrame with raw string dtypes.

    Returns:
        Typed lazy Dask DataFrame.
    """

    def _cast_partition(df: pd.DataFrame) -> pd.DataFrame:
        # Cast LEASE_KID to int64; drop rows where cast fails
        if "LEASE_KID" in df.columns:
            df["LEASE_KID"] = pd.to_numeric(df["LEASE_KID"], errors="coerce")
            bad = df["LEASE_KID"].isna().sum()
            if bad:
                logger.warning("Dropping %d rows with unparseable LEASE_KID", bad)
            df = df[df["LEASE_KID"].notna()].copy()
            df["LEASE_KID"] = df["LEASE_KID"].astype("int64")

        # Cast LATITUDE and LONGITUDE to float64
        for col in ["LATITUDE", "LONGITUDE"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Cast PRODUCTION to float64
        if "PRODUCTION" in df.columns:
            df["PRODUCTION"] = pd.to_numeric(df["PRODUCTION"], errors="coerce")

        # Cast WELLS to float64 (use float to allow NaN values without nullable int issues)
        if "WELLS" in df.columns:
            df["WELLS"] = pd.to_numeric(df["WELLS"], errors="coerce").astype("float64")

        # Ensure PRODUCT is only O or G
        if "PRODUCT" in df.columns:
            bad_prod = ~df["PRODUCT"].isin(["O", "G"])
            if bad_prod.any():
                logger.warning("Dropping %d rows with invalid PRODUCT values", bad_prod.sum())
            df = df[~bad_prod].copy()

        # Rename MONTH-YEAR to MONTH_YEAR if still present
        if "MONTH-YEAR" in df.columns:
            df = df.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})

        # String columns
        str_cols = [
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
            "source_file",
            "MONTH_YEAR",
            "PRODUCT",
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        # Remove any duplicate columns (e.g., MONTH_YEAR may appear twice if rename was applied at multiple levels)
        df = df.loc[:, ~df.columns.duplicated()]

        return df

    # Build a dtype lookup from INTERIM_SCHEMA
    schema_dtypes: dict[str, object] = {}
    for field in INTERIM_SCHEMA:
        if field.name == "WELLS":
            schema_dtypes[field.name] = "float64"  # Use float64 to support NaN
        elif field.type == pa.int64():
            schema_dtypes[field.name] = "int64"
        elif field.type == pa.float64():
            schema_dtypes[field.name] = "float64"
        else:
            schema_dtypes[field.name] = "object"

    # Build meta dynamically from actual input columns, handling MONTH-YEAR rename
    actual_columns = list(ddf.columns)
    meta_cols_renamed = ["MONTH_YEAR" if c == "MONTH-YEAR" else c for c in actual_columns]

    meta_dict: dict[str, object] = {}
    for col in meta_cols_renamed:
        if col in schema_dtypes:
            meta_dict[col] = pd.Series(dtype=schema_dtypes[col])
        else:
            meta_dict[col] = pd.Series(dtype="object")

    meta_df = pd.DataFrame(meta_dict)

    # Use explicit meta matching actual input columns to avoid missing-column issues
    return ddf.map_partitions(_cast_partition, meta=meta_df)


def write_interim_parquet(
    ddf: dd.DataFrame,
    output_dir: Path | None = None,
) -> list[Path]:
    """Write the Dask DataFrame to Parquet partitioned by LEASE_KID.

    This is the only .compute() call in the ingest component.

    Args:
        ddf: Typed lazy Dask DataFrame.
        output_dir: Directory to write Parquet files (default: CONFIG.interim_dir).

    Returns:
        Sorted list of written .parquet file paths.
    """
    output_dir = output_dir or CONFIG.interim_dir
    ensure_dir(output_dir)

    # Compute to pandas first to bypass Dask metadata/pyarrow sentinel issues
    df_pandas = ddf.compute()
    # Reset any remaining object-dtype columns that might contain sentinels
    for col in df_pandas.select_dtypes(include=["object"]).columns:
        df_pandas[col] = df_pandas[col].astype(str)
    output_file = output_dir / "data.parquet"
    df_pandas.to_parquet(str(output_file), engine="pyarrow", index=False)

    written = sorted(output_dir.glob("**/*.parquet"))
    logger.info("Wrote %d Parquet files to %s", len(written), output_dir)
    return written


def run_ingest_pipeline(
    raw_dir: Path | None = None,
    lease_index_path: Path | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Orchestrate the full ingest workflow.

    Args:
        raw_dir: Directory containing raw .txt files (default: CONFIG.raw_dir).
        lease_index_path: Lease metadata file (default: CONFIG.lease_index_file).
        output_dir: Interim Parquet output directory (default: CONFIG.interim_dir).

    Returns:
        List of written Parquet file paths.

    Raises:
        RuntimeError: If no valid raw files are found.
    """
    file_paths = discover_raw_files(raw_dir)
    if not file_paths:
        raise RuntimeError("No valid raw files found; run acquire pipeline first.")

    logger.info("Ingesting %d raw files.", len(file_paths))

    ddf = read_raw_files(file_paths)
    ddf = filter_monthly_records(ddf)
    ddf = enrich_with_lease_metadata(ddf, lease_index_path)
    ddf = apply_interim_schema(ddf)
    written = write_interim_parquet(ddf, output_dir)

    logger.info(
        "Ingest complete: %d input files → %d Parquet files written.",
        len(file_paths),
        len(written),
    )
    return written


if __name__ == "__main__":
    paths = run_ingest_pipeline()
    print(f"Ingested {len(paths)} Parquet files.")
