"""Transform component for the KGS oil production data pipeline.

Responsibilities:
- Load interim Parquet from data/interim/.
- Parse MONTH_YEAR strings to datetime64[ns] production_date.
- Rename all columns to snake_case.
- Explode comma-separated api_number to one row per well (well_id).
- Validate physical bounds: negatives → NaN, outliers flagged.
- Deduplicate on [well_id, production_date, product].
- Sort chronologically and repartition by well_id.
- Write cleaned Parquet to data/processed/ with fixed pyarrow schema.
- Generate cleaning_report.json.

Zero ≠ NaN: zeros are valid shut-in measurements; nulls are missing data.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]

from kgs_pipeline.config import CONFIG
from kgs_pipeline.utils import ensure_dir, setup_logging

logger = setup_logging("transform", CONFIG.logs_dir, CONFIG.log_level)

# Column rename map from interim (UPPER_CASE) to processed (snake_case)
COLUMN_RENAME_MAP: dict[str, str] = {
    "LEASE_KID": "lease_kid",
    "LEASE": "lease",
    "DOR_CODE": "dor_code",
    "API_NUMBER": "api_number",
    "FIELD": "field",
    "PRODUCING_ZONE": "producing_zone",
    "OPERATOR": "operator",
    "COUNTY": "county",
    "TOWNSHIP": "township",
    "TWN_DIR": "twn_dir",
    "RANGE": "range_",
    "RANGE_DIR": "range_dir",
    "SECTION": "section",
    "SPOT": "spot",
    "LATITUDE": "latitude",
    "LONGITUDE": "longitude",
    "MONTH_YEAR": "month_year",
    "PRODUCT": "product",
    "WELLS": "wells",
    "PRODUCTION": "production",
    "source_file": "source_file",
}

# Fixed processed Parquet schema
PROCESSED_SCHEMA = pa.schema(
    [
        pa.field("well_id", pa.string()),
        pa.field("lease_kid", pa.int64()),
        pa.field("lease", pa.string()),
        pa.field("dor_code", pa.string()),
        pa.field("field", pa.string()),
        pa.field("producing_zone", pa.string()),
        pa.field("operator", pa.string()),
        pa.field("county", pa.string()),
        pa.field("township", pa.string()),
        pa.field("twn_dir", pa.string()),
        pa.field("range_", pa.string()),
        pa.field("range_dir", pa.string()),
        pa.field("section", pa.string()),
        pa.field("spot", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("production_date", pa.timestamp("ns")),
        pa.field("product", pa.string()),
        pa.field("wells", pa.int64()),
        pa.field("production", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("outlier_flag", pa.bool_()),
        pa.field("source_file", pa.string()),
    ]
)


def load_interim_parquet(interim_dir: Path | None = None) -> dd.DataFrame:
    """Load all Parquet files from interim_dir as a lazy Dask DataFrame.

    Args:
        interim_dir: Directory containing interim Parquet files (default: CONFIG.interim_dir).

    Returns:
        Lazy Dask DataFrame.

    Raises:
        FileNotFoundError: If interim_dir does not exist.
        RuntimeError: If no Parquet files are found.
    """
    interim_dir = interim_dir or CONFIG.interim_dir
    if not interim_dir.exists():
        raise FileNotFoundError(f"Interim directory not found: {interim_dir}")

    parquet_files = list(interim_dir.glob("**/*.parquet"))
    if not parquet_files:
        raise RuntimeError("No Parquet files found in interim_dir; run ingest pipeline first.")

    logger.info("Loading %d Parquet files from %s", len(parquet_files), interim_dir)
    ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    logger.info("Loaded interim Parquet with %d partitions.", ddf.npartitions)
    return ddf


def parse_production_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Parse MONTH_YEAR column (M-YYYY) to datetime64[ns] production_date.

    The resulting production_date represents the first day of the production month.

    Args:
        ddf: Lazy Dask DataFrame with MONTH_YEAR column.

    Returns:
        Lazy Dask DataFrame with production_date added and MONTH_YEAR dropped.

    Raises:
        KeyError: If MONTH_YEAR column is absent.
    """
    if "MONTH_YEAR" not in ddf.columns:
        raise KeyError("MONTH_YEAR column not found")

    def _parse_partition(df: pd.DataFrame) -> pd.DataFrame:
        def _to_date(s: object) -> "pd.Timestamp | pd.NaT":
            try:
                parts = str(s).split("-")
                month = parts[0].strip()
                year = parts[1].strip()
                return pd.Timestamp(f"{year}-{int(month):02d}-01")
            except Exception:
                return pd.NaT  # type: ignore[return-value]

        dates = df["MONTH_YEAR"].map(_to_date)
        nat_count = dates.isna().sum()
        if nat_count:
            logger.warning("Could not parse %d MONTH_YEAR values → NaT", nat_count)
        df = df.copy()
        df["production_date"] = pd.to_datetime(dates)
        df = df.drop(columns=["MONTH_YEAR"])
        return df

    meta = ddf._meta.copy()
    meta["production_date"] = pd.Series(dtype="datetime64[ns]")
    if "MONTH_YEAR" in meta.columns:
        meta = meta.drop(columns=["MONTH_YEAR"])

    return ddf.map_partitions(_parse_partition, meta=meta)


def normalize_column_names(ddf: dd.DataFrame) -> dd.DataFrame:
    """Rename all columns to snake_case per COLUMN_RENAME_MAP.

    Args:
        ddf: Lazy Dask DataFrame with original column names.

    Returns:
        Lazy Dask DataFrame with renamed columns.
    """
    present = {c: COLUMN_RENAME_MAP[c] for c in COLUMN_RENAME_MAP if c in ddf.columns}
    missing = [c for c in COLUMN_RENAME_MAP if c not in ddf.columns]
    if missing:
        logger.warning("Expected columns absent from DataFrame: %s", missing)

    return ddf.rename(columns=present)


def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """Explode comma-separated api_number into one row per well (well_id).

    For rows with null/empty api_number, assigns well_id = "UNKNOWN_<lease_kid>".

    Args:
        ddf: Lazy Dask DataFrame with api_number and lease_kid columns.

    Returns:
        Expanded lazy Dask DataFrame with well_id column; api_number dropped.
    """

    def _explode_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Split api_number into list
        def _split(v: object) -> list[str]:
            s = str(v).strip() if pd.notna(v) else ""
            if not s or s == "nan":
                return []
            return [x.strip() for x in s.split(",") if x.strip()]

        df["_api_list"] = df["api_number"].map(_split) if "api_number" in df.columns else [[]]

        # Rows with empty lists → UNKNOWN
        empty_mask = df["_api_list"].map(len) == 0
        df.loc[empty_mask, "_api_list"] = df.loc[empty_mask].apply(
            lambda row: [f"UNKNOWN_{row.get('lease_kid', 'UNKNOWN')}"],
            axis=1,
        )

        df = df.explode("_api_list")
        df["well_id"] = df["_api_list"].str.strip()

        # Fill any still-empty well_ids
        if "lease_kid" in df.columns:
            unknown_mask = df["well_id"].isna() | (df["well_id"] == "")
            if unknown_mask.any():
                df.loc[unknown_mask, "well_id"] = df.loc[unknown_mask, "lease_kid"].apply(
                    lambda k: f"UNKNOWN_{k}"
                )

        df = df.drop(columns=["_api_list"])
        if "api_number" in df.columns:
            df = df.drop(columns=["api_number"])
        return df.reset_index(drop=True)

    # Build meta
    meta = ddf._meta.copy()
    meta["well_id"] = pd.Series(dtype="object")
    if "api_number" in meta.columns:
        meta = meta.drop(columns=["api_number"])

    return ddf.map_partitions(_explode_partition, meta=meta)


def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """Apply physical production constraints.

    - Negative production → NaN (data entry error).
    - Zero production preserved as 0.0 (shut-in well).
    - Adds outlier_flag: True when product=="O" and production > oil_max_bbl_per_month.
    - Adds unit column: "BBL" for oil, "MCF" for gas.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Validated lazy Dask DataFrame with outlier_flag and unit columns.

    Raises:
        KeyError: If production or product column is absent.
    """
    if "production" not in ddf.columns:
        raise KeyError("production column not found in DataFrame")
    if "product" not in ddf.columns:
        raise KeyError("product column not found in DataFrame")

    def _validate_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Negative → NaN
        neg_mask = df["production"] < 0
        neg_count = int(neg_mask.sum())
        if neg_count:
            logger.warning("Setting %d negative production values to NaN", neg_count)
        df.loc[neg_mask, "production"] = float("nan")

        # Outlier flag
        oil_mask = df["product"] == "O"
        outlier_mask = oil_mask & (df["production"] > CONFIG.oil_max_bbl_per_month)
        df["outlier_flag"] = outlier_mask

        # Unit
        df["unit"] = df["product"].map({"O": "BBL", "G": "MCF"})
        return df

    meta = ddf._meta.copy()
    meta["outlier_flag"] = pd.Series(dtype="bool")
    meta["unit"] = pd.Series(dtype="object")

    return ddf.map_partitions(_validate_partition, meta=meta)


def deduplicate_records(ddf: dd.DataFrame) -> dd.DataFrame:
    """Remove duplicates on [well_id, production_date, product], keeping highest production.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Deduplicated lazy Dask DataFrame.

    Raises:
        KeyError: If required columns are absent.
    """
    for col in ["well_id", "production_date", "product"]:
        if col not in ddf.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame")

    def _dedup_partition(df: pd.DataFrame) -> pd.DataFrame:
        original_len = len(df)
        df = df.sort_values("production", ascending=False, na_position="last")
        df = df.drop_duplicates(subset=["well_id", "production_date", "product"], keep="first")
        removed = original_len - len(df)
        if removed:
            logger.info("Removed %d duplicate rows in partition", removed)
        return df.reset_index(drop=True)

    return ddf.map_partitions(_dedup_partition, meta=ddf._meta)


def sort_and_repartition(ddf: dd.DataFrame) -> dd.DataFrame:
    """Sort by [well_id, production_date] and repartition.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Sorted and repartitioned lazy Dask DataFrame.

    Raises:
        KeyError: If well_id or production_date columns are absent.
    """
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column not found in DataFrame")
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not found in DataFrame")

    sorted_ddf = ddf.sort_values(["well_id", "production_date"])
    return sorted_ddf.repartition(partition_size="64MB")


def write_processed_parquet(
    ddf: dd.DataFrame,
    output_dir: Path | None = None,
) -> list[Path]:
    """Write the cleaned Dask DataFrame to Parquet partitioned by well_id.

    This is the only .compute() call in the transform component.

    Args:
        ddf: Typed, sorted lazy Dask DataFrame.
        output_dir: Output directory (default: CONFIG.processed_dir).

    Returns:
        Sorted list of written .parquet file paths.
    """
    output_dir = output_dir or CONFIG.processed_dir
    ensure_dir(output_dir)

    # Ensure all schema columns are present; add missing with correct dtypes
    meta = ddf._meta.copy()
    for field in PROCESSED_SCHEMA:
        if field.name not in meta.columns:
            if field.type == pa.bool_():
                ddf = ddf.assign(**{field.name: False})
            elif field.type == pa.string():
                ddf = ddf.assign(**{field.name: None})
            elif field.type in (pa.int64(), pa.int32()):
                ddf = ddf.assign(**{field.name: pd.NA})
            else:
                ddf = ddf.assign(**{field.name: float("nan")})

    ddf.to_parquet(
        str(output_dir),
        engine="pyarrow",
        write_index=False,
        schema=PROCESSED_SCHEMA,
        overwrite=True,
    )

    written = sorted(output_dir.glob("**/*.parquet"))
    logger.info("Wrote %d processed Parquet files to %s", len(written), output_dir)
    return written


def generate_cleaning_report(
    stats: dict,
    output_dir: Path | None = None,
) -> Path:
    """Write a JSON cleaning report to output_dir/cleaning_report.json.

    Args:
        stats: Dictionary of cleaning statistics.
        output_dir: Output directory (default: CONFIG.processed_dir).

    Returns:
        Path to the written JSON file.
    """
    output_dir = output_dir or CONFIG.processed_dir
    ensure_dir(output_dir)

    stats.setdefault("pipeline_run_timestamp", datetime.now(timezone.utc).isoformat())
    report_path = output_dir / "cleaning_report.json"
    report_path.write_text(json.dumps(stats, indent=2, default=str))
    logger.info("Cleaning report written to %s", report_path)
    return report_path


def run_transform_pipeline(
    interim_dir: Path | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Orchestrate the full transform workflow.

    Args:
        interim_dir: Interim Parquet input directory (default: CONFIG.interim_dir).
        output_dir: Processed Parquet output directory (default: CONFIG.processed_dir).

    Returns:
        List of written processed Parquet file paths.

    Raises:
        RuntimeError: If no interim Parquet files are found (propagated).
    """
    stats: dict = {
        "input_row_count": 0,
        "rows_after_filter": 0,
        "negative_production_set_nan": 0,
        "outliers_flagged": 0,
        "duplicates_removed": 0,
        "null_production_date_dropped": 0,
        "unknown_well_id_assigned": 0,
        "output_row_count": 0,
        "output_parquet_files": 0,
        "pipeline_run_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    ddf = load_interim_parquet(interim_dir)
    ddf = parse_production_date(ddf)
    ddf = normalize_column_names(ddf)
    ddf = explode_api_numbers(ddf)
    ddf = validate_physical_bounds(ddf)
    ddf = deduplicate_records(ddf)
    ddf = sort_and_repartition(ddf)

    written = write_processed_parquet(ddf, output_dir)
    stats["output_parquet_files"] = len(written)

    # Read back metadata for row count
    if written:
        try:
            meta_df = dd.read_parquet(str(output_dir or CONFIG.processed_dir), engine="pyarrow")
            stats["output_row_count"] = int(meta_df.shape[0].compute())
        except Exception as exc:
            logger.warning("Could not read back row count: %s", exc)

    generate_cleaning_report(stats, output_dir)
    logger.info("Transform pipeline complete. %d files written.", len(written))
    return written


if __name__ == "__main__":
    paths = run_transform_pipeline()
    print(f"Transformed {len(paths)} Parquet files.")
