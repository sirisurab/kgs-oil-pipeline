"""Transform component — cleans and restructures interim data to well-level processed Parquet."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow as pa

import kgs_pipeline.config as config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task 13: Column renaming and dtype casting
# ---------------------------------------------------------------------------


def rename_and_cast_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """Rename columns per COLUMN_RENAME_MAP and cast to correct dtypes.

    Unknown columns not in the rename map are preserved unchanged.

    Args:
        ddf: Lazy Dask DataFrame with raw column names.

    Returns:
        Lazy Dask DataFrame with standardised column names and dtypes.
    """
    # Only rename columns that exist in the DataFrame
    rename = {k: v for k, v in config.COLUMN_RENAME_MAP.items() if k in ddf.columns}
    if rename:
        ddf = ddf.rename(columns=rename)

    def _cast_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        str_cols = [
            "lease_kid", "lease_name", "dor_code", "api_number", "field_name",
            "producing_zone", "operator", "county", "twn_dir", "range_dir",
            "spot", "month_year", "product", "source_file", "url",
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).where(df[col].notna(), other=pd.NA)

        float_cols = ["latitude", "longitude", "production"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        int_cols = {
            "township": pd.Int32Dtype(),
            "range_num": pd.Int32Dtype(),
            "section": pd.Int32Dtype(),
            "well_count": pd.Int32Dtype(),
        }
        for col, dtype in int_cols.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

        return df

    meta = ddf._meta.copy()
    # Build expected meta with correct dtypes
    meta = _cast_partition(meta)
    return ddf.map_partitions(_cast_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 14: Production date parser
# ---------------------------------------------------------------------------


def parse_production_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Parse month_year string (M-YYYY) into datetime64[ns] production_date column.

    Drops the month_year column after parsing.

    Args:
        ddf: Lazy Dask DataFrame with month_year column.

    Returns:
        Lazy Dask DataFrame with production_date column and no month_year.

    Raises:
        KeyError: If month_year column is absent.
    """
    if "month_year" not in ddf.columns:
        raise KeyError("Required column 'month_year' is absent from the DataFrame")

    def _parse_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        parts = df["month_year"].astype(str).str.split("-", n=1, expand=True)
        month = parts[0].str.zfill(2)
        year = parts[1] if parts.shape[1] > 1 else pd.Series([""] * len(df), index=df.index)
        date_str = year + "-" + month + "-01"
        df["production_date"] = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce")
        df = df.drop(columns=["month_year"])
        return df

    meta = ddf._meta.copy()
    meta["production_date"] = pd.Series(dtype="datetime64[ns]")
    if "month_year" in meta.columns:
        meta = meta.drop(columns=["month_year"])

    return ddf.map_partitions(_parse_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 15: API number explosion (lease-to-well expansion)
# ---------------------------------------------------------------------------


def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """Split comma-separated api_number into one row per well (well_id).

    Args:
        ddf: Lazy Dask DataFrame with api_number and lease_kid columns.

    Returns:
        Lazy Dask DataFrame with well_id column (api_number renamed).

    Raises:
        KeyError: If api_number or lease_kid columns are absent.
    """
    if "api_number" not in ddf.columns:
        raise KeyError("Required column 'api_number' is absent from the DataFrame")
    if "lease_kid" not in ddf.columns:
        raise KeyError("Required column 'lease_kid' is absent from the DataFrame")

    def _explode_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Normalise api_number: strip whitespace, empty strings -> NaN
        df["api_number"] = df["api_number"].astype(str).str.strip()
        df.loc[df["api_number"].isin(["", "nan", "None", "<NA>"]), "api_number"] = np.nan

        # Count rows with NaN lease_kid for logging
        nan_lease_count = df["lease_kid"].isna().sum()
        if nan_lease_count > 0:
            logger.warning("%d rows have NaN lease_kid; assigning well_id='LEASE-UNKNOWN'", nan_lease_count)

        # Synthetic ID for null api_number
        synthetic = "LEASE-" + df["lease_kid"].astype(str).where(
            df["lease_kid"].notna(), other="UNKNOWN"
        )
        df["api_number"] = df["api_number"].fillna(synthetic)

        # Split on comma to produce lists
        df["api_number"] = df["api_number"].str.split(",")

        # Explode
        df = df.explode("api_number")
        df["api_number"] = df["api_number"].astype(str).str.strip()

        df = df.rename(columns={"api_number": "well_id"})
        return df

    meta = ddf._meta.copy()
    if "api_number" in meta.columns:
        meta = meta.rename(columns={"api_number": "well_id"})
    meta["well_id"] = pd.Series(dtype="object")

    return ddf.map_partitions(_explode_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 16: Physical bounds validation
# ---------------------------------------------------------------------------


def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """Flag and nullify physically impossible production values.

    Adds outlier_flag boolean column. Negative production -> NaN + flag=True.
    Oil > OIL_OUTLIER_THRESHOLD_BBL -> flag=True (value retained).

    Args:
        ddf: Lazy Dask DataFrame with production and product columns.

    Returns:
        Lazy Dask DataFrame with outlier_flag column.

    Raises:
        KeyError: If production or product columns are absent.
    """
    if "production" not in ddf.columns:
        raise KeyError("Required column 'production' is absent from the DataFrame")
    if "product" not in ddf.columns:
        raise KeyError("Required column 'product' is absent from the DataFrame")

    threshold = config.OIL_OUTLIER_THRESHOLD_BBL

    def _validate_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["outlier_flag"] = False

        neg_mask = df["production"] < 0
        df.loc[neg_mask, "production"] = np.nan
        df.loc[neg_mask, "outlier_flag"] = True

        oil_high_mask = (df["product"] == "O") & (df["production"] > threshold)
        df.loc[oil_high_mask, "outlier_flag"] = True

        neg_count = neg_mask.sum()
        outlier_count = oil_high_mask.sum()
        if neg_count > 0 or outlier_count > 0:
            logger.info(
                "Physical validation: %d negative->NaN, %d oil outliers flagged",
                neg_count,
                outlier_count,
            )
        return df

    meta = ddf._meta.copy()
    meta["outlier_flag"] = pd.Series(dtype="bool")
    return ddf.map_partitions(_validate_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 17: Unit label assignment
# ---------------------------------------------------------------------------


def assign_unit_labels(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add unit column: 'BBL' for oil, 'MCF' for gas, 'UNKNOWN' otherwise.

    Args:
        ddf: Lazy Dask DataFrame with product column.

    Returns:
        Lazy Dask DataFrame with unit column.

    Raises:
        KeyError: If product column is absent.
    """
    if "product" not in ddf.columns:
        raise KeyError("Required column 'product' is absent from the DataFrame")

    def _assign_units(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["unit"] = np.where(
            df["product"] == "O",
            "BBL",
            np.where(df["product"] == "G", "MCF", "UNKNOWN"),
        )
        return df

    meta = ddf._meta.copy()
    meta["unit"] = pd.Series(dtype="object")
    return ddf.map_partitions(_assign_units, meta=meta)


# ---------------------------------------------------------------------------
# Task 18: Deduplication
# ---------------------------------------------------------------------------


def deduplicate_records(ddf: dd.DataFrame) -> dd.DataFrame:
    """Remove duplicate rows on [well_id, production_date, product], keeping last.

    Args:
        ddf: Lazy Dask DataFrame with well_id, production_date, product columns.

    Returns:
        Deduplicated lazy Dask DataFrame.

    Raises:
        KeyError: If any of the key columns are absent.
    """
    key_cols = ["well_id", "production_date", "product"]
    missing = [c for c in key_cols if c not in ddf.columns]
    if missing:
        raise KeyError(f"Missing deduplication key columns: {missing}")

    logger.debug(
        "Deduplicating records (partition count before: %d)", ddf.npartitions
    )

    def _dedup_partition(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=key_cols, keep="last")

    meta = ddf._meta
    ddf = ddf.map_partitions(_dedup_partition, meta=meta)
    ddf = ddf.drop_duplicates(subset=key_cols, keep="last")
    return ddf


# ---------------------------------------------------------------------------
# Task 19: Chronological sort and well-level repartitioning
# ---------------------------------------------------------------------------


def sort_and_repartition(ddf: dd.DataFrame) -> dd.DataFrame:
    """Sort records by well_id and production_date, repartition by well_id.

    Args:
        ddf: Lazy Dask DataFrame with well_id and production_date columns.

    Returns:
        Lazy Dask DataFrame indexed and sorted by well_id.

    Raises:
        KeyError: If well_id or production_date columns are absent.
    """
    if "well_id" not in ddf.columns:
        raise KeyError("Required column 'well_id' is absent from the DataFrame")
    if "production_date" not in ddf.columns:
        raise KeyError("Required column 'production_date' is absent from the DataFrame")

    ddf = ddf.set_index("well_id", drop=False, sorted=False)

    def _sort_partition(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(["well_id", "production_date"], ascending=True)

    meta = ddf._meta
    ddf = ddf.map_partitions(_sort_partition, meta=meta)
    return ddf


# ---------------------------------------------------------------------------
# Task 20: Processed Parquet writer with schema enforcement
# ---------------------------------------------------------------------------

PROCESSED_SCHEMA = pa.schema(
    [
        pa.field("lease_kid", pa.string()),
        pa.field("lease_name", pa.string()),
        pa.field("dor_code", pa.string()),
        pa.field("field_name", pa.string()),
        pa.field("producing_zone", pa.string()),
        pa.field("operator", pa.string()),
        pa.field("county", pa.string()),
        pa.field("township", pa.int32()),
        pa.field("twn_dir", pa.string()),
        pa.field("range_num", pa.int32()),
        pa.field("range_dir", pa.string()),
        pa.field("section", pa.int32()),
        pa.field("spot", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("product", pa.string()),
        pa.field("well_count", pa.int32()),
        pa.field("production", pa.float64()),
        pa.field("source_file", pa.string()),
        pa.field("production_date", pa.timestamp("ns")),
        pa.field("well_id", pa.string()),
        pa.field("outlier_flag", pa.bool_()),
        pa.field("unit", pa.string()),
    ]
)


def write_processed_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """Write well-level Dask DataFrame to Parquet with schema enforcement.

    Args:
        ddf: Lazy Dask DataFrame with all processed columns.
        output_dir: Directory where Parquet files are written.

    Returns:
        output_dir as a Path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(output_dir),
            engine="pyarrow",
            schema=PROCESSED_SCHEMA,
            write_index=True,
            overwrite=True,
        )
    except Exception as exc:
        logger.error("Schema mismatch writing processed Parquet: %s", exc)
        raise

    parquet_files = list(output_dir.glob("*.parquet"))
    logger.info(
        "Wrote processed Parquet to %s (%d files)", output_dir, len(parquet_files)
    )
    return output_dir


# ---------------------------------------------------------------------------
# Task 21: Transform pipeline orchestrator
# ---------------------------------------------------------------------------


def run_transform_pipeline() -> Path:
    """Orchestrate the full transform workflow end-to-end.

    Returns:
        Path to the processed Parquet directory.

    Raises:
        RuntimeError: If INTERIM_DATA_DIR contains no Parquet files.
    """
    start = time.time()
    logger.info("Transform pipeline starting")

    interim_files = list(config.INTERIM_DATA_DIR.glob("*.parquet"))
    if not interim_files:
        raise RuntimeError(
            f"No Parquet files found in INTERIM_DATA_DIR: {config.INTERIM_DATA_DIR}"
        )

    ddf = dd.read_parquet(str(config.INTERIM_DATA_DIR), engine="pyarrow")

    logger.info("Step: rename_and_cast_columns")
    ddf = rename_and_cast_columns(ddf)

    logger.info("Step: parse_production_date")
    ddf = parse_production_date(ddf)

    logger.info("Step: explode_api_numbers")
    ddf = explode_api_numbers(ddf)

    logger.info("Step: validate_physical_bounds")
    ddf = validate_physical_bounds(ddf)

    logger.info("Step: assign_unit_labels")
    ddf = assign_unit_labels(ddf)

    logger.info("Step: deduplicate_records")
    ddf = deduplicate_records(ddf)

    logger.info("Step: sort_and_repartition")
    ddf = sort_and_repartition(ddf)

    logger.info("Step: write_processed_parquet")
    output = write_processed_parquet(ddf, config.PROCESSED_DATA_DIR)

    elapsed = time.time() - start
    logger.info("Transform pipeline complete in %.1fs — output: %s", elapsed, output)
    return output
