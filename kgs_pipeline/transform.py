"""Transform stage — clean, parse dates, index by entity, sort by date."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TRN-01: Production date parser
# ---------------------------------------------------------------------------


def parse_production_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert MONTH-YEAR to a production_date datetime column.

    Rows with month == 0 (yearly summaries) are dropped before parsing.

    Args:
        df: Partition DataFrame with a ``MONTH-YEAR`` column.

    Returns:
        DataFrame with ``production_date`` added and ``MONTH-YEAR`` removed.
    """
    if df.empty or "MONTH-YEAR" not in df.columns:
        if "MONTH-YEAR" in df.columns:
            df = df.drop(columns=["MONTH-YEAR"])
        if "production_date" not in df.columns:
            df = df.copy()
            df["production_date"] = pd.NaT
        return df

    # Drop month=0 rows (yearly summary)
    def _month(val: object) -> int | None:
        try:
            return int(str(val).split("-")[0].strip())
        except (ValueError, AttributeError):
            return None

    months = df["MONTH-YEAR"].map(_month)
    df = df[~((months.notna()) & (months == 0))].copy()

    def _to_date(val: object) -> pd.Timestamp:
        try:
            parts = str(val).split("-")
            month = int(parts[0].strip())
            year = int(parts[-1].strip())
            return pd.Timestamp(year=year, month=month, day=1)
        except Exception:
            return pd.NaT  # type: ignore[return-value]

    dates = df["MONTH-YEAR"].map(_to_date)
    bad_count = int(dates.isna().sum())
    if bad_count > 0:
        logger.warning(
            "parse_production_date: %d rows have unparseable MONTH-YEAR → NaT", bad_count
        )

    df = df.drop(columns=["MONTH-YEAR"])
    df["production_date"] = pd.to_datetime(dates)
    return df


# ---------------------------------------------------------------------------
# TRN-02: Null and invalid value cleaner
# ---------------------------------------------------------------------------


def clean_invalid_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with invalid/unresolvable values.

    Args:
        df: Partition DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    if df.empty:
        return df

    before = len(df)

    # Drop NaT production_date
    if "production_date" in df.columns:
        mask_nat = df["production_date"].isna()
        if mask_nat.any():
            logger.debug(
                "clean_invalid_values: dropping %d rows with NaT production_date", mask_nat.sum()
            )
            df = df[~mask_nat].copy()

    # Drop null LEASE_KID
    if "LEASE_KID" in df.columns:
        mask_kid = df["LEASE_KID"].isna()
        if mask_kid.any():
            logger.debug(
                "clean_invalid_values: dropping %d rows with null LEASE_KID", int(mask_kid.sum())
            )
            df = df[~mask_kid].copy()

    # Drop null PRODUCT
    if "PRODUCT" in df.columns:
        mask_prod = df["PRODUCT"].isna()
        if mask_prod.any():
            logger.debug(
                "clean_invalid_values: dropping %d rows with null PRODUCT", int(mask_prod.sum())
            )
            df = df[~mask_prod].copy()

    # Impute null PRODUCTION where WELLS > 0
    if "PRODUCTION" in df.columns and "WELLS" in df.columns:
        prod_null = df["PRODUCTION"].isna()
        wells_positive = df["WELLS"].notna() & (pd.to_numeric(df["WELLS"], errors="coerce") > 0)
        impute_mask = prod_null & wells_positive
        if impute_mask.any():
            df = df.copy()
            df.loc[impute_mask, "PRODUCTION"] = 0.0

    # Drop rows with negative PRODUCTION
    if "PRODUCTION" in df.columns:
        prod_num = pd.to_numeric(df["PRODUCTION"], errors="coerce")
        neg_mask = prod_num < 0
        if neg_mask.any():
            lease_ids = df.loc[neg_mask, "LEASE_KID"].tolist() if "LEASE_KID" in df.columns else []
            logger.warning(
                "clean_invalid_values: dropping %d rows with PRODUCTION < 0 (LEASE_KIDs: %s)",
                int(neg_mask.sum()),
                lease_ids[:5],
            )
            df = df[~neg_mask].copy()

    logger.debug("clean_invalid_values: %d → %d rows", before, len(df))
    return df


# ---------------------------------------------------------------------------
# TRN-03: Unit range validator
# ---------------------------------------------------------------------------


def validate_production_units(df: pd.DataFrame) -> pd.DataFrame:
    """Flag likely unit errors in production volumes (diagnostic only).

    Args:
        df: Partition DataFrame.

    Returns:
        Unchanged DataFrame (warnings emitted for flagged rows).
    """
    if df.empty or "PRODUCT" not in df.columns or "PRODUCTION" not in df.columns:
        return df

    prod_num = pd.to_numeric(df["PRODUCTION"], errors="coerce")
    # Flag oil rows > 50,000 BBL/month
    is_oil = df["PRODUCT"].astype(str) == "O"
    high_oil = is_oil & (prod_num > 50000)
    if high_oil.any():
        for _, row in df[high_oil].iterrows():
            logger.warning(
                "validate_production_units: LEASE_KID=%s has OIL PRODUCTION=%.1f > 50000 (likely unit error)",
                row.get("LEASE_KID", "?"),
                row.get("PRODUCTION", float("nan")),
            )
    return df


# ---------------------------------------------------------------------------
# TRN-04: Deduplicator
# ---------------------------------------------------------------------------


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows on (LEASE_KID, PRODUCT, production_date).

    Args:
        df: Partition DataFrame.

    Returns:
        Deduplicated DataFrame.
    """
    if df.empty:
        return df

    key_cols = [c for c in ["LEASE_KID", "PRODUCT", "production_date"] if c in df.columns]
    if not key_cols:
        return df

    return df.drop_duplicates(subset=key_cols, keep="first").copy()


# ---------------------------------------------------------------------------
# TRN-05: Entity indexer and temporal sorter
# ---------------------------------------------------------------------------


def index_and_sort(ddf: dd.DataFrame) -> dd.DataFrame:
    """Set LEASE_KID as index and sort by production_date within partitions.

    Args:
        ddf: Dask DataFrame after partition transforms.

    Returns:
        Dask DataFrame with LEASE_KID index and production_date sorted.

    Raises:
        KeyError: If required columns are absent.
    """
    if "LEASE_KID" not in ddf.columns:
        raise KeyError("LEASE_KID column not present")
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not present")

    ddf = ddf.set_index("LEASE_KID", drop=False, sorted=False)

    meta = ddf._meta.copy()  # noqa: SLF001
    meta_sorted = meta.sort_values("production_date")
    ddf = ddf.map_partitions(lambda df: df.sort_values("production_date"), meta=meta_sorted)

    return ddf


# ---------------------------------------------------------------------------
# TRN-06: Well completeness checker
# ---------------------------------------------------------------------------


def check_well_completeness(df: pd.DataFrame) -> pd.DataFrame:
    """Log warnings for gaps > 31 days in monthly production records.

    Args:
        df: Partition DataFrame with ``production_date``.

    Returns:
        Unchanged DataFrame (diagnostic only).
    """
    if df.empty or "production_date" not in df.columns:
        return df

    group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]
    if not group_cols:
        return df

    for key, grp in df.groupby(group_cols, sort=False):
        dates = pd.to_datetime(grp["production_date"]).sort_values()
        diffs = dates.diff().dropna()
        big_gaps = diffs[diffs.dt.days > 31]
        if not big_gaps.empty:
            for idx in big_gaps.index:
                gap_start = dates.loc[idx] - big_gaps.loc[idx]
                gap_end = dates.loc[idx]
                logger.warning(
                    "check_well_completeness: gap > 31 days for %s from %s to %s",
                    key,
                    gap_start.date(),
                    gap_end.date(),
                )
    return df


# ---------------------------------------------------------------------------
# TRN-07: Transform map_partitions orchestrator
# ---------------------------------------------------------------------------


def apply_partition_transforms(ddf: dd.DataFrame) -> dd.DataFrame:
    """Chain all partition-level transforms lazily.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with transforms applied (lazy).
    """
    meta1 = parse_production_date(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(parse_production_date, meta=meta1)

    meta2 = clean_invalid_values(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(clean_invalid_values, meta=meta2)

    meta3 = validate_production_units(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(validate_production_units, meta=meta3)

    meta4 = deduplicate(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(deduplicate, meta=meta4)

    meta5 = check_well_completeness(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(check_well_completeness, meta=meta5)

    return ddf


# ---------------------------------------------------------------------------
# TRN-08: Transform Parquet writer
# ---------------------------------------------------------------------------


def write_transform(ddf: dd.DataFrame, config: dict) -> None:
    """Write transformed Dask DataFrame to partitioned Parquet.

    Args:
        ddf: Transformed Dask DataFrame.
        config: Pipeline configuration dict.
    """
    output_dir = Path(config["transform"]["processed_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    n = ddf.npartitions
    n_partitions = max(10, min(n, 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    try:
        ddf.to_parquet(str(output_dir), write_index=True, overwrite=True)
        logger.info("Wrote transform Parquet to %s (%d partitions)", output_dir, n_partitions)
    except Exception as exc:
        logger.error("Failed to write transform Parquet: %s", exc)
        raise


# ---------------------------------------------------------------------------
# TRN-09: Data integrity spot-checker
# ---------------------------------------------------------------------------


def spot_check_integrity(
    interim_dir: Path,
    processed_dir: Path,
    sample_size: int,
) -> None:
    """Spot-check production values between interim and processed Parquet.

    Args:
        interim_dir: Path to interim Parquet directory.
        processed_dir: Path to processed Parquet directory.
        sample_size: Number of tuples to sample.
    """
    try:
        interim_df = pd.read_parquet(interim_dir)
        processed_df = pd.read_parquet(processed_dir)
    except Exception as exc:
        logger.warning("spot_check_integrity: cannot read Parquet files: %s", exc)
        return

    required = {"LEASE_KID", "PRODUCT", "production_date", "PRODUCTION"}
    if not required.issubset(interim_df.columns) or not required.issubset(processed_df.columns):
        logger.warning("spot_check_integrity: required columns missing, skipping")
        return

    keys = interim_df[["LEASE_KID", "PRODUCT", "production_date"]].drop_duplicates()
    if len(keys) == 0:
        return

    sample_n = min(sample_size, len(keys))
    sampled = keys.sample(n=sample_n, random_state=42)

    for _, row in sampled.iterrows():
        k_id = row["LEASE_KID"]
        product = row["PRODUCT"]
        p_date = row["production_date"]

        interim_row = interim_df[
            (interim_df["LEASE_KID"] == k_id)
            & (interim_df["PRODUCT"] == product)
            & (interim_df["production_date"] == p_date)
        ]
        processed_row = processed_df[
            (processed_df["LEASE_KID"] == k_id)
            & (processed_df["PRODUCT"] == product)
            & (processed_df["production_date"] == p_date)
        ]

        if interim_row.empty or processed_row.empty:
            continue

        interim_val = float(interim_row["PRODUCTION"].iloc[0])
        processed_val = float(processed_row["PRODUCTION"].iloc[0])

        if abs(interim_val - processed_val) > 1e-6:
            logger.warning(
                "spot_check_integrity: mismatch for LEASE_KID=%s PRODUCT=%s date=%s "
                "interim=%.4f processed=%.4f",
                k_id,
                product,
                p_date,
                interim_val,
                processed_val,
            )


# ---------------------------------------------------------------------------
# TRN-10: Stage entry point
# ---------------------------------------------------------------------------


def transform(config: dict, client: object) -> None:
    """Top-level entry point for the transform stage.

    Args:
        config: Pipeline configuration dict.
        client: Dask distributed Client.
    """
    interim_dir = Path(config["transform"]["interim_dir"])
    ddf = dd.read_parquet(str(interim_dir))

    ddf = apply_partition_transforms(ddf)
    ddf = index_and_sort(ddf)
    write_transform(ddf, config)

    processed_dir = Path(config["transform"]["processed_dir"])
    spot_check_integrity(interim_dir, processed_dir, sample_size=20)

    logger.info("Transform stage complete")
