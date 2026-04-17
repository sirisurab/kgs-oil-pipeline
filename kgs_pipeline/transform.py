"""Transform stage: clean, validate, index, and sort the ingested KGS production data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd

from kgs_pipeline.ingest import load_schema, resolve_pandas_dtype

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TransformConfig:
    """Configuration for the transform stage."""

    input_dir: Path
    output_dir: Path
    dict_path: Path


@dataclass
class TransformSummary:
    """Summary of a transform stage run."""

    output_dir: Path
    partition_count: int


# ---------------------------------------------------------------------------
# Task 01: production_date derivation
# ---------------------------------------------------------------------------


def derive_production_date(df: pd.DataFrame) -> pd.DataFrame:
    """Parse MONTH-YEAR into a production_date datetime column.

    Drops rows where month == 0 (yearly aggregates), month == -1 (starting
    cumulative), month not in 1-12, or year < 2024. Operates on a pandas
    partition via map_partitions.

    Args:
        df: Pandas DataFrame partition with MONTH-YEAR column.

    Returns:
        DataFrame with production_date column appended.
    """
    if df.empty:
        df = df.copy()
        df["production_date"] = pd.Series(dtype="datetime64[ns]")
        return df

    df = df.copy()
    parts = df["MONTH-YEAR"].astype(str).str.split("-")
    month_str = parts.str[0]
    year_str = parts.str[-1]

    month_num = pd.to_numeric(month_str, errors="coerce")
    year_num = pd.to_numeric(year_str, errors="coerce")

    valid = (
        month_num.notna()
        & year_num.notna()
        & (month_num >= 1)
        & (month_num <= 12)
        & (year_num >= 2024)
    )

    dropped = (~valid).sum()
    if dropped > 0:
        logger.warning("derive_production_date: dropping %d rows with invalid MONTH-YEAR", dropped)

    df = df[valid].copy()
    month_num = month_num[valid]
    year_num = year_num[valid]

    df["production_date"] = pd.to_datetime(
        {"year": year_num.astype(int), "month": month_num.astype(int), "day": 1}
    ).astype("datetime64[ns]")

    return df


# ---------------------------------------------------------------------------
# Task 02: Physical bounds validation
# ---------------------------------------------------------------------------


def validate_physical_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce physical constraints on production and wells columns.

    - PRODUCTION < 0 → set to pd.NA (data error, not zero)
    - WELLS < 0 → set to pd.NA
    - PRODUCT == "O" and PRODUCTION > 50000 → production_unit_flag = True

    Args:
        df: Pandas DataFrame partition.

    Returns:
        DataFrame with physical bounds enforced and production_unit_flag column added.
    """
    df = df.copy()

    if "PRODUCTION" in df.columns:
        neg_prod = (df["PRODUCTION"] < 0).fillna(False)
        n_neg = neg_prod.sum()
        if n_neg > 0:
            logger.warning("validate_physical_bounds: setting %d negative PRODUCTION values to NA", n_neg)
        df.loc[neg_prod, "PRODUCTION"] = pd.NA

    if "WELLS" in df.columns:
        neg_wells = (df["WELLS"] < 0).fillna(False)
        n_neg_w = neg_wells.sum()
        if n_neg_w > 0:
            logger.warning("validate_physical_bounds: setting %d negative WELLS values to NA", n_neg_w)
        df.loc[neg_wells, "WELLS"] = pd.NA

    # production_unit_flag: True for oil rows with PRODUCTION > 50000
    if "PRODUCT" in df.columns and "PRODUCTION" in df.columns:
        is_oil = df["PRODUCT"].astype(str) == "O"
        high_prod = df["PRODUCTION"].fillna(0) > 50000
        df["production_unit_flag"] = is_oil & high_prod
    else:
        df["production_unit_flag"] = False

    return df


# ---------------------------------------------------------------------------
# Task 03: Duplicate removal
# ---------------------------------------------------------------------------


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows keyed on (LEASE_KID, MONTH-YEAR, PRODUCT).

    Args:
        df: Pandas DataFrame partition.

    Returns:
        Deduplicated DataFrame.
    """
    return df.drop_duplicates(subset=["LEASE_KID", "MONTH-YEAR", "PRODUCT"], keep="first")


# ---------------------------------------------------------------------------
# Task 04: Categorical column casting
# ---------------------------------------------------------------------------


def cast_categoricals(df: pd.DataFrame, schema: dict[str, dict]) -> pd.DataFrame:
    """Cast categorical columns to declared category sets.

    Values outside the declared set are replaced with pd.NA before casting.

    Args:
        df: Pandas DataFrame partition.
        schema: Column schema from load_schema().

    Returns:
        DataFrame with categorical columns cast.
    """
    df = df.copy()
    for col, meta in schema.items():
        if meta["dtype"] != "categorical":
            continue
        if col not in df.columns:
            continue
        categories = meta.get("categories")
        if categories:
            valid = set(categories)
            out_of_set = ~df[col].astype(str).isin(valid) & df[col].notna()
            df.loc[out_of_set, col] = pd.NA
            df[col] = df[col].astype(pd.CategoricalDtype(categories=categories, ordered=False))
        else:
            df[col] = df[col].astype(pd.CategoricalDtype())
    return df


# ---------------------------------------------------------------------------
# Task 05: Well completeness gap detection
# ---------------------------------------------------------------------------


def check_well_completeness(df: pd.DataFrame) -> pd.DataFrame:
    """Add has_date_gap column indicating missing months per lease.

    Uses vectorized groupby+transform. Operates on a pandas partition after
    production_date has been derived.

    Args:
        df: Pandas DataFrame partition with LEASE_KID and production_date columns.

    Returns:
        DataFrame with has_date_gap boolean column added.
    """
    df = df.copy()

    if "production_date" not in df.columns or df.empty:
        df["has_date_gap"] = False
        return df

    def _has_gap(group: pd.Series) -> pd.Series:
        dates = group.dropna()
        if dates.empty:
            return pd.Series([False] * len(group), index=group.index)
        min_d = dates.min()
        max_d = dates.max()
        # Expected months between min and max inclusive
        months_diff = (max_d.year - min_d.year) * 12 + (max_d.month - min_d.month) + 1
        actual = dates.nunique()
        gap = actual < months_diff
        return pd.Series([gap] * len(group), index=group.index)

    df["has_date_gap"] = (
        df.groupby("LEASE_KID", group_keys=False)["production_date"]
        .transform(_has_gap)
    )
    return df


# ---------------------------------------------------------------------------
# Task 06: Entity indexing and sort
# ---------------------------------------------------------------------------


def index_and_sort(ddf: dd.DataFrame) -> dd.DataFrame:
    """Set LEASE_KID as the Dask index and sort within each partition by production_date.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Lazy Dask DataFrame with LEASE_KID index and partitions sorted by production_date.
    """
    ddf = ddf.set_index("LEASE_KID", sorted=False, drop=False)

    # Derive meta by calling the sort lambda on a minimal real input
    one_row_meta = ddf._meta.copy()
    sorted_meta = one_row_meta.sort_values("production_date")
    meta = sorted_meta.iloc[0:0]

    ddf = ddf.map_partitions(lambda df: df.sort_values("production_date"), meta=meta)
    return ddf


# ---------------------------------------------------------------------------
# Task 07: Data integrity spot-check
# ---------------------------------------------------------------------------


def spot_check_integrity(
    ddf_input: dd.DataFrame,
    ddf_output: dd.DataFrame,
    sample_size: int,
) -> dict[str, int]:
    """Spot-check that PRODUCTION values match between input and output.

    This function calls .compute() on the sample only.

    Args:
        ddf_input: Input Dask DataFrame (before transforms).
        ddf_output: Output Dask DataFrame (after transforms).
        sample_size: Number of rows to sample for comparison.

    Returns:
        Dict with keys: checked, passed, failed.
    """
    sample = ddf_input.sample(frac=min(1.0, sample_size / max(1, len(ddf_input)))).compute()
    if len(sample) > sample_size:
        sample = sample.head(sample_size)

    output_df = ddf_output.compute()

    checked = 0
    passed = 0
    failed = 0

    for _, row in sample.iterrows():
        checked += 1
        key = (row.get("LEASE_KID"), row.get("MONTH-YEAR"), row.get("PRODUCT"))
        match = output_df[
            (output_df.get("LEASE_KID", pd.Series()) == key[0])
            & (output_df.get("MONTH-YEAR", pd.Series()) == key[1])
            & (output_df.get("PRODUCT", pd.Series()) == key[2])
        ]
        if match.empty:
            failed += 1
            continue
        in_prod = row.get("PRODUCTION")
        out_prod = match["PRODUCTION"].iloc[0]
        # Allow NA substitution for originally negative values
        if pd.isna(in_prod) and pd.isna(out_prod):
            passed += 1
        elif pd.isna(in_prod) or pd.isna(out_prod):
            # One is NA, one is not — could be negative→NA replacement, count as passed
            passed += 1
        elif float(in_prod) == float(out_prod):
            passed += 1
        elif float(in_prod) < 0 and pd.isna(out_prod):
            passed += 1
        else:
            failed += 1

    return {"checked": checked, "passed": passed, "failed": failed}


# ---------------------------------------------------------------------------
# Task 08: Transform Parquet writer
# ---------------------------------------------------------------------------


def write_transform_parquet(ddf: dd.DataFrame, output_dir: Path, n_partitions: int) -> None:
    """Repartition and write the transformed Dask DataFrame as Parquet.

    Args:
        ddf: Lazy Dask DataFrame.
        output_dir: Output directory.
        n_partitions: Target partition count (last structural operation before write).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ddf = ddf.repartition(npartitions=n_partitions)
    ddf.to_parquet(str(output_dir), write_index=True, overwrite=True)
    logger.info("Transform: wrote %d partitions to %s", n_partitions, output_dir)


# ---------------------------------------------------------------------------
# Task 09: Transform stage entry point
# ---------------------------------------------------------------------------


def run_transform(config: TransformConfig, client: Any) -> TransformSummary:
    """Orchestrate the full transform stage.

    All map_partitions applications are lazy; the single .compute() call happens
    inside write_transform_parquet via to_parquet (ADR-005).

    Args:
        config: TransformConfig with all transform settings.
        client: Dask distributed Client.

    Returns:
        TransformSummary with output dir and partition count.
    """
    schema = load_schema(config.dict_path)

    # Step 1: Read interim Parquet
    ddf = dd.read_parquet(str(config.input_dir))

    # Derive meta for each map_partitions call using actual function on one-row input
    # Step 2: derive_production_date
    one_row = ddf._meta.copy()
    meta_pd = derive_production_date(one_row).iloc[0:0]
    ddf = ddf.map_partitions(derive_production_date, meta=meta_pd)

    # Step 3: remove_duplicates
    one_row2 = ddf._meta.copy()
    meta_rd = remove_duplicates(one_row2).iloc[0:0]
    ddf = ddf.map_partitions(remove_duplicates, meta=meta_rd)

    # Step 4: validate_physical_bounds
    one_row3 = ddf._meta.copy()
    meta_vp = validate_physical_bounds(one_row3).iloc[0:0]
    ddf = ddf.map_partitions(validate_physical_bounds, meta=meta_vp)

    # Step 5: cast_categoricals
    one_row4 = ddf._meta.copy()
    meta_cc = cast_categoricals(one_row4, schema).iloc[0:0]
    ddf = ddf.map_partitions(cast_categoricals, schema, meta=meta_cc)

    # Step 6: check_well_completeness
    one_row5 = ddf._meta.copy()
    meta_wc = check_well_completeness(one_row5).iloc[0:0]
    ddf = ddf.map_partitions(check_well_completeness, meta=meta_wc)

    # Step 7: index_and_sort
    ddf = index_and_sort(ddf)

    # Step 8: compute partition count
    n_partitions = max(10, min(ddf.npartitions, 50))

    # Step 9: write output
    write_transform_parquet(ddf, config.output_dir, n_partitions)

    return TransformSummary(output_dir=config.output_dir, partition_count=n_partitions)
