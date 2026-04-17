"""Transform stage: clean, index, sort, and write processed Parquet."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task T-01: production_date derivation
# ---------------------------------------------------------------------------


def add_production_date(partition: pd.DataFrame) -> pd.DataFrame:
    """Parse MONTH-YEAR into a production_date datetime column.

    Drops rows with invalid or non-monthly MONTH-YEAR values (month=0, month=-1,
    non-numeric). Returns the partition with production_date as datetime64[ns].
    """
    if partition.empty:
        partition = partition.copy()
        partition["production_date"] = pd.Series(dtype="datetime64[ns]")
        return partition

    my_str = partition["MONTH-YEAR"].astype(str)

    # Split on "-" — month is the first element, year is the last element.
    # Edge cases: "-1-2024" splits as ['', '1', '2024'] → month_part='', year_part='2024'
    parts = my_str.str.split("-")
    month_part = parts.str[0]
    year_part = parts.str[-1]

    def _is_int(s: pd.Series) -> pd.Series:
        return s.str.match(r"^\d+$").fillna(False)

    valid_month = _is_int(month_part)
    valid_year = _is_int(year_part)

    # Month must be 1–12
    month_int = pd.to_numeric(month_part, errors="coerce")
    valid_range = (month_int >= 1) & (month_int <= 12)

    mask_valid = valid_month & valid_year & valid_range
    n_dropped = (~mask_valid).sum()
    if n_dropped > 0:
        logger.info("add_production_date: dropped %d rows with invalid MONTH-YEAR", n_dropped)

    partition = partition[mask_valid].copy()
    if partition.empty:
        partition["production_date"] = pd.Series(dtype="datetime64[ns]")
        return partition

    my_valid = partition["MONTH-YEAR"].astype(str)
    parts_valid = my_valid.str.split("-")
    m = pd.to_numeric(parts_valid.str[0], errors="coerce")
    y = pd.to_numeric(parts_valid.str[-1], errors="coerce")

    partition["production_date"] = pd.to_datetime({"year": y, "month": m, "day": 1})
    return partition


# ---------------------------------------------------------------------------
# Task T-02: Categorical cleaner
# ---------------------------------------------------------------------------


def clean_categoricals(
    partition: pd.DataFrame,
    categorical_cols: dict[str, list[str]],
) -> pd.DataFrame:
    """Replace out-of-vocabulary categorical values with NA and cast to CategoricalDtype.

    All masking is vectorized — no row iteration.
    """
    partition = partition.copy()
    for col_name, allowed in categorical_cols.items():
        if col_name not in partition.columns:
            continue
        col = partition[col_name].astype(object)  # work in object space for masking
        invalid_mask = col.notna() & ~col.isin(allowed)
        if invalid_mask.any():
            col = col.where(~invalid_mask, other=np.nan)
        partition[col_name] = col.astype(pd.CategoricalDtype(categories=allowed, ordered=False))
    return partition


# ---------------------------------------------------------------------------
# Task T-03: Physical bounds cleaner
# ---------------------------------------------------------------------------


def clean_physical_bounds(partition: pd.DataFrame) -> pd.DataFrame:
    """Enforce physical constraints on production values.

    - Replaces negative PRODUCTION with NA (TR-01).
    - Logs warning for oil PRODUCTION > 50,000 BBL/month (TR-02).
    - Preserves zero PRODUCTION as valid (TR-05).
    """
    partition = partition.copy()
    prod = partition["PRODUCTION"]

    negative_mask = prod < 0
    n_neg = int(negative_mask.sum())
    if n_neg > 0:
        logger.warning("Replacing %d negative PRODUCTION values with NA in partition", n_neg)
        partition.loc[negative_mask, "PRODUCTION"] = float("nan")

    # TR-02: flag suspicious high oil values
    if "PRODUCT" in partition.columns:
        prod_updated = partition["PRODUCTION"]
        oil_high_mask = (partition["PRODUCT"].astype(str) == "O") & (prod_updated > 50_000)
        n_high = int(oil_high_mask.sum())
        if n_high > 0:
            logger.warning(
                "%d oil PRODUCTION values exceed 50,000 BBL/month — possible unit error",
                n_high,
            )

    return partition


# ---------------------------------------------------------------------------
# Task T-04: Deduplicator
# ---------------------------------------------------------------------------


def deduplicate(partition: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows by (LEASE_KID, MONTH-YEAR, PRODUCT), keep first."""
    before = len(partition)
    partition = partition.drop_duplicates(
        subset=["LEASE_KID", "MONTH-YEAR", "PRODUCT"], keep="first"
    )
    after = len(partition)
    if before > after:
        logger.info("Dropped %d duplicate rows in partition", before - after)
    return partition


# ---------------------------------------------------------------------------
# Task T-05: Entity indexer and sorter
# ---------------------------------------------------------------------------


def set_entity_index(ddf: dd.DataFrame) -> dd.DataFrame:
    """Set the Dask DataFrame index to LEASE_KID and sort within partitions by production_date."""
    ddf = ddf.set_index("LEASE_KID", sorted=False, drop=True)

    meta = ddf._meta.sort_values("production_date")
    ddf = ddf.map_partitions(
        lambda part: part.sort_values("production_date"),
        meta=meta,
    )
    return ddf


# ---------------------------------------------------------------------------
# Task T-06: Transform runner
# ---------------------------------------------------------------------------


def run_transform(config: dict) -> dd.DataFrame:
    """Orchestrate the full transform stage.

    Reads interim Parquet, applies all cleaning steps, indexes and sorts,
    repartitions, and writes to data/processed/transform/.
    Returns the lazy Dask DataFrame.
    """
    interim_dir = config["interim_dir"]
    if not Path(interim_dir).exists():
        raise FileNotFoundError(f"interim_dir does not exist: {interim_dir}")

    t0 = time.time()
    logger.info("Transform stage starting — reading from %s", interim_dir)

    ddf: dd.DataFrame = dd.read_parquet(interim_dir)

    categorical_cols: dict[str, list[str]] = {
        "PRODUCT": ["O", "G"],
        "TWN_DIR": ["S", "N"],
        "RANGE_DIR": ["E", "W"],
    }

    # T-01: add production_date
    meta_01 = add_production_date(ddf._meta.copy())
    ddf = ddf.map_partitions(add_production_date, meta=meta_01)

    # T-02: clean categoricals
    meta_02 = clean_categoricals(ddf._meta.copy(), categorical_cols)
    ddf = ddf.map_partitions(clean_categoricals, categorical_cols, meta=meta_02)

    # T-03: physical bounds
    meta_03 = clean_physical_bounds(ddf._meta.copy())
    ddf = ddf.map_partitions(clean_physical_bounds, meta=meta_03)

    # T-04: deduplication
    meta_04 = deduplicate(ddf._meta.copy())
    ddf = ddf.map_partitions(deduplicate, meta=meta_04)

    # T-05: entity index + sort
    ddf = set_entity_index(ddf)

    # Repartition (last op before write — ADR-004)
    n = ddf.npartitions
    n_out = max(10, min(n, 50))
    ddf = ddf.repartition(npartitions=n_out)

    processed_dir = config["processed_dir"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    ddf.to_parquet(processed_dir, engine="pyarrow", write_index=True, overwrite=True)

    elapsed = time.time() - t0
    logger.info(
        "Transform complete: %d partitions written to %s (%.1fs)",
        n_out,
        processed_dir,
        elapsed,
    )
    return ddf
