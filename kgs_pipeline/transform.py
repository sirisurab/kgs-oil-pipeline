"""
Transform stage: clean, reshape, entity-index, and sort the interim Parquet data.

Design decisions per ADRs and stage manifests:
- Scheduler reuse: stages reuse the pipeline's client (build-env-manifest).
- Laziness: no .compute() before final write (ADR-005).
- Partition count: max(10, min(n, 50)) as last operation before write (ADR-004).
- Vectorization: no per-row iteration (ADR-002).
- Null sentinel and dtype preservation (ADR-003).
- Sort-before-set_index ordering (stage-manifest-transform H1, H4):
  sort by production_date within each partition via map_partitions, then
  set_index on LEASE_KID as the last structural operation.
- String-operation row filtering done inside partition-level function (ADR-004).
- Logging: dual-channel (ADR-006).

Entity column: LEASE_KID (kgs_monthly_data_dictionary.csv, task spec).
Production date column: derived from MONTH-YEAR column format "M-YYYY".
"""

from __future__ import annotations

import logging
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task T1: Partition-level cleaning function
# ---------------------------------------------------------------------------


def clean_partition(df: pd.DataFrame, unit_error_threshold: float) -> pd.DataFrame:
    """Clean a single partition from ingest output.

    Operations per task spec and stage manifests:
    1. Derive production_date (nullable datetime) from MONTH-YEAR.
    2. Drop rows where MONTH-YEAR month component == 0 (yearly aggregate) or
       == -1 (starting cumulative) — these are not monthly records.
    3. Remove duplicates on (LEASE_KID, production_date, PRODUCT) — idempotent
       per TR-15.
    4. Replace PRODUCTION < 0 with null (TR-01 physical bound).
    5. Replace WELLS < 0 with null.
    6. Replace PRODUCTION above unit_error_threshold with null (TR-02).
    7. Preserve zeros as zeros — not converted to null (TR-05).
    8. Preserve categorical column dtypes (ADR-003, stage-manifest-transform H2).
    9. Sort by production_date within partition (stage-manifest-transform H1).

    Vectorization per ADR-002: no per-row iteration.

    Parameters
    ----------
    df:
        Ingest-output partition conforming to boundary-ingest-transform contract.
    unit_error_threshold:
        PRODUCTION values above this threshold are replaced with null (TR-02).
        Read from config.yaml → transform.unit_error_threshold.

    Returns
    -------
    pd.DataFrame
        Cleaned partition with production_date added and invalid values nulled.
    """
    if df.empty:
        # Return empty DataFrame with full schema including production_date
        result = df.copy()
        result["production_date"] = pd.Series([], dtype="datetime64[us]")
        return result

    df = df.copy()

    # Derive production_date from MONTH-YEAR (format "M-YYYY")
    # Drop rows with month component == 0 (yearly) or == -1 (starting cumulative)
    month_str = df["MONTH-YEAR"].str.strip().str.rsplit("-", n=1)
    month_component = month_str.str[0]
    year_component = month_str.str[-1]

    # Exclude month == 0 or month == -1 (non-monthly records per task spec)
    numeric_month_mask = month_component.str.match(r"^-?\d+$", na=False)
    month_int = pd.to_numeric(month_component.where(numeric_month_mask), errors="coerce")
    bad_month = month_int.isin([0, -1])
    df = df[~bad_month].copy()
    month_str = df["MONTH-YEAR"].str.strip().str.rsplit("-", n=1)
    month_component = month_str.str[0]
    year_component = month_str.str[-1]

    if df.empty:
        result = df.copy()
        result["production_date"] = pd.Series([], dtype="datetime64[us]")
        return result

    # Build production_date: combine month + year into first-of-month date
    month_int2 = pd.to_numeric(month_component, errors="coerce")
    year_int = pd.to_numeric(year_component, errors="coerce")

    # Only valid months/years produce a date; invalid → NaT
    valid_date = month_int2.notna() & year_int.notna() & (month_int2 >= 1) & (month_int2 <= 12)
    date_strings = pd.Series(index=df.index, dtype="object")
    date_strings[valid_date] = (
        year_int[valid_date].astype(int).astype(str)
        + "-"
        + month_int2[valid_date].astype(int).astype(str).str.zfill(2)
        + "-01"
    )
    df["production_date"] = pd.to_datetime(date_strings, format="%Y-%m-%d", errors="coerce")

    # Deduplicate on (LEASE_KID, production_date, PRODUCT) — idempotent (TR-15)
    dedup_cols = ["LEASE_KID", "production_date", "PRODUCT"]
    existing_dedup = [c for c in dedup_cols if c in df.columns]
    if existing_dedup:
        df = df.drop_duplicates(subset=existing_dedup)

    if df.empty:
        return df

    # Replace PRODUCTION < 0 with null (TR-01); zeros preserved (TR-05)
    if "PRODUCTION" in df.columns:
        prod = df["PRODUCTION"]
        # Only null out negative values; zero stays zero
        neg_mask = prod < 0
        if neg_mask.any():
            df["PRODUCTION"] = prod.where(~neg_mask, other=np.nan)

        # Replace unit-error outliers (TR-02)
        prod = df["PRODUCTION"]
        outlier_mask = prod > unit_error_threshold
        if outlier_mask.any():
            df["PRODUCTION"] = prod.where(~outlier_mask, other=np.nan)

    # Replace WELLS < 0 with null; zeros preserved (TR-05)
    if "WELLS" in df.columns:
        wells = df["WELLS"]
        neg_mask = wells < 0
        if neg_mask.any():
            df["WELLS"] = wells.where(~neg_mask, other=np.nan)

    # Sort by production_date within partition (H1 — sort must be valid at stage exit)
    df = df.sort_values("production_date", kind="mergesort", na_position="last")

    return df


# ---------------------------------------------------------------------------
# Task T2: Meta-derivation helper for map_partitions
# ---------------------------------------------------------------------------


def derive_meta(func: Any, meta_input: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Derive Dask meta by calling the actual function on a zero-row copy of meta_input.

    ADR-003: Meta derivation and function execution must share the same code path.
    Calling on a zero-row real DataFrame guarantees column names, order, and dtypes
    match the live partition output exactly — separate meta construction is prohibited.

    Parameters
    ----------
    func:
        The partition-level function to derive meta for.
    meta_input:
        Zero-row copy of the upstream _meta DataFrame.
    **kwargs:
        Extra keyword arguments forwarded to func.

    Returns
    -------
    pd.DataFrame
        Zero-row output with the exact schema func will produce.
    """
    return func(meta_input, **kwargs)


# ---------------------------------------------------------------------------
# Task T3: Transform stage entry point
# ---------------------------------------------------------------------------


def transform(config: dict[str, Any]) -> None:
    """Stage entry point: read interim Parquet, clean, sort, entity-index, write.

    Execution order per stage-manifest-transform:
    1. Read interim Parquet.
    2. Apply clean_partition via map_partitions (with T2-derived meta).
    3. sort_values on production_date happens inside clean_partition per partition.
    4. set_index on LEASE_KID — last structural operation before write (H4).
    5. repartition(max(10, min(n, 50))) — last operation before write (ADR-004, H3).
    6. Write Parquet.

    Note on TR-13: ADR-004 prohibits per-entity partitioning. After set_index +
    repartition, Dask may place multiple LEASE_KID values in one partition file.
    TR-13's "exactly one LEASE_KID per partition" is therefore inapplicable at
    production data volumes — see test_transform.py for the reconciliation comment.

    Parameters
    ----------
    config:
        Full pipeline config dict.
    """
    tfm = config["transform"]
    interim_dir = Path(tfm["interim_dir"])
    processed_dir = Path(tfm["processed_dir"])
    unit_error_threshold = float(tfm["unit_error_threshold"])

    logger.info("Transform: reading interim Parquet from %s", interim_dir)
    ddf = dd.read_parquet(str(interim_dir))

    # Derive meta by calling the actual function on a zero-row copy (ADR-003, T2)
    meta = derive_meta(
        clean_partition,
        ddf._meta.iloc[:0].copy(),
        unit_error_threshold=unit_error_threshold,
    )

    # Apply cleaning via map_partitions — lazy (ADR-005)
    ddf_clean = ddf.map_partitions(
        clean_partition, unit_error_threshold=unit_error_threshold, meta=meta
    )

    # set_index on LEASE_KID — last structural op before write (H4)
    # sorted=False lets Dask handle the distributed shuffle
    ddf_indexed = ddf_clean.set_index("LEASE_KID", sorted=False, drop=True)

    # Repartition to max(10, min(n, 50)) — last operation before write (ADR-004, H3)
    n_source_partitions = ddf_clean.npartitions
    n_out = max(10, min(n_source_partitions, 50))
    ddf_final = ddf_indexed.repartition(npartitions=n_out)

    processed_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Transform: writing processed Parquet to %s (%d partitions)", processed_dir, n_out)

    ddf_final.to_parquet(str(processed_dir), overwrite=True, write_index=True)
    logger.info("Transform complete.")
