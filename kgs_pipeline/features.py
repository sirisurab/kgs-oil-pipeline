"""
Features stage: compute all derived features for ML workflows.

Design decisions per ADRs and stage manifests:
- Scheduler reuse: stages reuse the pipeline's client (build-env-manifest).
- Laziness: no .compute() before final write (ADR-005).
- Partition count: max(10, min(n, 50)) as last operation before write (ADR-004).
- Vectorization: grouped transformations via groupby-transform/cumsum/shift (ADR-002).
  No per-row iteration and no per-entity Python loops.
- Input contract reliance: temporal ordering per boundary-transform-features.md H1.
  Features does not re-sort or re-index.
- Dtype policy: nullable-aware dtypes for all new float columns (ADR-003).
- Zero-vs-null distinction preserved (TR-05).
- Meta derivation: actual function called on zero-row copy of upstream _meta (ADR-003).
- Logging: dual-channel (ADR-006).

Product-aware aggregation: KGS production has PRODUCT ∈ {O, G} with PRODUCTION
column. F1 reshapes to one row per (LEASE_KID, production_date) with oil_bbl and
gas_mcf columns.

Attribute conflict resolution (F1 docstring): where an attribute (e.g. OPERATOR)
differs between PRODUCT=O and PRODUCT=G rows for the same (LEASE_KID, month),
the first occurrence after sorting by PRODUCT is taken. This is deterministic
and documented per the task spec requirement.
"""

from __future__ import annotations

import logging
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# Columns that identify a row (non-volume attributes to preserve after pivot)
_IDENTIFIER_COLS = [
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
    "source_file",
]

# New feature columns added by F1
_VOLUME_COLS = ["oil_bbl", "gas_mcf", "water_bbl"]

# New feature columns added by F2
_F2_COLS = ["cum_oil", "cum_gas", "cum_water", "gor", "water_cut", "decline_rate"]

# New feature columns added by F3
_F3_ROLLING_COLS = [
    "oil_bbl_rolling_3m",
    "oil_bbl_rolling_6m",
    "oil_bbl_lag_1m",
    "gas_mcf_rolling_3m",
    "gas_mcf_rolling_6m",
    "gas_mcf_lag_1m",
    "water_bbl_rolling_3m",
    "water_bbl_rolling_6m",
    "water_bbl_lag_1m",
]


def _empty_f1_output(meta_input: pd.DataFrame) -> pd.DataFrame:
    """Return empty F1 output schema from the input meta."""
    columns: dict[str, Any] = {}
    # LEASE_KID first (as a regular column)
    columns["LEASE_KID"] = pd.Series([], dtype="Int64")
    # production_date second
    columns["production_date"] = pd.Series([], dtype="datetime64[us]")
    # Volume columns next
    for col in _VOLUME_COLS:
        columns[col] = pd.Series([], dtype="Float64")
    # Then identifier columns from meta_input
    for c in _IDENTIFIER_COLS:
        if c in meta_input.columns:
            columns[c] = meta_input[c].iloc[:0]
    return pd.DataFrame(columns)


# ---------------------------------------------------------------------------
# Task F1: Product-wise reshape of production
# ---------------------------------------------------------------------------


def reshape_products(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot PRODUCT/PRODUCTION into oil_bbl, gas_mcf, water_bbl columns.

    Takes a transform-stage partition (entity-indexed on LEASE_KID, sorted by
    production_date within each group) and returns one row per
    (LEASE_KID, production_date).

    Attribute conflict resolution: where an identifier column (e.g. OPERATOR)
    differs between PRODUCT=O and PRODUCT=G rows for the same (LEASE_KID, month),
    the first occurrence in alphabetical PRODUCT order is taken (deterministic).

    Zero-vs-null distinction preserved (TR-05): PRODUCTION==0 produces
    oil_bbl==0 or gas_mcf==0 (not null).

    Empty input → empty output with full schema (stage-manifest-ingest H2 applies).

    Vectorization per ADR-002: uses pivot/groupby, no per-row loops.

    Parameters
    ----------
    df:
        Single partition from transform output (LEASE_KID is index or column).

    Returns
    -------
    pd.DataFrame
        One row per (LEASE_KID, production_date) with oil_bbl, gas_mcf, water_bbl.
    """
    if df.empty:
        return _empty_f1_output(df)

    # Reset index to work with LEASE_KID as a column if it's the index
    df_work = df.reset_index() if df.index.name == "LEASE_KID" else df.copy()

    # Ensure LEASE_KID is present
    if "LEASE_KID" not in df_work.columns:
        logger.warning("LEASE_KID column not found in partition; returning empty F1 output.")
        return _empty_f1_output(df_work)

    if "production_date" not in df_work.columns:
        logger.warning("production_date column not found; returning empty F1 output.")
        return _empty_f1_output(df_work)

    # Drop rows with PRODUCT values outside {O, G} — ADR-003 null sentinel rule
    product_col = df_work["PRODUCT"]
    if hasattr(product_col, "cat"):
        valid_mask = product_col.isin(["O", "G"])
    else:
        valid_mask = product_col.isin(["O", "G"])
    df_work = df_work[valid_mask].copy()

    if df_work.empty:
        return _empty_f1_output(df_work)

    group_key = ["LEASE_KID", "production_date"]

    # Pivot PRODUCTION by PRODUCT into oil_bbl, gas_mcf
    # Use pivot_table with aggfunc='first' to handle any duplicate (LEASE_KID, date, PRODUCT)
    prod_pivot = df_work.pivot_table(
        index=group_key,
        columns="PRODUCT",
        values="PRODUCTION",
        aggfunc="first",
    ).reset_index()

    prod_pivot.columns.name = None
    prod_pivot = prod_pivot.rename(columns={"O": "oil_bbl", "G": "gas_mcf"})

    # Ensure both volume columns exist
    if "oil_bbl" not in prod_pivot.columns:
        prod_pivot["oil_bbl"] = pd.array([pd.NA] * len(prod_pivot), dtype="Float64")
    else:
        prod_pivot["oil_bbl"] = prod_pivot["oil_bbl"].astype("Float64")

    if "gas_mcf" not in prod_pivot.columns:
        prod_pivot["gas_mcf"] = pd.array([pd.NA] * len(prod_pivot), dtype="Float64")
    else:
        prod_pivot["gas_mcf"] = prod_pivot["gas_mcf"].astype("Float64")

    # water_bbl: not in KGS source data — add as all-NA column (absent-column rule)
    prod_pivot["water_bbl"] = pd.array([pd.NA] * len(prod_pivot), dtype="Float64")

    # Merge identifier/attribute columns: take first occurrence per (LEASE_KID, date)
    # Sort by PRODUCT to make selection deterministic
    df_sorted = df_work.sort_values("PRODUCT")
    attr_cols = [c for c in _IDENTIFIER_COLS if c in df_sorted.columns]
    attrs = df_sorted.groupby(group_key, sort=False)[attr_cols].first().reset_index()

    result = prod_pivot.merge(attrs, on=group_key, how="left")

    # Cast volume columns to Float64 (ADR-003)
    for col in _VOLUME_COLS:
        if col in result.columns:
            result[col] = result[col].astype("Float64")

    return result


# ---------------------------------------------------------------------------
# Task F2: Cumulative, ratio, and decline-rate features
# ---------------------------------------------------------------------------


def add_cumulative_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water, gor, water_cut, decline_rate columns.

    Input is F1 output (one row per LEASE_KID × production_date, sorted by
    production_date within LEASE_KID per boundary-transform-features.md H1).

    ADR-002: vectorized grouped transformations only (groupby-transform/cumsum/shift).

    GOR (TR-06):
    - oil_bbl == 0 and gas_mcf > 0 → NaN
    - oil_bbl == 0 and gas_mcf == 0 → NaN (documented: both zero means no production)
    - oil_bbl > 0 → gas_mcf / oil_bbl

    Water-cut (TR-09(e), TR-10): water_bbl / (oil_bbl + water_bbl).
    When denominator == 0, water_cut == NaN.

    Decline rate (TR-07): month-over-month rate = (oil_bbl - prev) / prev.
    Clipped to [-1.0, 10.0] after raw computation. Raw computed first, then clipped.

    Cumulative monotonicity (TR-03): cumsum over non-negative float values.
    Flat across zero-production months (TR-08).

    Parameters
    ----------
    df:
        F1 output partition.

    Returns
    -------
    pd.DataFrame
        F1 columns plus cum_oil, cum_gas, cum_water, gor, water_cut, decline_rate.
    """
    if df.empty:
        result = df.copy()
        for col in _F2_COLS:
            result[col] = pd.Series([], dtype="Float64")
        return result

    df = df.copy()

    # Ensure LEASE_KID is a column (not index)
    if df.index.name == "LEASE_KID":
        df = df.reset_index()

    # --- Cumulative sums per LEASE_KID ---
    # groupby-transform with cumsum — vectorized (ADR-002)
    # NA-safe: cumsum skips NA by default in pandas groupby
    for src_col, cum_col in [
        ("oil_bbl", "cum_oil"),
        ("gas_mcf", "cum_gas"),
        ("water_bbl", "cum_water"),
    ]:
        if src_col in df.columns:
            df[cum_col] = (
                df.groupby("LEASE_KID", sort=False)[src_col]
                .transform(lambda s: s.astype("Float64").cumsum())
                .astype("Float64")
            )
        else:
            df[cum_col] = pd.array([pd.NA] * len(df), dtype="Float64")

    # --- GOR: gas_mcf / oil_bbl (TR-06) ---
    # oil == 0 → GOR is NaN regardless of gas value (TR-06 (a), (b))
    # oil > 0 → GOR = gas_mcf / oil_bbl (TR-06 (c))
    oil = (
        df["oil_bbl"].astype("Float64")
        if "oil_bbl" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Float64")
    )
    gas = (
        df["gas_mcf"].astype("Float64")
        if "gas_mcf" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Float64")
    )

    oil_zero = oil == 0
    gor_raw = gas / oil
    gor_raw = gor_raw.replace([np.inf, -np.inf], np.nan)
    df["gor"] = gor_raw.where(~oil_zero, other=np.nan).astype("Float64")

    # --- Water cut: water_bbl / (oil_bbl + water_bbl) (TR-09(e)) ---
    water = (
        df["water_bbl"].astype("Float64")
        if "water_bbl" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Float64")
    )
    total_liquid = oil + water
    total_zero = total_liquid == 0
    water_cut_raw = water / total_liquid
    df["water_cut"] = water_cut_raw.where(~total_zero, other=np.nan).astype("Float64")

    # --- Decline rate: month-over-month on oil_bbl per LEASE_KID (TR-07) ---
    # prev = previous month's oil_bbl within LEASE_KID (shift(1))
    prev_oil = (
        df.groupby("LEASE_KID", sort=False)["oil_bbl"]
        .transform(lambda s: s.astype("Float64").shift(1))
        .astype("Float64")
    )
    prev_nonzero = prev_oil != 0
    decline_raw = (oil - prev_oil) / prev_oil.where(prev_nonzero, other=np.nan)
    # Clip after raw computation (TR-07(d))
    df["decline_rate"] = decline_raw.clip(lower=-1.0, upper=10.0).astype("Float64")

    # Cast all F2 columns to Float64 (ADR-003)
    for col in _F2_COLS:
        if col in df.columns:
            df[col] = df[col].astype("Float64")

    return df


# ---------------------------------------------------------------------------
# Task F3: Rolling-average and lag features
# ---------------------------------------------------------------------------


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 3m/6m rolling averages and 1m lag for oil_bbl, gas_mcf, water_bbl.

    Rolling windows computed per LEASE_KID over production_date-sorted sequence.
    Months with history shorter than the window produce NaN (not silently zero)
    — TR-09(b). Full-window mode (min_periods=window) is used consistently
    across all rolling columns. If coder chooses partial-window mode, it must
    be applied identically across all columns.

    Lag features: 1-month lag via shift(1) within LEASE_KID (TR-09(c)).

    Vectorization per ADR-002: grouped rolling and shift operations.

    Parameters
    ----------
    df:
        F2 output partition.

    Returns
    -------
    pd.DataFrame
        F2 columns plus rolling and lag columns.
    """
    if df.empty:
        result = df.copy()
        for col in _F3_ROLLING_COLS:
            result[col] = pd.Series([], dtype="Float64")
        return result

    df = df.copy()

    if df.index.name == "LEASE_KID":
        df = df.reset_index()

    for base_col, window_3m, window_6m, lag_col in [
        ("oil_bbl", "oil_bbl_rolling_3m", "oil_bbl_rolling_6m", "oil_bbl_lag_1m"),
        ("gas_mcf", "gas_mcf_rolling_3m", "gas_mcf_rolling_6m", "gas_mcf_lag_1m"),
        ("water_bbl", "water_bbl_rolling_3m", "water_bbl_rolling_6m", "water_bbl_lag_1m"),
    ]:
        if base_col not in df.columns:
            for out_col in [window_3m, window_6m, lag_col]:
                df[out_col] = pd.array([pd.NA] * len(df), dtype="Float64")
            continue

        # Rolling averages: full-window mode (min_periods=window) — NaN for short history
        df[window_3m] = (
            df.groupby("LEASE_KID", sort=False)[base_col]
            .transform(lambda s: s.astype("Float64").rolling(window=3, min_periods=3).mean())
            .astype("Float64")
        )
        df[window_6m] = (
            df.groupby("LEASE_KID", sort=False)[base_col]
            .transform(lambda s: s.astype("Float64").rolling(window=6, min_periods=6).mean())
            .astype("Float64")
        )

        # 1-month lag via shift(1) within LEASE_KID
        df[lag_col] = (
            df.groupby("LEASE_KID", sort=False)[base_col]
            .transform(lambda s: s.astype("Float64").shift(1))
            .astype("Float64")
        )

    return df


# ---------------------------------------------------------------------------
# Task F4: Meta-derivation for features map_partitions calls
# ---------------------------------------------------------------------------


def derive_features_meta(
    func: Any,
    meta_input: pd.DataFrame,
    **kwargs: Any,
) -> pd.DataFrame:
    """Derive Dask meta by calling func on a zero-row copy of meta_input.

    ADR-003: Meta derivation and function execution must share the same code path.
    Separate construction of meta is prohibited.
    """
    return func(meta_input.iloc[:0].copy(), **kwargs)


# ---------------------------------------------------------------------------
# Task F5: Features stage entry point
# ---------------------------------------------------------------------------


def features(config: dict[str, Any]) -> None:
    """Stage entry point: apply F1 → F2 → F3 via map_partitions, write ML-ready Parquet.

    Input contract reliance per boundary-transform-features.md:
    - Entity-indexed on LEASE_KID.
    - Sorted by production_date within each partition.
    - Categoricals cast and clean.
    - No re-sorting, re-indexing, or re-casting needed.

    Laziness: no .compute() before final write (ADR-005).
    Partition count: max(10, min(n, 50)) as last operation before write (ADR-004).

    Parameters
    ----------
    config:
        Full pipeline config dict.
    """
    feat = config["features"]
    processed_dir = Path(feat["processed_dir"])
    features_dir = Path(feat["features_dir"])

    logger.info("Features: reading transform Parquet from %s", processed_dir)
    ddf = dd.read_parquet(str(processed_dir))

    # Derive meta for each function by calling on zero-row copy of upstream _meta
    # (ADR-003 — meta derivation shares the same code path as execution)
    meta_input = ddf._meta.iloc[:0].copy()

    meta_f1 = derive_features_meta(reshape_products, meta_input)
    ddf_f1 = ddf.map_partitions(reshape_products, meta=meta_f1)

    meta_f2 = derive_features_meta(add_cumulative_features, meta_f1.iloc[:0].copy())
    ddf_f2 = ddf_f1.map_partitions(add_cumulative_features, meta=meta_f2)

    meta_f3 = derive_features_meta(add_rolling_features, meta_f2.iloc[:0].copy())
    ddf_f3 = ddf_f2.map_partitions(add_rolling_features, meta=meta_f3)

    # LEASE_KID is already a regular column (not index) from reshape_products
    # No reset_index needed
    ddf_final = ddf_f3

    # Repartition — last operation before write (ADR-004)
    n_out = max(10, min(ddf_f3.npartitions, 50))
    ddf_final = ddf_final.repartition(npartitions=n_out)

    features_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Features: writing ML-ready Parquet to %s (%d partitions)", features_dir, n_out)

    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
    logger.info("Features complete.")
