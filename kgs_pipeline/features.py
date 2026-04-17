"""Features stage: pivot, cumulative production, GOR, decline rate, rolling, lag features."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task F-01: Wide-to-long pivot
# ---------------------------------------------------------------------------

def pivot_oil_gas(ddf: dd.DataFrame) -> dd.DataFrame:
    """Pivot transform output from one row per (LEASE_KID, MONTH-YEAR, PRODUCT)
    to one row per (LEASE_KID, production_date) with oil_bbl, gas_mcf, water_bbl.
    """
    # Reset index so LEASE_KID is a column available for merge
    ddf_reset = ddf.reset_index()

    # Determine metadata columns to carry through from oil rows
    meta_cols = [
        c for c in ddf_reset.columns
        if c not in ("PRODUCT", "PRODUCTION", "MONTH-YEAR", "source_file", "URL")
    ]
    # Always include LEASE_KID and production_date in merge key
    key_cols = ["LEASE_KID", "production_date"]
    keep_meta = [c for c in meta_cols if c not in key_cols]

    oil_cols = key_cols + keep_meta + ["PRODUCTION"]
    gas_cols = key_cols + ["PRODUCTION"]

    # Filter available columns
    oil_cols = [c for c in oil_cols if c in ddf_reset.columns]
    gas_cols = [c for c in gas_cols if c in ddf_reset.columns]

    oil_ddf = ddf_reset[ddf_reset["PRODUCT"] == "O"][oil_cols].rename(
        columns={"PRODUCTION": "oil_bbl"}
    )
    gas_ddf = ddf_reset[ddf_reset["PRODUCT"] == "G"][gas_cols].rename(
        columns={"PRODUCTION": "gas_mcf"}
    )

    n_oil = len(oil_ddf)  # lazy length — used only for logging at compute
    n_gas = len(gas_ddf)
    logger.info("Pivot: oil rows=%s, gas rows=%s", n_oil, n_gas)

    merged = oil_ddf.merge(gas_ddf, on=key_cols, how="outer")

    merged["oil_bbl"] = merged["oil_bbl"].fillna(0.0)
    merged["gas_mcf"] = merged["gas_mcf"].fillna(0.0)
    merged["water_bbl"] = 0.0

    # Ensure float64
    merged["oil_bbl"] = merged["oil_bbl"].astype(np.float64)
    merged["gas_mcf"] = merged["gas_mcf"].astype(np.float64)
    merged["water_bbl"] = merged["water_bbl"].astype(np.float64)

    return merged


# ---------------------------------------------------------------------------
# Task F-02: Cumulative production
# ---------------------------------------------------------------------------

def add_cumulative_production(partition: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative oil, gas, and water production per LEASE_KID."""
    partition = partition.copy()
    for src, dst in [("oil_bbl", "cum_oil"), ("gas_mcf", "cum_gas"), ("water_bbl", "cum_water")]:
        partition[dst] = partition.groupby("LEASE_KID")[src].cumsum()
    return partition


# ---------------------------------------------------------------------------
# Task F-03: GOR and water cut
# ---------------------------------------------------------------------------

def add_ratio_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Compute Gas-Oil Ratio (GOR) and water cut per row.

    GOR  = gas_mcf / oil_bbl  (NaN when oil_bbl == 0)
    water_cut = water_bbl / (oil_bbl + water_bbl)  (NaN when both == 0)
    """
    partition = partition.copy()
    oil = partition["oil_bbl"].astype(np.float64)
    gas = partition["gas_mcf"].astype(np.float64)
    water = partition["water_bbl"].astype(np.float64)

    # GOR: NaN when oil == 0 (regardless of gas value)
    gor = np.where(oil > 0, gas / oil, np.nan)
    partition["gor"] = gor.astype(np.float64)

    # Water cut: NaN when total liquid == 0
    total_liquid = oil + water
    water_cut = np.where(total_liquid > 0, water / total_liquid, np.nan)
    partition["water_cut"] = water_cut.astype(np.float64)

    return partition


# ---------------------------------------------------------------------------
# Task F-04: Decline rate
# ---------------------------------------------------------------------------

def add_decline_rate(partition: pd.DataFrame) -> pd.DataFrame:
    """Compute period-over-period oil decline rate per LEASE_KID, clipped to [-1, 10]."""
    partition = partition.copy()
    oil = partition["oil_bbl"].astype(np.float64)

    prev_oil = partition.groupby("LEASE_KID")["oil_bbl"].shift(1)

    # decline_rate = (prev - curr) / prev
    # Handle zero denominator before division:
    #   prev == 0 and curr == 0 → 0.0 (shut-in, no change)
    #   prev == 0 and curr > 0  → -1.0 (came back online, clip handles it)
    #   prev > 0                → standard formula
    #   prev is NaN (first month) → NaN
    raw = np.where(
        prev_oil.isna(),
        np.nan,
        np.where(
            prev_oil == 0,
            np.where(oil == 0, 0.0, -1.0),
            (prev_oil - oil) / prev_oil,
        ),
    )
    decline = pd.Series(raw, index=partition.index, dtype=np.float64).clip(-1.0, 10.0)
    # Restore NaN for first-month rows (clip converts NaN → NaN so this is fine)
    partition["decline_rate"] = decline
    return partition


# ---------------------------------------------------------------------------
# Task F-05: Rolling averages
# ---------------------------------------------------------------------------

def add_rolling_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Compute 3- and 6-month rolling averages of oil_bbl, gas_mcf, water_bbl per LEASE_KID.

    Uses min_periods=window so partial windows return NaN (TR-09b).
    """
    partition = partition.copy()
    rolling_spec = [
        ("oil_bbl", 3, "oil_roll_3"),
        ("oil_bbl", 6, "oil_roll_6"),
        ("gas_mcf", 3, "gas_roll_3"),
        ("gas_mcf", 6, "gas_roll_6"),
        ("water_bbl", 3, "water_roll_3"),
        ("water_bbl", 6, "water_roll_6"),
    ]
    for src_col, window, out_col in rolling_spec:
        partition[out_col] = (
            partition.groupby("LEASE_KID")[src_col]
            .transform(lambda s, w=window: s.rolling(w, min_periods=w).mean())
        )
    return partition


# ---------------------------------------------------------------------------
# Task F-06: Lag features
# ---------------------------------------------------------------------------

def add_lag_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Compute lag-1 features for oil_bbl, gas_mcf, and water_bbl per LEASE_KID."""
    partition = partition.copy()
    for src_col, out_col in [
        ("oil_bbl", "oil_lag_1"),
        ("gas_mcf", "gas_lag_1"),
        ("water_bbl", "water_lag_1"),
    ]:
        partition[out_col] = partition.groupby("LEASE_KID")[src_col].shift(1)
    return partition


# ---------------------------------------------------------------------------
# Task F-07: Features runner
# ---------------------------------------------------------------------------

def run_features(config: dict) -> dd.DataFrame:
    """Orchestrate the full features stage.

    Reads processed Parquet, pivots, computes all features, writes ML-ready Parquet.
    Returns the lazy Dask DataFrame.
    """
    processed_dir = config["processed_dir"]
    if not Path(processed_dir).exists():
        raise FileNotFoundError(f"processed_dir does not exist: {processed_dir}")

    t0 = time.time()
    logger.info("Features stage starting — reading from %s", processed_dir)

    ddf: dd.DataFrame = dd.read_parquet(processed_dir)

    # F-01: pivot
    ddf = pivot_oil_gas(ddf)

    # Build meta by applying each function to an empty DataFrame
    meta_cum = add_cumulative_production(ddf._meta.copy())
    ddf = ddf.map_partitions(add_cumulative_production, meta=meta_cum)

    meta_ratio = add_ratio_features(ddf._meta.copy())
    ddf = ddf.map_partitions(add_ratio_features, meta=meta_ratio)

    meta_decline = add_decline_rate(ddf._meta.copy())
    ddf = ddf.map_partitions(add_decline_rate, meta=meta_decline)

    meta_rolling = add_rolling_features(ddf._meta.copy())
    ddf = ddf.map_partitions(add_rolling_features, meta=meta_rolling)

    meta_lag = add_lag_features(ddf._meta.copy())
    ddf = ddf.map_partitions(add_lag_features, meta=meta_lag)

    # Repartition (last op before write — ADR-004)
    n_out = max(10, min(ddf.npartitions, 50))
    ddf = ddf.repartition(npartitions=n_out)

    output_dir = config["output_dir"]
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ddf.to_parquet(output_dir, engine="pyarrow", write_index=False, overwrite=True)

    elapsed = time.time() - t0
    logger.info(
        "Features complete: %d partitions written to %s (%.1fs)",
        n_out, output_dir, elapsed,
    )
    return ddf
