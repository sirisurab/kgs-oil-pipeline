"""Features stage: cumulative production, GOR, water cut, decline rate, rolling features."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task F-01: Compute cumulative production volumes
# ---------------------------------------------------------------------------


def add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_production column: cumulative sum of PRODUCTION per LEASE_KID/PRODUCT group.

    The temporal sort from transform is preserved (stage-manifest-features H1).
    Shut-in months (zero production) produce a flat cumulative value (TR-03, TR-08).

    Uses vectorized grouped transform (ADR-002).

    Args:
        df: Pandas partition with PRODUCTION, LEASE_KID, PRODUCT, production_date.

    Returns:
        Partition with cum_production added.

    Raises:
        KeyError: If any required column is absent.
    """
    for col in ("PRODUCTION", "LEASE_KID", "PRODUCT", "production_date"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    result = df.copy()
    # Vectorized grouped cumsum — sort is guaranteed by transform stage
    result["cum_production"] = result.groupby(["LEASE_KID", "PRODUCT"], observed=True, sort=False)[
        "PRODUCTION"
    ].transform("cumsum")
    return result


# ---------------------------------------------------------------------------
# Task F-02: Compute GOR and water cut
# ---------------------------------------------------------------------------


def add_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add gor and water_cut columns derived from PRODUCTION values.

    GOR = gas_mcf / oil_bbl per LEASE_KID/production_date pair.
    Water cut is set to NaN (water data unavailable in KGS raw data).

    Zero-denominator rules (TR-06):
    - oil_bbl == 0 and gas_mcf > 0 → NaN
    - both zero → 0.0 (shut-in consistent sentinel)
    - oil_bbl > 0 and gas_mcf == 0 → 0.0

    Args:
        df: Pandas partition with PRODUCTION, PRODUCT, LEASE_KID, production_date.

    Returns:
        Partition with gor and water_cut added.

    Raises:
        KeyError: If PRODUCTION or PRODUCT are absent.
    """
    for col in ("PRODUCTION", "PRODUCT"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    result = df.copy()

    # Pivot oil and gas rows to wide format per (LEASE_KID, production_date)
    oil_rows = result[result["PRODUCT"] == "O"][
        ["LEASE_KID", "production_date", "PRODUCTION"]
    ].rename(columns={"PRODUCTION": "oil_bbl"})
    gas_rows = result[result["PRODUCT"] == "G"][
        ["LEASE_KID", "production_date", "PRODUCTION"]
    ].rename(columns={"PRODUCTION": "gas_mcf"})

    # Merge onto result by lease/date
    wide = oil_rows.merge(gas_rows, on=["LEASE_KID", "production_date"], how="outer")
    result = result.merge(wide, on=["LEASE_KID", "production_date"], how="left")

    oil = pd.to_numeric(result["oil_bbl"], errors="coerce").fillna(0.0)
    gas = pd.to_numeric(result["gas_mcf"], errors="coerce").fillna(0.0)

    # Compute GOR with explicit zero-denominator handling (TR-06)
    gor = pd.Series(np.nan, index=result.index, dtype="float64")
    both_zero = (oil == 0.0) & (gas == 0.0)
    oil_zero_gas_pos = (oil == 0.0) & (gas > 0.0)
    oil_pos_gas_zero = (oil > 0.0) & (gas == 0.0)
    normal = (oil > 0.0) & (gas > 0.0)

    gor[both_zero] = 0.0  # TR-06b consistent sentinel
    gor[oil_zero_gas_pos] = np.nan  # TR-06a
    gor[oil_pos_gas_zero] = 0.0  # TR-06c
    gor[normal] = gas[normal] / oil[normal]  # TR-06 normal case

    result["gor"] = gor

    # water_cut — water data is unavailable in KGS raw data
    result["water_cut"] = np.nan

    # Drop helper columns added during merge
    result = result.drop(columns=["oil_bbl", "gas_mcf"], errors="ignore")

    return result


# ---------------------------------------------------------------------------
# Task F-03: Compute production decline rate
# ---------------------------------------------------------------------------


def add_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate column: period-over-period production change per LEASE_KID/PRODUCT.

    Clipped to [-1.0, 10.0] per TR-07. Shut-in (0/0) is handled before clipping.

    Uses vectorized grouped transformations (ADR-002).

    Args:
        df: Pandas partition with PRODUCTION, LEASE_KID, PRODUCT, production_date.

    Returns:
        Partition with decline_rate added.

    Raises:
        KeyError: If any required column is absent.
    """
    for col in ("PRODUCTION", "LEASE_KID", "PRODUCT", "production_date"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    result = df.copy()

    def _decline(series: pd.Series) -> pd.Series:  # type: ignore[type-arg]
        prod = series.to_numpy(dtype=float, na_value=np.nan)
        prev = np.roll(prod, 1)
        prev[0] = np.nan  # first row has no prior
        # Handle shut-in (0/0) explicitly — produce 0.0, not inf/nan (TR-07d)
        both_zero = (prod == 0.0) & (prev == 0.0)
        with np.errstate(divide="ignore", invalid="ignore"):
            rate = np.where(prev == 0.0, np.nan, (prod - prev) / prev)
        rate_series = pd.Series(rate, index=series.index, dtype="float64")
        rate_series[both_zero] = 0.0
        return rate_series.clip(lower=-1.0, upper=10.0)

    result["decline_rate"] = result.groupby(["LEASE_KID", "PRODUCT"], observed=True, sort=False)[
        "PRODUCTION"
    ].transform(_decline)

    return result


# ---------------------------------------------------------------------------
# Task F-04: Compute rolling averages and lag features
# ---------------------------------------------------------------------------


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling_3m, rolling_6m, and lag_1 features per LEASE_KID/PRODUCT group.

    Temporal sort from transform is preserved (boundary-transform-features H1).
    Insufficient history → NaN (not zero fill) per TR-09b.

    Uses vectorized grouped transformations (ADR-002).

    Args:
        df: Pandas partition with PRODUCTION, production_date.

    Returns:
        Partition with rolling_3m, rolling_6m, lag_1 added.

    Raises:
        KeyError: If PRODUCTION or production_date are absent.
    """
    for col in ("PRODUCTION", "production_date"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    result = df.copy()

    grp = result.groupby(["LEASE_KID", "PRODUCT"], observed=True, sort=False)["PRODUCTION"]

    # min_periods=window to produce NaN when history < window (TR-09b)
    result["rolling_3m"] = grp.transform(lambda s: s.rolling(window=3, min_periods=3).mean())
    result["rolling_6m"] = grp.transform(lambda s: s.rolling(window=6, min_periods=6).mean())
    result["lag_1"] = grp.transform(lambda s: s.shift(1))

    return result


# ---------------------------------------------------------------------------
# Task F-05: Orchestrate features stage
# ---------------------------------------------------------------------------


def features(config: dict[str, Any]) -> dd.DataFrame:
    """Orchestrate the full features stage.

    Reads processed Parquet, applies all feature functions via map_partitions,
    repartitions, and writes ML-ready Parquet to data/processed/ (features subdir).

    Per ADR-003: meta derived by calling actual functions on zero-row copy of _meta.

    Args:
        config: Pipeline configuration dict.

    Returns:
        Lazy Dask DataFrame (not computed).

    Raises:
        FileNotFoundError: If processed Parquet path does not exist.
    """
    features_cfg = config["features"]
    processed_path = Path(features_cfg["processed_path"])
    output_path = Path(features_cfg["output_path"])

    if not processed_path.exists() or not any(processed_path.iterdir()):
        raise FileNotFoundError(f"Processed Parquet path not found or empty: {processed_path}")

    ddf = dd.read_parquet(str(processed_path))

    meta_cum = add_cumulative_production(ddf._meta.copy())
    ddf = ddf.map_partitions(add_cumulative_production, meta=meta_cum)

    meta_ratio = add_ratio_features(ddf._meta.copy())
    ddf = ddf.map_partitions(add_ratio_features, meta=meta_ratio)

    meta_decline = add_decline_rate(ddf._meta.copy())
    ddf = ddf.map_partitions(add_decline_rate, meta=meta_decline)

    meta_rolling = add_rolling_features(ddf._meta.copy())
    ddf = ddf.map_partitions(add_rolling_features, meta=meta_rolling)

    n_partitions = max(10, min(ddf.npartitions, 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    output_path.mkdir(parents=True, exist_ok=True)
    ddf.to_parquet(str(output_path), write_index=True, overwrite=True)

    logger.info("Features stage complete → %s", output_path)
    return ddf
