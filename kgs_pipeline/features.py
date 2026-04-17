"""Features stage: compute ML-ready features from transformed KGS production data."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FeaturesConfig:
    """Configuration for the features stage."""

    input_dir: Path
    output_dir: Path
    manifest_path: Path


@dataclass
class FeaturesSummary:
    """Summary of a features stage run."""

    output_dir: Path
    manifest_path: Path
    partition_count: int
    feature_columns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Task 01: Cumulative production features
# ---------------------------------------------------------------------------


def add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative oil and gas production per lease.

    For oil rows (PRODUCT=="O"): cum_oil = cumulative sum of PRODUCTION per LEASE_KID.
    For gas rows (PRODUCT=="G"): cum_gas = cumulative sum of PRODUCTION per LEASE_KID.
    Opposite product type rows get pd.NA for the respective column.

    Partition must be sorted by production_date (guaranteed by transform).

    Args:
        df: Pandas DataFrame partition.

    Returns:
        DataFrame with cum_oil and cum_gas columns appended.
    """
    df = df.copy()
    df["cum_oil"] = pd.array([pd.NA] * len(df), dtype=pd.Float64Dtype())
    df["cum_gas"] = pd.array([pd.NA] * len(df), dtype=pd.Float64Dtype())

    oil_mask = df["PRODUCT"].astype(str) == "O"
    gas_mask = df["PRODUCT"].astype(str) == "G"

    if oil_mask.any():
        df.loc[oil_mask, "cum_oil"] = (
            df.loc[oil_mask]
            .groupby("LEASE_KID", group_keys=False)["PRODUCTION"]
            .cumsum()
        )

    if gas_mask.any():
        df.loc[gas_mask, "cum_gas"] = (
            df.loc[gas_mask]
            .groupby("LEASE_KID", group_keys=False)["PRODUCTION"]
            .cumsum()
        )

    return df


# ---------------------------------------------------------------------------
# Task 02: GOR (Gas-Oil Ratio) feature
# ---------------------------------------------------------------------------


def add_gor(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Gas-Oil Ratio per (LEASE_KID, MONTH-YEAR).

    GOR = gas_mcf / oil_bbl where gas_mcf is PRODUCTION for PRODUCT=="G" and
    oil_bbl is PRODUCTION for PRODUCT=="O" for the same lease and month.

    GOR rules (TR-06):
    - oil > 0: gor = gas / oil
    - oil == 0 and gas > 0: gor = NaN (gas well, no oil denominator)
    - oil == 0 and gas == 0 (shut-in): gor = 0.0
    - oil > 0 and gas == 0: gor = 0.0

    GOR is assigned to both oil and gas rows for the same (LEASE_KID, MONTH-YEAR).

    Args:
        df: Pandas DataFrame partition.

    Returns:
        DataFrame with gor column appended.
    """
    df = df.copy()
    df["gor"] = np.nan

    if df.empty or "PRODUCT" not in df.columns:
        return df

    # Pivot oil and gas production per (LEASE_KID, MONTH-YEAR)
    oil_df = df[df["PRODUCT"].astype(str) == "O"][["LEASE_KID", "MONTH-YEAR", "PRODUCTION"]].copy()
    gas_df = df[df["PRODUCT"].astype(str) == "G"][["LEASE_KID", "MONTH-YEAR", "PRODUCTION"]].copy()

    oil_df = oil_df.rename(columns={"PRODUCTION": "oil_bbl"})
    gas_df = gas_df.rename(columns={"PRODUCTION": "gas_mcf"})

    paired = pd.merge(oil_df, gas_df, on=["LEASE_KID", "MONTH-YEAR"], how="outer")
    paired["oil_bbl"] = paired["oil_bbl"].fillna(0.0)
    paired["gas_mcf"] = paired["gas_mcf"].fillna(0.0)

    # Compute GOR
    def _gor_row(row: pd.Series) -> float:
        oil = float(row["oil_bbl"])
        gas = float(row["gas_mcf"])
        if oil > 0:
            return gas / oil
        elif gas > 0:
            return np.nan  # gas well, undefined GOR
        else:
            return 0.0  # shut-in

    paired["gor_val"] = paired.apply(_gor_row, axis=1)

    # Map GOR back to both oil and gas rows by (LEASE_KID, MONTH-YEAR)
    gor_map = paired.set_index(["LEASE_KID", "MONTH-YEAR"])["gor_val"]

    def _lookup_gor(row: pd.Series) -> float:
        key = (row["LEASE_KID"], row["MONTH-YEAR"])
        try:
            return float(gor_map.loc[key])
        except (KeyError, TypeError):
            return np.nan

    # Vectorized lookup via merge (drop=True avoids spurious 'index' column)
    df_idx = df.reset_index(drop=True)
    gor_lookup = paired[["LEASE_KID", "MONTH-YEAR", "gor_val"]].copy()
    df_merged = df_idx.merge(gor_lookup, on=["LEASE_KID", "MONTH-YEAR"], how="left")
    df["gor"] = df_merged["gor_val"].values

    return df


# ---------------------------------------------------------------------------
# Task 03: Decline rate feature
# ---------------------------------------------------------------------------


def add_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Compute period-over-period production decline rate per (LEASE_KID, PRODUCT).

    decline_rate = (prod_t - prod_{t-1}) / prod_{t-1}
    Clipped to [-1.0, 10.0]. NaN for first record of each group.
    When lag_production == 0, raw rate is 0.0 before clipping (TR-07d).

    Args:
        df: Pandas DataFrame partition sorted by production_date.

    Returns:
        DataFrame with decline_rate column appended.
    """
    df = df.copy()
    lag = df.groupby(["LEASE_KID", "PRODUCT"], group_keys=False)["PRODUCTION"].shift(1)

    prod = df["PRODUCTION"].astype(float)
    lag_f = lag.astype(float)

    # Raw decline rate
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = np.where(
            lag_f.isna(),
            np.nan,
            np.where(lag_f == 0.0, 0.0, (prod - lag_f) / lag_f),
        )

    clipped = np.clip(raw, -1.0, 10.0)
    # Preserve NaN for first record
    clipped = np.where(np.isnan(raw), np.nan, clipped)

    df["decline_rate"] = clipped
    return df


# ---------------------------------------------------------------------------
# Task 04: Rolling average features
# ---------------------------------------------------------------------------


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute 3-month and 6-month rolling average production per (LEASE_KID, PRODUCT).

    Uses min_periods=1 so partial windows return partial means (TR-09b).

    Args:
        df: Pandas DataFrame partition sorted by production_date.

    Returns:
        DataFrame with rolling_3m_production and rolling_6m_production columns.
    """
    df = df.copy()

    def _roll(series: pd.Series, window: int) -> pd.Series:
        return series.rolling(window=window, min_periods=1).mean()

    df["rolling_3m_production"] = (
        df.groupby(["LEASE_KID", "PRODUCT"], group_keys=False)["PRODUCTION"]
        .transform(lambda s: _roll(s, 3))
    )
    df["rolling_6m_production"] = (
        df.groupby(["LEASE_KID", "PRODUCT"], group_keys=False)["PRODUCTION"]
        .transform(lambda s: _roll(s, 6))
    )
    return df


# ---------------------------------------------------------------------------
# Task 05: Lag feature
# ---------------------------------------------------------------------------


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute lag-1 production per (LEASE_KID, PRODUCT).

    Args:
        df: Pandas DataFrame partition sorted by production_date.

    Returns:
        DataFrame with lag_1_production column appended.
    """
    df = df.copy()
    df["lag_1_production"] = (
        df.groupby(["LEASE_KID", "PRODUCT"], group_keys=False)["PRODUCTION"].shift(1)
    )
    return df


# ---------------------------------------------------------------------------
# Task 06: Temporal features
# ---------------------------------------------------------------------------


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive calendar-based features from production_date.

    Args:
        df: Pandas DataFrame partition with production_date column.

    Returns:
        DataFrame with year, month, quarter, days_in_month columns appended.
    """
    df = df.copy()
    dt = df["production_date"]
    df["year"] = dt.dt.year.astype(pd.Int64Dtype())
    df["month"] = dt.dt.month.astype(pd.Int64Dtype())
    df["quarter"] = dt.dt.quarter.astype(pd.Int64Dtype())
    df["days_in_month"] = dt.dt.days_in_month.astype(pd.Int64Dtype())
    return df


# ---------------------------------------------------------------------------
# Task 07: Categorical label encoding
# ---------------------------------------------------------------------------


def build_encoding_maps(ddf: dd.DataFrame) -> dict[str, dict[str, int]]:
    """Compute global integer label encoding maps for categorical string columns.

    This function calls .compute() to materialize unique values.

    Args:
        ddf: Dask DataFrame with COUNTY, FIELD, PRODUCING_ZONE, OPERATOR columns.

    Returns:
        Dict of dicts: {column_name: {value: int_code, ...}}.
    """
    cols = ["COUNTY", "FIELD", "PRODUCING_ZONE", "OPERATOR"]
    encoding_maps: dict[str, dict[str, int]] = {}

    for col in cols:
        if col not in ddf.columns:
            encoding_maps[col] = {}
            continue
        unique_vals = ddf[col].dropna().unique().compute().tolist()
        unique_sorted = sorted(str(v) for v in unique_vals)
        encoding_maps[col] = {v: i for i, v in enumerate(unique_sorted)}

    # Fixed encoding for PRODUCT
    encoding_maps["PRODUCT"] = {"O": 0, "G": 1}
    return encoding_maps


def add_label_encodings(
    df: pd.DataFrame,
    encoding_maps: dict[str, dict[str, int]],
) -> pd.DataFrame:
    """Apply integer label encoding to categorical string columns.

    Values absent from the encoding map map to -1 (sentinel for unseen categories).

    Args:
        df: Pandas DataFrame partition.
        encoding_maps: Pre-computed encoding maps from build_encoding_maps().

    Returns:
        DataFrame with *_code columns appended.
    """
    df = df.copy()
    col_map = {
        "COUNTY": "county_code",
        "FIELD": "field_code",
        "PRODUCING_ZONE": "producing_zone_code",
        "OPERATOR": "operator_code",
        "PRODUCT": "product_code",
    }
    for src_col, dst_col in col_map.items():
        enc = encoding_maps.get(src_col, {})
        if src_col in df.columns:
            df[dst_col] = (
                df[src_col].astype(str).map(enc).fillna(-1).astype(pd.Int64Dtype())
            )
        else:
            df[dst_col] = pd.array([-1] * len(df), dtype=pd.Int64Dtype())
    return df


# ---------------------------------------------------------------------------
# Task 08: Feature manifest writer
# ---------------------------------------------------------------------------


def write_feature_manifest(
    feature_columns: list[str],
    encoding_maps: dict[str, dict[str, int]],
    output_path: Path,
) -> None:
    """Write feature manifest JSON describing output columns and encoding maps.

    Args:
        feature_columns: List of all output column names.
        encoding_maps: Encoding maps from build_encoding_maps().
        output_path: Path to write the manifest JSON.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "feature_columns": feature_columns,
        "encoding_maps": encoding_maps,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "description": "Feature manifest for KGS oil and gas production ML features",
    }
    output_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Feature manifest written to %s", output_path)


# ---------------------------------------------------------------------------
# Task 09: Features Parquet writer
# ---------------------------------------------------------------------------


def write_features_parquet(ddf: dd.DataFrame, output_dir: Path, n_partitions: int) -> None:
    """Repartition and write the features Dask DataFrame as Parquet.

    Args:
        ddf: Lazy Dask DataFrame with all feature columns.
        output_dir: Output directory.
        n_partitions: Target partition count (last structural operation before write).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ddf = ddf.repartition(npartitions=n_partitions)
    ddf.to_parquet(str(output_dir), write_index=True, overwrite=True)
    logger.info("Features: wrote %d partitions to %s", n_partitions, output_dir)


# ---------------------------------------------------------------------------
# Task 10: Features stage entry point
# ---------------------------------------------------------------------------


def run_features(config: FeaturesConfig, client: Any) -> FeaturesSummary:
    """Orchestrate the full features stage.

    All map_partitions applications are lazy; the single .compute() call
    (aside from build_encoding_maps) happens inside write_features_parquet
    via to_parquet (ADR-005, TR-17).

    Args:
        config: FeaturesConfig with all features settings.
        client: Dask distributed Client.

    Returns:
        FeaturesSummary with output dir, manifest path, partitions, and feature columns.
    """
    # Step 1: Read transformed Parquet
    ddf = dd.read_parquet(str(config.input_dir))

    # Step 2: Build encoding maps (one .compute() call)
    encoding_maps = build_encoding_maps(ddf)

    # Steps 3-9: Compose lazy feature pipeline via map_partitions

    # Step 3: Temporal features
    one_row = ddf._meta.copy()
    meta3 = add_temporal_features(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_temporal_features, meta=meta3)

    # Step 4: Cumulative production
    one_row = ddf._meta.copy()
    meta4 = add_cumulative_production(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_cumulative_production, meta=meta4)

    # Step 5: Decline rate
    one_row = ddf._meta.copy()
    meta5 = add_decline_rate(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_decline_rate, meta=meta5)

    # Step 6: Rolling features
    one_row = ddf._meta.copy()
    meta6 = add_rolling_features(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_rolling_features, meta=meta6)

    # Step 7: Lag features
    one_row = ddf._meta.copy()
    meta7 = add_lag_features(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_lag_features, meta=meta7)

    # Step 8: GOR
    one_row = ddf._meta.copy()
    meta8 = add_gor(one_row).iloc[0:0]
    ddf = ddf.map_partitions(add_gor, meta=meta8)

    # Step 9: Label encodings
    one_row = ddf._meta.copy()
    meta9 = add_label_encodings(one_row, encoding_maps).iloc[0:0]
    ddf = ddf.map_partitions(add_label_encodings, encoding_maps, meta=meta9)

    # Step 10: Compute partition count
    n_partitions = max(10, min(ddf.npartitions, 50))

    # Step 11: Write output Parquet
    write_features_parquet(ddf, config.output_dir, n_partitions)

    # Step 12: Write feature manifest
    feature_columns = list(ddf._meta.columns)
    write_feature_manifest(feature_columns, encoding_maps, config.manifest_path)

    return FeaturesSummary(
        output_dir=config.output_dir,
        manifest_path=config.manifest_path,
        partition_count=n_partitions,
        feature_columns=feature_columns,
    )
