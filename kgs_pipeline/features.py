"""Features stage: compute ML-ready derived features from processed Parquet."""

import logging
import os

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water columns (float64) to the partition.

    Input is entity-indexed on LEASE_KID, sorted by production_date per entity.
    PRODUCTION contains oil (PRODUCT=="O") and gas (PRODUCT=="G") in the same column.
    cum_water is all-np.nan because the schema has no separate water column.
    """
    df = df.copy()

    # Separate oil and gas production into their own float columns
    oil_prod = df["PRODUCTION"].where(df["PRODUCT"] == "O", other=0.0)
    gas_prod = df["PRODUCTION"].where(df["PRODUCT"] == "G", other=0.0)

    # Vectorized grouped cumsum (ADR-002); groupby index level
    df["cum_oil"] = oil_prod.groupby(level=0).cumsum().astype("float64")
    df["cum_gas"] = gas_prod.groupby(level=0).cumsum().astype("float64")
    df["cum_water"] = np.nan
    df["cum_water"] = df["cum_water"].astype("float64")

    return df


def add_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add gor (float64) and water_cut (float64) columns.

    GOR = gas_mcf / oil_bbl per entity-month, broadcast to all rows of each
    (entity, production_date) pair.  water_cut requires a water column which is
    absent from the schema, so it is all np.nan.
    """
    df = df.copy()

    # Extract per-row oil and gas volumes (nan for the other product)
    oil_bbl = df["PRODUCTION"].where(df["PRODUCT"] == "O", other=0.0)
    gas_mcf = df["PRODUCTION"].where(df["PRODUCT"] == "G", other=0.0)

    # Aggregate oil and gas per (entity, production_date) so we can divide
    # group key is the index (LEASE_KID) + production_date
    entity_idx = df.index
    date_col = df["production_date"] if "production_date" in df.columns else None

    if date_col is not None:
        group_keys = [entity_idx, date_col]
        oil_per_group = oil_bbl.groupby(group_keys).transform("sum")
        gas_per_group = gas_mcf.groupby(group_keys).transform("sum")
    else:
        oil_per_group = oil_bbl.groupby(level=0).transform("sum")
        gas_per_group = gas_mcf.groupby(level=0).transform("sum")

    # GOR: handle zero-denominator before division (ADR-003, TR-06)
    with np.errstate(divide="ignore", invalid="ignore"):
        gor_vals = np.where(
            oil_per_group == 0,
            np.nan,
            gas_per_group / oil_per_group,
        )
    df["gor"] = pd.array(gor_vals, dtype="float64")

    # water_cut: no water column in schema → all nan (TR-10)
    df["water_cut"] = np.nan
    df["water_cut"] = df["water_cut"].astype("float64")

    return df


def add_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate (float64): period-over-period production change per entity.

    decline_rate = (prod_t - prod_{t-1}) / prod_{t-1}
    Zero-denominator → np.nan (before clipping).
    Clipped to [-1.0, 10.0] after handling zero-denominator.
    """
    df = df.copy()

    # Per-entity lag using vectorized grouped shift (ADR-002)
    prior = df.groupby(level=0)["PRODUCTION"].shift(1)

    # Handle zero denominator before clipping
    with np.errstate(divide="ignore", invalid="ignore"):
        rate = np.where(
            prior == 0,
            np.nan,
            (df["PRODUCTION"] - prior) / prior,
        )

    rate_series = pd.Series(pd.array(rate, dtype="float64"))
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
    df["decline_rate"] = df["decline_rate"].astype("float64")

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling_3m_oil, rolling_6m_oil, rolling_3m_gas, rolling_6m_gas (float64).

    Per-entity rolling averages; min_periods equals the window size so early months
    with insufficient history return np.nan (TR-09).
    """
    df = df.copy()

    oil_prod = df["PRODUCTION"].where(df["PRODUCT"] == "O", other=np.nan)
    gas_prod = df["PRODUCTION"].where(df["PRODUCT"] == "G", other=np.nan)

    grp_oil = oil_prod.groupby(level=0)
    grp_gas = gas_prod.groupby(level=0)

    df["rolling_3m_oil"] = grp_oil.transform(lambda s: s.rolling(3, min_periods=3).mean()).astype(
        "float64"
    )
    df["rolling_6m_oil"] = grp_oil.transform(lambda s: s.rolling(6, min_periods=6).mean()).astype(
        "float64"
    )
    df["rolling_3m_gas"] = grp_gas.transform(lambda s: s.rolling(3, min_periods=3).mean()).astype(
        "float64"
    )
    df["rolling_6m_gas"] = grp_gas.transform(lambda s: s.rolling(6, min_periods=6).mean()).astype(
        "float64"
    )

    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add lag1_oil and lag1_gas (float64) — per-entity lag-1 PRODUCTION values."""
    df = df.copy()

    oil_prod = df["PRODUCTION"].where(df["PRODUCT"] == "O", other=np.nan)
    gas_prod = df["PRODUCTION"].where(df["PRODUCT"] == "G", other=np.nan)

    df["lag1_oil"] = oil_prod.groupby(level=0).shift(1).astype("float64")
    df["lag1_gas"] = gas_prod.groupby(level=0).shift(1).astype("float64")

    return df


def apply_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Apply all feature functions to every partition via map_partitions.

    Meta is derived by calling the actual function on ddf._meta.iloc[0:0] (ADR-003).
    Does not call .compute() (ADR-005).
    """
    # --- add_cumulative_production ---
    meta_cum = add_cumulative_production(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(add_cumulative_production, meta=meta_cum)

    # --- add_ratio_features ---
    meta_ratio = add_ratio_features(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(add_ratio_features, meta=meta_ratio)

    # --- add_decline_rate ---
    meta_decline = add_decline_rate(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(add_decline_rate, meta=meta_decline)

    # --- add_rolling_features ---
    meta_rolling = add_rolling_features(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(add_rolling_features, meta=meta_rolling)

    # --- add_lag_features ---
    meta_lag = add_lag_features(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(add_lag_features, meta=meta_lag)

    return ddf


def features(config: dict) -> None:
    """Orchestrate the full features stage and write ML-ready Parquet."""
    processed_dir: str = config["features"]["processed_dir"]
    output_dir: str = config["features"]["output_dir"]

    logger.info("Features: reading processed Parquet from %s", processed_dir)
    ddf = dd.read_parquet(processed_dir)

    n = ddf.npartitions
    n_partitions = max(10, min(n, 50))
    logger.info("Features: %d input partitions → %d output partitions", n, n_partitions)

    ddf = apply_features(ddf)

    # Repartition is the last operation before write (ADR-004)
    ddf = ddf.repartition(npartitions=n_partitions)

    os.makedirs(output_dir, exist_ok=True)
    logger.info("Features: writing ML-ready Parquet to %s", output_dir)
    ddf.to_parquet(output_dir, overwrite=True)
    logger.info("Features: complete")
