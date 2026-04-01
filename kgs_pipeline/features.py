"""KGS feature engineering module.

Loads cleaned Parquet data, computes production features, encodes categoricals,
and writes the ML-ready dataset to data/processed/features/.
"""

from pathlib import Path

import dask.dataframe as dd  # type: ignore[import-untyped]
import numpy as np
import pandas as pd  # type: ignore[import-untyped]

try:
    from sklearn.preprocessing import LabelEncoder  # type: ignore[import-not-found]
except ImportError:
    LabelEncoder = None  # type: ignore[assignment,misc]

from kgs_pipeline.config import MAX_READ_PARTITIONS
from kgs_pipeline.utils import setup_logging

logger = setup_logging(__name__)


# ---------------------------------------------------------------------------
# Task 01: Cleaned data loader
# ---------------------------------------------------------------------------


def load_clean(clean_dir: str) -> dd.DataFrame:
    """Read all Parquet files from clean_dir into a Dask DataFrame.

    Args:
        clean_dir: Path to data/processed/clean/.

    Returns:
        Repartitioned Dask DataFrame (npartitions <= 50).

    Raises:
        FileNotFoundError: If the directory does not exist.
        ValueError: If no Parquet files are found.
    """
    path = Path(clean_dir)
    if not path.exists():
        raise FileNotFoundError(f"Clean directory not found: {clean_dir}")
    parquet_files = list(path.glob("*.parquet")) + list(path.glob("**/*.parquet"))
    if not parquet_files:
        raise ValueError(f"No Parquet files found in: {clean_dir}")

    ddf = dd.read_parquet(clean_dir)
    n = ddf.npartitions
    return ddf.repartition(npartitions=min(n, MAX_READ_PARTITIONS))


# ---------------------------------------------------------------------------
# Task 02: Oil/gas/water column pivot
# ---------------------------------------------------------------------------


def pivot_products(ddf: dd.DataFrame) -> dd.DataFrame:
    """Pivot from long format (one row per PRODUCT) to wide format.

    Creates separate oil_bbl and gas_mcf columns, plus water_bbl (NaN).

    Args:
        ddf: Cleaned Dask DataFrame with PRODUCT column.

    Returns:
        Wide-format Dask DataFrame with oil_bbl, gas_mcf, water_bbl columns.
    """
    df = ddf.compute()

    # Metadata columns from oil rows (preferred) or gas rows
    meta_cols = [
        "LEASE_KID",
        "production_date",
        "COUNTY",
        "PRODUCING_ZONE",
        "OPERATOR",
        "LEASE",
        "FIELD",
        "source_file",
        "MONTH-YEAR",
        "PRODUCT",
    ]
    available_meta = [c for c in meta_cols if c in df.columns]

    oil_df = df[df["PRODUCT"] == "O"].copy()
    gas_df = df[df["PRODUCT"] == "G"].copy()

    merge_keys = ["LEASE_KID", "production_date"]

    if len(oil_df) > 0:
        oil_pivot = oil_df[
            merge_keys + ["PRODUCTION"] + [c for c in available_meta if c not in merge_keys]
        ].copy()
        oil_pivot = oil_pivot.rename(columns={"PRODUCTION": "oil_bbl"})
    else:
        oil_pivot = pd.DataFrame(columns=merge_keys + ["oil_bbl"])

    if len(gas_df) > 0:
        gas_pivot = gas_df[merge_keys + ["PRODUCTION"]].copy()
        gas_pivot = gas_pivot.rename(columns={"PRODUCTION": "gas_mcf"})
    else:
        gas_pivot = pd.DataFrame(columns=merge_keys + ["gas_mcf"])

    # Outer merge
    if len(oil_pivot) > 0 and len(gas_pivot) > 0:
        merged = oil_pivot.merge(gas_pivot, on=merge_keys, how="outer")
    elif len(oil_pivot) > 0:
        merged = oil_pivot.copy()
        merged["gas_mcf"] = 0.0
    elif len(gas_pivot) > 0:
        # Gas-only lease: add metadata from gas rows
        gas_with_meta = gas_df[
            merge_keys + ["PRODUCTION"] + [c for c in available_meta if c not in merge_keys]
        ].copy()
        gas_with_meta = gas_with_meta.rename(columns={"PRODUCTION": "gas_mcf"})
        merged = gas_with_meta
        merged["oil_bbl"] = 0.0
    else:
        merged = pd.DataFrame(columns=merge_keys + ["oil_bbl", "gas_mcf"])

    merged["oil_bbl"] = merged["oil_bbl"].fillna(0.0)
    merged["gas_mcf"] = merged["gas_mcf"].fillna(0.0)
    merged["water_bbl"] = np.nan

    result = dd.from_pandas(merged.reset_index(drop=True), npartitions=1)
    n = result.npartitions
    return result.repartition(npartitions=min(n, MAX_READ_PARTITIONS))


# ---------------------------------------------------------------------------
# Task 03: Cumulative production features
# ---------------------------------------------------------------------------


def compute_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative production for a single-lease sorted DataFrame.

    Args:
        df: Single-lease sorted time-series DataFrame.

    Returns:
        DataFrame with cum_oil, cum_gas, cum_water columns added.
    """
    df = df.sort_values("production_date").copy()
    df["cum_oil"] = df["oil_bbl"].cumsum()
    df["cum_gas"] = df["gas_mcf"].cumsum()

    if "water_bbl" in df.columns and df["water_bbl"].notna().any():
        df["cum_water"] = df["water_bbl"].cumsum(skipna=True)
    else:
        df["cum_water"] = np.nan

    cum_oil_diff = df["cum_oil"].diff().dropna()
    assert (cum_oil_diff >= -1e-9).all(), "cum_oil is not monotonically non-decreasing"
    return df


# ---------------------------------------------------------------------------
# Task 04: GOR and water cut
# ---------------------------------------------------------------------------


def compute_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Compute GOR and water cut for a single-lease DataFrame.

    GOR = gas_mcf / oil_bbl (NaN when oil_bbl=0 and gas_mcf>0, 0.0 when both=0).
    water_cut = water_bbl / (oil_bbl + water_bbl) (NaN when denominator=0).

    Args:
        df: Single-lease DataFrame with oil_bbl, gas_mcf, water_bbl.

    Returns:
        DataFrame with gor and water_cut columns added.
    """
    df = df.copy()

    oil = df["oil_bbl"].astype(float)
    gas = df["gas_mcf"].astype(float)

    gor = np.where(
        oil > 0,
        gas / oil,
        np.where(gas > 0, np.nan, 0.0),
    )
    df["gor"] = gor

    if "water_bbl" in df.columns:
        water = df["water_bbl"].astype(float)
        denom = oil + water
        water_cut = np.where(denom > 0, water / denom, np.nan)
        # Clip to [0, 1] — values outside indicate data error
        water_cut = np.where((water_cut >= 0) & (water_cut <= 1), water_cut, np.nan)
        df["water_cut"] = water_cut
    else:
        df["water_cut"] = np.nan

    return df


# ---------------------------------------------------------------------------
# Task 05: Decline rate feature
# ---------------------------------------------------------------------------


def compute_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Compute period-over-period oil decline rate.

    Formula: (oil_bbl[t-1] - oil_bbl[t]) / oil_bbl[t-1], clipped to [-1, 10].

    Args:
        df: Single-lease sorted DataFrame with oil_bbl.

    Returns:
        DataFrame with decline_rate column added.
    """
    df = df.copy()
    oil = df["oil_bbl"].astype(float).values
    n = len(oil)
    decline = np.full(n, np.nan)

    for t in range(1, n):
        prev = oil[t - 1]
        curr = oil[t]
        if prev == 0:
            if curr == 0:
                decline[t] = 0.0
            else:
                decline[t] = -1.0
        else:
            raw = (prev - curr) / prev
            decline[t] = np.clip(raw, -1.0, 10.0)

    df["decline_rate"] = decline
    return df


# ---------------------------------------------------------------------------
# Task 06: Well age feature
# ---------------------------------------------------------------------------


def compute_well_age(df: pd.DataFrame) -> pd.DataFrame:
    """Compute well_age_months since first production date for the lease.

    Args:
        df: Single-lease sorted DataFrame with production_date.

    Returns:
        DataFrame with well_age_months (int) column added.
    """
    df = df.copy()
    first_date = df["production_date"].min()
    df["well_age_months"] = (
        df["production_date"]
        .apply(
            lambda d: (
                (d.year - first_date.year) * 12 + (d.month - first_date.month)
                if pd.notna(d) and pd.notna(first_date)
                else pd.NA
            )
        )
        .astype("Int64")
    )
    return df


# ---------------------------------------------------------------------------
# Task 07: Rolling average features
# ---------------------------------------------------------------------------


def compute_rolling(df: pd.DataFrame, windows: list[int] | None = None) -> pd.DataFrame:
    """Compute rolling averages for oil_bbl, gas_mcf, water_bbl.

    Args:
        df: Single-lease sorted DataFrame.
        windows: Rolling window sizes. Defaults to [3, 6].

    Returns:
        DataFrame with rolling columns added.
    """
    if windows is None:
        windows = [3, 6]
    df = df.copy()
    for col, prefix in [("oil_bbl", "oil"), ("gas_mcf", "gas"), ("water_bbl", "water")]:
        if col in df.columns:
            series = df[col].astype(float)
            for w in windows:
                df[f"{prefix}_roll{w}"] = series.rolling(window=w, min_periods=1).mean()
    return df


# ---------------------------------------------------------------------------
# Task 08: Lag features
# ---------------------------------------------------------------------------


def compute_lags(df: pd.DataFrame, lags: list[int] | None = None) -> pd.DataFrame:
    """Compute lag features for oil_bbl and gas_mcf.

    Args:
        df: Single-lease sorted DataFrame.
        lags: Lag periods. Defaults to [1, 3].

    Returns:
        DataFrame with lag columns added.
    """
    if lags is None:
        lags = [1, 3]
    df = df.copy()
    for col, prefix in [("oil_bbl", "oil"), ("gas_mcf", "gas")]:
        if col in df.columns:
            series = df[col].astype(float)
            for lag in lags:
                df[f"{prefix}_lag{lag}"] = series.shift(lag)
    return df


# ---------------------------------------------------------------------------
# Task 09: Aggregate features
# ---------------------------------------------------------------------------


def compute_aggregates(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute county and formation aggregate oil statistics.

    Args:
        ddf: Input Dask DataFrame with COUNTY, PRODUCING_ZONE, oil_bbl.

    Returns:
        Dask DataFrame with county_mean_oil, county_std_oil,
        formation_mean_oil, formation_std_oil columns added.
    """
    df = ddf[["COUNTY", "PRODUCING_ZONE", "oil_bbl"]].compute()

    county_agg = df.groupby("COUNTY")["oil_bbl"].agg(["mean", "std"]).reset_index()
    county_agg.columns = ["COUNTY", "county_mean_oil", "county_std_oil"]
    county_agg["county_std_oil"] = county_agg["county_std_oil"].fillna(0.0)

    formation_agg = df.groupby("PRODUCING_ZONE")["oil_bbl"].agg(["mean", "std"]).reset_index()
    formation_agg.columns = ["PRODUCING_ZONE", "formation_mean_oil", "formation_std_oil"]
    formation_agg["formation_std_oil"] = formation_agg["formation_std_oil"].fillna(0.0)

    global_oil_mean = df["oil_bbl"].mean()

    def _merge_aggs(part: pd.DataFrame) -> pd.DataFrame:
        part = part.merge(county_agg, on="COUNTY", how="left")
        part = part.merge(formation_agg, on="PRODUCING_ZONE", how="left")
        part["county_mean_oil"] = part["county_mean_oil"].fillna(global_oil_mean)
        part["county_std_oil"] = part["county_std_oil"].fillna(0.0)
        part["formation_mean_oil"] = part["formation_mean_oil"].fillna(global_oil_mean)
        part["formation_std_oil"] = part["formation_std_oil"].fillna(0.0)
        return part

    meta = ddf._meta.copy()
    for col in ["county_mean_oil", "county_std_oil", "formation_mean_oil", "formation_std_oil"]:
        meta[col] = pd.Series(dtype="float64")

    return ddf.map_partitions(_merge_aggs, meta=meta)


# ---------------------------------------------------------------------------
# Task 10: Categorical encoding
# ---------------------------------------------------------------------------


def encode_categoricals(ddf: dd.DataFrame) -> dd.DataFrame:
    """Label-encode COUNTY, PRODUCING_ZONE, OPERATOR, PRODUCT columns.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with _enc columns appended (original columns retained).
    """
    encode_cols = ["COUNTY", "PRODUCING_ZONE", "OPERATOR", "PRODUCT"]
    available = [c for c in encode_cols if c in ddf.columns]

    df = ddf[available].compute()
    # Build mapping from categorical values to codes for each column
    mappings: dict[str, dict[str, int]] = {}
    for col in available:
        # Use pandas category codes to create a mapping
        cat_series = pd.Categorical(df[col].astype(str).fillna("UNKNOWN"))
        # Create a mapping from category values to codes
        code_map = {cat: code for code, cat in enumerate(cat_series.categories)}
        mappings[col] = code_map

    def _encode_partition(part: pd.DataFrame) -> pd.DataFrame:
        for col, code_map in mappings.items():
            if col not in part.columns:
                continue
            vals = part[col].astype(str).fillna("UNKNOWN")
            # Use map with default -1 for unseen values
            part[f"{col}_enc"] = vals.map(code_map).fillna(-1).astype("int64")
        return part

    meta = ddf._meta.copy()
    for col in available:
        meta[f"{col}_enc"] = pd.Series(dtype="int64")

    return ddf.map_partitions(_encode_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 11: Per-lease feature pipeline dispatcher
# ---------------------------------------------------------------------------


def compute_per_lease_features(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all per-lease time-series feature functions.

    Chains: sort → cumulative → ratios → decline_rate → well_age → rolling → lags.

    Args:
        df: Rows for one LEASE_KID, sorted by production_date.

    Returns:
        DataFrame with all feature columns added.
    """
    lease_id = df["LEASE_KID"].iloc[0] if (len(df) > 0 and "LEASE_KID" in df.columns) else "unknown"
    try:
        df = df.sort_values("production_date").reset_index(drop=True)
        df = compute_cumulative(df)
        df = compute_ratios(df)
        df = compute_decline_rate(df)
        df = compute_well_age(df)
        df = compute_rolling(df)
        df = compute_lags(df)
        return df
    except Exception as exc:  # noqa: BLE001
        logger.warning("Per-lease feature computation failed for LEASE_KID=%s: %s", lease_id, exc)
        return df


# ---------------------------------------------------------------------------
# Task 12: Features orchestrator
# ---------------------------------------------------------------------------


def _build_per_lease_meta(sample_df: pd.DataFrame) -> pd.DataFrame:
    """Build meta DataFrame for groupby.apply result."""
    meta = sample_df.copy()
    feature_cols = {
        "cum_oil": "float64",
        "cum_gas": "float64",
        "cum_water": "float64",
        "gor": "float64",
        "water_cut": "float64",
        "decline_rate": "float64",
        "well_age_months": "Int64",
        "oil_roll3": "float64",
        "oil_roll6": "float64",
        "gas_roll3": "float64",
        "gas_roll6": "float64",
        "water_roll3": "float64",
        "water_roll6": "float64",
        "oil_lag1": "float64",
        "oil_lag3": "float64",
        "gas_lag1": "float64",
        "gas_lag3": "float64",
    }
    for col, dtype in feature_cols.items():
        if col not in meta.columns:
            meta[col] = pd.Series(dtype=dtype)
    return meta.iloc[:0]


def run_features(clean_dir: str, output_dir: str) -> dd.DataFrame:
    """Main entry point for the features stage.

    Args:
        clean_dir: Path to data/processed/clean/.
        output_dir: Path to data/processed/features/.

    Returns:
        ML-ready Dask DataFrame (not yet computed).
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ddf = load_clean(clean_dir)
    ddf = pivot_products(ddf)

    # Apply per-lease features: compute fully then reconvert.
    # We iterate over groups manually to ensure LEASE_KID column is preserved
    # across all pandas versions (groupby.apply may drop group key columns).
    df_computed = ddf.compute()
    if "LEASE_KID" in df_computed.columns:
        groups = []
        for lease_id, group_df in df_computed.groupby("LEASE_KID", sort=False):
            # Ensure LEASE_KID column is present inside the group
            if "LEASE_KID" not in group_df.columns:
                group_df = group_df.copy()
                group_df["LEASE_KID"] = lease_id
            groups.append(compute_per_lease_features(group_df))
        df_featured = pd.concat(groups, ignore_index=True) if groups else df_computed
    else:
        df_featured = compute_per_lease_features(df_computed)

    ddf = dd.from_pandas(df_featured, npartitions=max(1, len(df_featured) // 500_000))
    n = ddf.npartitions
    ddf = ddf.repartition(npartitions=min(n, 50))

    ddf = compute_aggregates(ddf)
    ddf = encode_categoricals(ddf)

    # Estimate partitions without full compute
    try:
        estimated_rows = ddf.npartitions * 500_000
    except Exception:  # noqa: BLE001
        estimated_rows = 500_000

    n_partitions = max(1, min(estimated_rows // 500_000, 200))
    ddf = ddf.repartition(npartitions=n_partitions)
    ddf.to_parquet(output_dir, write_index=False)

    ddf_features = dd.read_parquet(output_dir)
    n_out = ddf_features.npartitions
    ddf_features = ddf_features.repartition(npartitions=min(n_out, 50))

    logger.info("Features stage complete. Output: %s", output_dir)
    return ddf_features


if __name__ == "__main__":
    import argparse

    from kgs_pipeline.config import CLEAN_DIR, FEATURES_DIR

    parser = argparse.ArgumentParser(description="KGS features stage")
    parser.add_argument("--clean-dir", default=str(CLEAN_DIR))
    parser.add_argument("--output-dir", default=str(FEATURES_DIR))
    args = parser.parse_args()

    run_features(args.clean_dir, args.output_dir)
