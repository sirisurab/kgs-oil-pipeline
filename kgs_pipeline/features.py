"""Features component for the KGS oil production data pipeline.

Responsibilities:
- Load processed Parquet from data/processed/.
- Engineer ML-ready features per well: cumulative production (Np/Gp),
  decline rate, rolling statistics, lag features, time features,
  GOR/water-cut/WOR ratios, and categorical encoding.
- Write feature Parquet to data/processed/features/ with fixed pyarrow schema.
- Export a sampled feature matrix CSV.

Note: water_bbl not available in KGS dataset; water_cut and wor are NaN
placeholders reserved for future enrichment.

Two permitted .compute() calls:
  1. build_encoding_maps() — to compute global unique categorical values.
  2. write_features_parquet() — triggered via to_parquet().
"""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]

from kgs_pipeline.config import CONFIG
from kgs_pipeline.utils import ensure_dir, setup_logging

logger = setup_logging("features", CONFIG.logs_dir, CONFIG.log_level)

# Fixed features Parquet schema
FEATURES_SCHEMA = pa.schema(
    [
        pa.field("well_id", pa.string()),
        pa.field("production_date", pa.timestamp("ns")),
        pa.field("product", pa.string()),
        pa.field("production", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("lease_kid", pa.int64()),
        pa.field("operator", pa.string()),
        pa.field("county", pa.string()),
        pa.field("field", pa.string()),
        pa.field("producing_zone", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("cum_production", pa.float64()),
        pa.field("decline_rate", pa.float64()),
        pa.field("rolling_mean_3m", pa.float64()),
        pa.field("rolling_mean_6m", pa.float64()),
        pa.field("rolling_mean_12m", pa.float64()),
        pa.field("rolling_std_3m", pa.float64()),
        pa.field("rolling_std_6m", pa.float64()),
        pa.field("lag_1m", pa.float64()),
        pa.field("lag_3m", pa.float64()),
        pa.field("months_since_first_prod", pa.int64()),
        pa.field("production_year", pa.int32()),
        pa.field("production_month", pa.int32()),
        pa.field("production_quarter", pa.int32()),
        pa.field("gor", pa.float64()),
        pa.field("water_cut", pa.float64()),
        pa.field("wor", pa.float64()),
        pa.field("county_encoded", pa.int32()),
        pa.field("operator_encoded", pa.int32()),
        pa.field("producing_zone_encoded", pa.int32()),
        pa.field("field_encoded", pa.int32()),
        pa.field("product_encoded", pa.int32()),
        pa.field("outlier_flag", pa.bool_()),
    ]
)

CATEGORICAL_COLS = ["county", "operator", "producing_zone", "field", "product"]


def load_processed_parquet(processed_dir: Path | None = None) -> dd.DataFrame:
    """Load all processed Parquet files as a lazy Dask DataFrame.

    Args:
        processed_dir: Directory with processed Parquet (default: CONFIG.processed_dir).

    Returns:
        Lazy Dask DataFrame.

    Raises:
        FileNotFoundError: If processed_dir does not exist.
        RuntimeError: If no Parquet files are found.
    """
    processed_dir = processed_dir or CONFIG.processed_dir
    if not processed_dir.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")

    parquet_files = list(processed_dir.glob("**/*.parquet"))
    if not parquet_files:
        raise RuntimeError("No Parquet files found in processed_dir; run transform pipeline first.")

    logger.info("Loading %d Parquet files from %s", len(parquet_files), processed_dir)
    ddf = dd.read_parquet(str(processed_dir), engine="pyarrow")
    logger.info("Loaded processed Parquet with %d partitions.", ddf.npartitions)
    return ddf


def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add cum_production column: cumulative production per [well_id, product].

    cum_production is monotonically non-decreasing (flat during shut-in months).

    Args:
        ddf: Lazy Dask DataFrame with production, well_id, product columns.

    Returns:
        Lazy Dask DataFrame with cum_production column added.

    Raises:
        KeyError: If required columns are absent.
    """
    for col in ["production", "well_id", "product"]:
        if col not in ddf.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame")

    def _cum_prod_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["well_id", "product", "production_date"]).copy()
        df["cum_production"] = df.groupby(["well_id", "product"])["production"].cumsum()
        return df

    meta = ddf._meta.copy()
    meta["cum_production"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_cum_prod_partition, meta=meta)


def compute_decline_rate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add decline_rate column: month-over-month decline per [well_id, product].

    Decline = (prod[t] - prod[t-1]) / prod[t-1], clipped to [-1.0, 10.0].
    When prod[t-1] == 0: decline_rate = NaN (avoid division by zero).
    First row per group: NaN.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Lazy Dask DataFrame with decline_rate column.

    Raises:
        KeyError: If required columns are absent.
    """
    for col in ["production", "well_id", "product"]:
        if col not in ddf.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame")

    def _decline_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["well_id", "product", "production_date"]).copy()

        def _group_decline(g: pd.Series) -> pd.Series:
            lag = g.shift(1)
            # Zero denominator → NaN before any division
            safe_lag = lag.where(lag != 0.0, other=np.nan)
            raw = (g - lag) / safe_lag
            return raw.clip(
                lower=CONFIG.decline_rate_clip_min,
                upper=CONFIG.decline_rate_clip_max,
            )

        df["decline_rate"] = df.groupby(["well_id", "product"])["production"].transform(
            _group_decline
        )
        return df

    meta = ddf._meta.copy()
    meta["decline_rate"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_decline_partition, meta=meta)


def compute_rolling_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add rolling mean and std columns per [well_id, product].

    Windows from CONFIG.rolling_windows (default: [3, 6, 12]).
    rolling_mean_{w}m: min_periods=1
    rolling_std_{w}m: min_periods=2

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Lazy Dask DataFrame with rolling feature columns added.

    Raises:
        KeyError: If required columns are absent.
    """
    for col in ["production", "well_id", "product"]:
        if col not in ddf.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame")

    windows = CONFIG.rolling_windows

    def _rolling_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["well_id", "product", "production_date"]).copy()
        for w in windows:
            mean_col = f"rolling_mean_{w}m"
            std_col = f"rolling_std_{w}m"
            df[mean_col] = df.groupby(["well_id", "product"])["production"].transform(
                lambda s, _w=w: s.rolling(window=_w, min_periods=1).mean()
            )
            df[std_col] = df.groupby(["well_id", "product"])["production"].transform(
                lambda s, _w=w: s.rolling(window=_w, min_periods=2).std()
            )
        return df

    meta = ddf._meta.copy()
    for w in windows:
        meta[f"rolling_mean_{w}m"] = pd.Series(dtype="float64")
        meta[f"rolling_std_{w}m"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_rolling_partition, meta=meta)


def compute_lag_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add lag_1m and lag_3m production features per [well_id, product].

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Lazy Dask DataFrame with lag_1m and lag_3m columns.

    Raises:
        KeyError: If required columns are absent.
    """
    for col in ["production", "well_id", "product"]:
        if col not in ddf.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame")

    def _lag_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["well_id", "product", "production_date"]).copy()
        df["lag_1m"] = df.groupby(["well_id", "product"])["production"].transform(
            lambda s: s.shift(1)
        )
        df["lag_3m"] = df.groupby(["well_id", "product"])["production"].transform(
            lambda s: s.shift(3)
        )
        return df

    meta = ddf._meta.copy()
    meta["lag_1m"] = pd.Series(dtype="float64")
    meta["lag_3m"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_lag_partition, meta=meta)


def compute_time_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add time-based features from production_date.

    Adds: production_year, production_month, production_quarter,
    months_since_first_prod.

    Args:
        ddf: Lazy Dask DataFrame with production_date and well_id columns.

    Returns:
        Lazy Dask DataFrame with time feature columns.

    Raises:
        KeyError: If production_date or well_id columns are absent.
    """
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not found in DataFrame")
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column not found in DataFrame")

    def _time_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        dt = pd.to_datetime(df["production_date"])
        df["production_year"] = dt.dt.year.astype("Int32")
        df["production_month"] = dt.dt.month.astype("Int32")
        df["production_quarter"] = dt.dt.quarter.astype("Int32")

        def _months_since(g: pd.Series) -> pd.Series:
            first = g.dropna().min()
            if pd.isna(first):
                return pd.Series(np.nan, index=g.index)
            result = g.map(
                lambda d: (
                    int((d.year - first.year) * 12 + (d.month - first.month))
                    if pd.notna(d)
                    else np.nan
                )
            )
            return result

        df["months_since_first_prod"] = (
            df.groupby(["well_id", "product"])["production_date"]
            .transform(_months_since)
            .astype("Int64")
        )
        return df

    meta = ddf._meta.copy()
    meta["production_year"] = pd.Series(dtype="Int32")
    meta["production_month"] = pd.Series(dtype="Int32")
    meta["production_quarter"] = pd.Series(dtype="Int32")
    meta["months_since_first_prod"] = pd.Series(dtype="Int64")
    return ddf.map_partitions(_time_partition, meta=meta)


def compute_ratio_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add GOR, water_cut, and WOR ratio features.

    GOR = gas_mcf / oil_bbl per well per month (NaN when oil==0 and gas>0).
    water_cut and wor are NaN placeholders (no water data in KGS dataset).

    # water_bbl not available in KGS dataset; reserved for future enrichment

    Args:
        ddf: Lazy Dask DataFrame with product, production, well_id, production_date.

    Returns:
        Lazy Dask DataFrame with gor, water_cut, wor columns.
    """

    def _ratio_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Separate oil and gas rows
        oil = df[df["product"] == "O"][["well_id", "production_date", "production"]].rename(
            columns={"production": "oil_bbl"}
        )
        gas = df[df["product"] == "G"][["well_id", "production_date", "production"]].rename(
            columns={"production": "gas_mcf"}
        )

        gor_df = oil.merge(gas, on=["well_id", "production_date"], how="left")

        # GOR rules:
        # oil > 0, gas >= 0 → gas/oil
        # oil == 0, gas > 0 → NaN
        # oil == 0, gas == 0 → NaN
        # oil > 0, gas == 0 → 0.0
        conditions = [
            (gor_df["oil_bbl"] > 0),
            (gor_df["oil_bbl"] == 0),
        ]
        choices_num = [
            gor_df["gas_mcf"].fillna(0.0) / gor_df["oil_bbl"],
            np.nan,
        ]
        gor_df["gor"] = np.select(conditions, choices_num, default=np.nan)
        # When oil > 0 and gas == 0 → GOR = 0.0 (already handled by 0/oil = 0)
        # When oil == 0 → NaN (already set)

        # Merge gor back onto full df
        df = df.merge(
            gor_df[["well_id", "production_date", "gor"]],
            on=["well_id", "production_date"],
            how="left",
        )

        # Gas rows get NaN gor
        df.loc[df["product"] == "G", "gor"] = np.nan

        # water_bbl not available in KGS dataset; reserved for future enrichment
        df["water_cut"] = np.nan
        df["wor"] = np.nan
        return df

    meta = ddf._meta.copy()
    meta["gor"] = pd.Series(dtype="float64")
    meta["water_cut"] = pd.Series(dtype="float64")
    meta["wor"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_ratio_partition, meta=meta)


def build_encoding_maps(ddf: dd.DataFrame) -> dict[str, dict[str, int]]:
    """Build globally consistent integer encoding maps for categorical columns.

    Calls .compute() once to materialise unique values from the full dataset.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Dict mapping column name → {value: int_code} (alphabetically sorted).
    """
    encoding_maps: dict[str, dict[str, int]] = {}
    for col in CATEGORICAL_COLS:
        if col not in ddf.columns:
            logger.warning("Categorical column '%s' not found; skipping encoding.", col)
            continue
        unique_vals = sorted(str(v) for v in ddf[col].dropna().unique().compute() if pd.notna(v))
        encoding_maps[col] = {v: i for i, v in enumerate(unique_vals)}
    return encoding_maps


def encode_categorical_features(
    ddf: dd.DataFrame,
    encoding_maps: dict[str, dict[str, int]],
) -> dd.DataFrame:
    """Apply integer encoding to categorical columns using precomputed maps.

    - county_encoded, operator_encoded, producing_zone_encoded, field_encoded: from maps.
    - product_encoded: O→0, G→1 (fixed, not from map).
    - Unseen values map to -1.

    Args:
        ddf: Lazy Dask DataFrame.
        encoding_maps: Dict of {col: {value: int_code}}.

    Returns:
        Lazy Dask DataFrame with *_encoded columns added.
    """

    def _encode_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in ["county", "operator", "producing_zone", "field"]:
            enc_col = f"{col}_encoded"
            if col not in df.columns:
                logger.warning("Column '%s' absent; skipping encoding.", col)
                df[enc_col] = -1
                continue
            col_map = encoding_maps.get(col, {})
            df[enc_col] = df[col].map(col_map).fillna(-1).astype("int32")

        # Fixed product encoding
        if "product" in df.columns:
            df["product_encoded"] = df["product"].map({"O": 0, "G": 1}).fillna(-1).astype("int32")
        else:
            df["product_encoded"] = -1
        return df

    meta = ddf._meta.copy()
    for col in ["county", "operator", "producing_zone", "field"]:
        meta[f"{col}_encoded"] = pd.Series(dtype="int32")
    meta["product_encoded"] = pd.Series(dtype="int32")
    return ddf.map_partitions(_encode_partition, meta=meta)


def write_features_parquet(
    ddf: dd.DataFrame,
    output_dir: Path | None = None,
) -> list[Path]:
    """Write ML-ready feature Dask DataFrame to Parquet partitioned by well_id.

    This is one of the two permitted .compute() calls.

    Args:
        ddf: Typed lazy Dask DataFrame with all feature columns.
        output_dir: Output directory (default: CONFIG.features_dir).

    Returns:
        Sorted list of written .parquet file paths.
    """
    output_dir = output_dir or CONFIG.features_dir
    ensure_dir(output_dir)

    # Add missing schema columns with default values
    for field in FEATURES_SCHEMA:
        if field.name not in ddf.columns:
            if field.type == pa.bool_():
                ddf = ddf.assign(**{field.name: False})
            elif field.type in (pa.int32(), pa.int64()):
                ddf = ddf.assign(**{field.name: pd.NA})
            elif field.type == pa.string():
                ddf = ddf.assign(**{field.name: None})
            else:
                ddf = ddf.assign(**{field.name: float("nan")})

    try:
        ddf.to_parquet(
            str(output_dir),
            engine="pyarrow",
            write_index=False,
            schema=FEATURES_SCHEMA,
            overwrite=True,
        )
    except Exception as exc:
        logger.error("Failed to write features Parquet: %s", exc)
        raise

    written = sorted(output_dir.glob("**/*.parquet"))
    logger.info("Wrote %d feature Parquet files to %s", len(written), output_dir)
    return written


def export_feature_matrix_csv(
    ddf: dd.DataFrame,
    output_dir: Path | None = None,
) -> Path:
    """Export up to 100,000 rows of the feature DataFrame as CSV.

    Args:
        ddf: Lazy Dask DataFrame.
        output_dir: Output directory (default: CONFIG.features_dir).

    Returns:
        Path to the written CSV file.
    """
    output_dir = output_dir or CONFIG.features_dir
    ensure_dir(output_dir)

    csv_path = output_dir / "feature_matrix.csv"
    sample = ddf.head(100000, compute=True, npartitions=-1)
    sample.to_csv(csv_path, index=False)
    logger.info("Feature matrix CSV written to %s (%d rows)", csv_path, len(sample))
    return csv_path


def run_features_pipeline(
    processed_dir: Path | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Orchestrate the full features engineering workflow.

    Args:
        processed_dir: Processed Parquet input (default: CONFIG.processed_dir).
        output_dir: Features Parquet output (default: CONFIG.features_dir).

    Returns:
        List of written feature Parquet file paths.

    Raises:
        RuntimeError: If no processed Parquet files are found (propagated).
    """
    ddf = load_processed_parquet(processed_dir)
    ddf = compute_cumulative_production(ddf)
    ddf = compute_decline_rate(ddf)
    ddf = compute_rolling_features(ddf)
    ddf = compute_lag_features(ddf)
    ddf = compute_time_features(ddf)
    ddf = compute_ratio_features(ddf)

    encoding_maps = build_encoding_maps(ddf)
    ddf = encode_categorical_features(ddf, encoding_maps)

    written = write_features_parquet(ddf, output_dir)
    export_feature_matrix_csv(ddf, output_dir)

    logger.info("Features pipeline complete. %d files written.", len(written))
    return written


if __name__ == "__main__":
    paths = run_features_pipeline()
    print(f"Features pipeline wrote {len(paths)} files.")
