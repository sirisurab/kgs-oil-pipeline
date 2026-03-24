"""Features component — ML-ready feature engineering for well production data."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]

import kgs_pipeline.config as config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task 23: Cumulative production
# ---------------------------------------------------------------------------


def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add cumulative_production column (running total per well).

    NaN production values are treated as 0 for the cumsum, but the original
    production column retains NaN.

    Args:
        ddf: Lazy Dask DataFrame with production and production_date columns.

    Returns:
        Lazy Dask DataFrame with cumulative_production column added.

    Raises:
        KeyError: If production or production_date columns are absent.
    """
    if "production" not in ddf.columns:
        raise KeyError("Required column 'production' is absent")
    if "production_date" not in ddf.columns:
        raise KeyError("Required column 'production_date' is absent")

    def _cumsum_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values(
            ["well_id", "production_date"] if "well_id" in df.columns else ["production_date"]
        )
        df["cumulative_production"] = df["production"].fillna(0.0).cumsum().astype("float64")
        return df

    meta = ddf._meta.copy()
    meta["cumulative_production"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_cumsum_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 24: Production decline rate
# ---------------------------------------------------------------------------


def compute_decline_rate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add decline_rate column: month-over-month proportional production change.

    Clipped to [DECLINE_RATE_CLIP_MIN, DECLINE_RATE_CLIP_MAX].
    First row per well is NaN; any prior-month zero or NaN gives NaN.

    Args:
        ddf: Lazy Dask DataFrame with production and production_date columns.

    Returns:
        Lazy Dask DataFrame with decline_rate column added.

    Raises:
        KeyError: If production or production_date columns are absent.
    """
    if "production" not in ddf.columns:
        raise KeyError("Required column 'production' is absent")
    if "production_date" not in ddf.columns:
        raise KeyError("Required column 'production_date' is absent")

    clip_min = config.DECLINE_RATE_CLIP_MIN
    clip_max = config.DECLINE_RATE_CLIP_MAX

    def _decline_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values("production_date")
        prev = df["production"].shift(1)
        raw_rate = (df["production"] - prev) / prev
        # Where prev is 0 or NaN, result should be NaN
        raw_rate = raw_rate.where(prev.notna() & (prev != 0.0), other=np.nan)
        df["decline_rate"] = raw_rate.clip(lower=clip_min, upper=clip_max).astype("float64")
        return df

    meta = ddf._meta.copy()
    meta["decline_rate"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_decline_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 25: Rolling statistics
# ---------------------------------------------------------------------------


def compute_rolling_statistics(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add 3-month and 6-month rolling mean and std dev of production.

    Args:
        ddf: Lazy Dask DataFrame with production and production_date columns.

    Returns:
        Lazy Dask DataFrame with four rolling stat columns added.

    Raises:
        KeyError: If production or production_date columns are absent.
    """
    if "production" not in ddf.columns:
        raise KeyError("Required column 'production' is absent")
    if "production_date" not in ddf.columns:
        raise KeyError("Required column 'production_date' is absent")

    short = config.ROLLING_WINDOW_SHORT
    long = config.ROLLING_WINDOW_LONG

    def _rolling_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values("production_date")
        prod = df["production"]
        df["rolling_mean_3m"] = prod.rolling(window=short, min_periods=1).mean().astype("float64")
        df["rolling_std_3m"] = prod.rolling(window=short, min_periods=1).std().astype("float64")
        df["rolling_mean_6m"] = prod.rolling(window=long, min_periods=1).mean().astype("float64")
        df["rolling_std_6m"] = prod.rolling(window=long, min_periods=1).std().astype("float64")
        return df

    meta = ddf._meta.copy()
    for col in ("rolling_mean_3m", "rolling_std_3m", "rolling_mean_6m", "rolling_std_6m"):
        meta[col] = pd.Series(dtype="float64")
    return ddf.map_partitions(_rolling_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 26: Time-based features
# ---------------------------------------------------------------------------


def compute_time_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add months_since_first_prod, production_year, production_month columns.

    Args:
        ddf: Lazy Dask DataFrame with production_date column.

    Returns:
        Lazy Dask DataFrame with three time feature columns added.

    Raises:
        KeyError: If production_date column is absent.
    """
    if "production_date" not in ddf.columns:
        raise KeyError("Required column 'production_date' is absent")

    def _time_features_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        dates = pd.to_datetime(df["production_date"], errors="coerce")
        first_date = dates.dropna().min() if dates.notna().any() else pd.NaT

        if pd.isna(first_date):
            df["months_since_first_prod"] = pd.array([pd.NA] * len(df), dtype=pd.Int32Dtype())
            df["production_year"] = pd.array([pd.NA] * len(df), dtype=pd.Int32Dtype())
            df["production_month"] = pd.array([pd.NA] * len(df), dtype=pd.Int32Dtype())
        else:
            months = (dates.dt.year - first_date.year) * 12 + (dates.dt.month - first_date.month)
            df["months_since_first_prod"] = months.where(dates.notna(), other=pd.NA).astype(
                pd.Int32Dtype()
            )
            df["production_year"] = dates.dt.year.where(dates.notna(), other=pd.NA).astype(
                pd.Int32Dtype()
            )
            df["production_month"] = dates.dt.month.where(dates.notna(), other=pd.NA).astype(
                pd.Int32Dtype()
            )
        return df

    meta = ddf._meta.copy()
    for col in ("months_since_first_prod", "production_year", "production_month"):
        meta[col] = pd.array([], dtype=pd.Int32Dtype())
    return ddf.map_partitions(_time_features_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 27: Gas-oil ratio
# ---------------------------------------------------------------------------


def compute_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add gor (gas-oil ratio) column per (well_id, production_date) pair.

    GOR = gas_production / oil_production. NaN where oil is 0 or missing.

    Args:
        ddf: Lazy Dask DataFrame with product, production, well_id, production_date.

    Returns:
        Lazy Dask DataFrame with gor column added.

    Raises:
        KeyError: If any required column is absent.
    """
    required = ["product", "production", "well_id", "production_date"]
    missing = [c for c in required if c not in ddf.columns]
    if missing:
        raise KeyError(f"Missing required columns for GOR: {missing}")

    def _gor_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Pivot to get oil and gas side-by-side
        pivot_idx = ["well_id", "production_date"]
        # Only pivot if we have product column with O or G values
        if df.empty or "product" not in df.columns:
            df["gor"] = np.nan
            return df

        has_o = "O" in df["product"].values
        has_g = "G" in df["product"].values

        if not (has_o and has_g):
            df["gor"] = np.nan
            return df

        try:
            pivot = df.pivot_table(
                index=pivot_idx,
                columns="product",
                values="production",
                aggfunc="first",
            )
        except Exception:
            df["gor"] = np.nan
            return df

        pivot.columns = pivot.columns.astype(str)
        oil_col = pivot.get("O", pd.Series(np.nan, index=pivot.index))
        gas_col = pivot.get("G", pd.Series(np.nan, index=pivot.index))

        gor_series = gas_col / oil_col.replace(0.0, np.nan)
        gor_series = gor_series.rename("gor").reset_index()

        df = df.merge(gor_series, on=pivot_idx, how="left")
        return df

    meta = ddf._meta.copy()
    meta["gor"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_gor_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 28: Water cut and WOR placeholder columns
# ---------------------------------------------------------------------------


def add_water_placeholders(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add water_cut and wor columns as NaN float64 placeholders.

    Reserved for future integration when water production data is available.

    Args:
        ddf: Lazy Dask DataFrame.

    Returns:
        Lazy Dask DataFrame with water_cut and wor columns appended.
    """

    def _add_placeholders(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["water_cut"] = np.nan
        df["wor"] = np.nan
        df["water_cut"] = df["water_cut"].astype("float64")
        df["wor"] = df["wor"].astype("float64")
        return df

    meta = ddf._meta.copy()
    meta["water_cut"] = pd.Series(dtype="float64")
    meta["wor"] = pd.Series(dtype="float64")
    return ddf.map_partitions(_add_placeholders, meta=meta)


# ---------------------------------------------------------------------------
# Task 29: Categorical label encoding
# ---------------------------------------------------------------------------


def encode_categorical_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Label-encode categorical columns with globally consistent integer codes.

    For each column in CATEGORICAL_COLUMNS, creates a <col>_encoded column.
    Missing/null values (None, NaN, pd.NA) map to -1. Original columns are retained.

    Uses pd.factorize which natively assigns -1 to NA values, guaranteeing
    None/NaN → -1 regardless of pandas storage format.

    Args:
        ddf: Lazy Dask DataFrame with categorical columns.

    Returns:
        Lazy Dask DataFrame with all _encoded columns appended.
    """
    # Materialise once — encoding requires global label assignment anyway
    npartitions = ddf.npartitions
    pdf: pd.DataFrame = ddf.compute().copy()

    for col in config.CATEGORICAL_COLUMNS:
        if col not in pdf.columns:
            logger.warning("Categorical column '%s' absent; skipping encoding", col)
            continue

        # pd.factorize assigns integer codes; na_sentinel=-1 maps NA/None → -1
        codes, _ = pd.factorize(pdf[col], sort=True, use_na_sentinel=True)
        pdf[f"{col}_encoded"] = codes.astype("int32")
        logger.debug("Encoded '%s' with %d unique values", col, len(_))

    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Task 30: Features Parquet writer with schema enforcement
# ---------------------------------------------------------------------------

FEATURES_SCHEMA = pa.schema(
    [
        pa.field("lease_kid", pa.string()),
        pa.field("lease_name", pa.string()),
        pa.field("dor_code", pa.string()),
        pa.field("field_name", pa.string()),
        pa.field("producing_zone", pa.string()),
        pa.field("operator", pa.string()),
        pa.field("county", pa.string()),
        pa.field("township", pa.int32()),
        pa.field("twn_dir", pa.string()),
        pa.field("range_num", pa.int32()),
        pa.field("range_dir", pa.string()),
        pa.field("section", pa.int32()),
        pa.field("spot", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("product", pa.string()),
        pa.field("well_count", pa.int32()),
        pa.field("production", pa.float64()),
        pa.field("source_file", pa.string()),
        pa.field("production_date", pa.timestamp("ns")),
        pa.field("well_id", pa.string()),
        pa.field("outlier_flag", pa.bool_()),
        pa.field("unit", pa.string()),
        pa.field("cumulative_production", pa.float64()),
        pa.field("decline_rate", pa.float64()),
        pa.field("rolling_mean_3m", pa.float64()),
        pa.field("rolling_std_3m", pa.float64()),
        pa.field("rolling_mean_6m", pa.float64()),
        pa.field("rolling_std_6m", pa.float64()),
        pa.field("months_since_first_prod", pa.int32()),
        pa.field("production_year", pa.int32()),
        pa.field("production_month", pa.int32()),
        pa.field("gor", pa.float64()),
        pa.field("water_cut", pa.float64()),
        pa.field("wor", pa.float64()),
        pa.field("county_encoded", pa.int32()),
        pa.field("operator_encoded", pa.int32()),
        pa.field("producing_zone_encoded", pa.int32()),
        pa.field("field_name_encoded", pa.int32()),
        pa.field("product_encoded", pa.int32()),
    ]
)


def write_features_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """Write ML-ready feature Dask DataFrame to Parquet with schema enforcement.

    Args:
        ddf: Lazy Dask DataFrame with all feature columns.
        output_dir: Directory where Parquet files are written.

    Returns:
        output_dir as a Path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(output_dir),
            engine="pyarrow",
            schema=FEATURES_SCHEMA,
            write_index=True,
            overwrite=True,
        )
    except Exception as exc:
        logger.error("Schema mismatch writing features Parquet: %s", exc)
        raise

    parquet_files = list(output_dir.glob("*.parquet"))
    logger.info("Wrote features Parquet to %s (%d files)", output_dir, len(parquet_files))
    return output_dir


# ---------------------------------------------------------------------------
# Task 31: Features pipeline orchestrator
# ---------------------------------------------------------------------------


def run_features_pipeline() -> Path:
    """Orchestrate the full features engineering workflow end-to-end.

    Returns:
        Path to the features Parquet directory.

    Raises:
        RuntimeError: If PROCESSED_DATA_DIR contains no Parquet files.
    """
    from kgs_pipeline.transform import sort_and_repartition  # noqa: PLC0415

    start = time.time()
    logger.info("Features pipeline starting")

    processed_files = [
        f
        for f in config.PROCESSED_DATA_DIR.glob("*.parquet")
        if f.parent == config.PROCESSED_DATA_DIR  # exclude features/ subdirectory files
    ]
    if not processed_files:
        raise RuntimeError(
            f"No Parquet files found in PROCESSED_DATA_DIR: {config.PROCESSED_DATA_DIR}"
        )

    ddf = dd.read_parquet(str(config.PROCESSED_DATA_DIR), engine="pyarrow")

    # Ensure well_id is the index
    if ddf.index.name != "well_id":
        logger.info("Index is not well_id; calling sort_and_repartition")
        ddf = sort_and_repartition(ddf)

    logger.info("Step: compute_cumulative_production")
    ddf = compute_cumulative_production(ddf)

    logger.info("Step: compute_decline_rate")
    ddf = compute_decline_rate(ddf)

    logger.info("Step: compute_rolling_statistics")
    ddf = compute_rolling_statistics(ddf)

    logger.info("Step: compute_time_features")
    ddf = compute_time_features(ddf)

    logger.info("Step: compute_gor")
    ddf = compute_gor(ddf)

    logger.info("Step: add_water_placeholders")
    ddf = add_water_placeholders(ddf)

    logger.info("Step: encode_categorical_features")
    ddf = encode_categorical_features(ddf)

    logger.info("Step: write_features_parquet")
    output = write_features_parquet(ddf, config.FEATURES_DATA_DIR)

    elapsed = time.time() - start
    logger.info("Features pipeline complete in %.1fs — output: %s", elapsed, output)
    return output
