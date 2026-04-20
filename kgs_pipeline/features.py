"""Features stage — compute derived ML features from transformed data."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pyarrow.parquet as pq  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# Required output columns (FEA-08)
REQUIRED_FEATURE_COLUMNS: list[str] = [
    "LEASE_KID",
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
    "PRODUCT",
    "WELLS",
    "PRODUCTION",
    "production_date",
    "source_file",
    "cum_production",
    "gor",
    "water_cut",
    "decline_rate",
    "rolling_3m_production",
    "rolling_6m_production",
    "lag_1m_production",
    "lag_3m_production",
    "lag_6m_production",
    "product_code",
    "twn_dir_code",
    "range_dir_code",
]


# ---------------------------------------------------------------------------
# FEA-01: Cumulative production calculator
# ---------------------------------------------------------------------------


def add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_production column (cumulative sum per LEASE_KID+PRODUCT group).

    Args:
        df: Partition DataFrame sorted by production_date.

    Returns:
        DataFrame with ``cum_production`` column added.

    Raises:
        KeyError: If ``PRODUCTION`` or ``production_date`` are absent.
    """
    if "PRODUCTION" not in df.columns:
        raise KeyError("PRODUCTION column absent")
    if "production_date" not in df.columns:
        raise KeyError("production_date column absent")

    if df.empty:
        df = df.copy()
        df["cum_production"] = pd.Series(dtype="float64")
        return df

    group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]

    # Sort within groups
    df = df.sort_values(["production_date"] if not group_cols else group_cols + ["production_date"])
    prod = pd.to_numeric(df["PRODUCTION"], errors="coerce").fillna(0.0)

    if group_cols:
        df = df.copy()
        df["cum_production"] = prod.groupby([df[c] for c in group_cols], group_keys=False).cumsum()
    else:
        df = df.copy()
        df["cum_production"] = prod.cumsum()

    return df


# ---------------------------------------------------------------------------
# FEA-02: GOR and water cut calculator
# ---------------------------------------------------------------------------


def add_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot to wide format and add gor and water_cut columns.

    Args:
        df: Partition DataFrame.

    Returns:
        Wide-format DataFrame with ``gor`` and ``water_cut`` columns.
    """
    if df.empty:
        df = df.copy()
        df["gor"] = pd.Series(dtype="float64")
        df["water_cut"] = pd.Series(dtype="float64")
        return df

    df = df.copy()

    prod_num = pd.to_numeric(df["PRODUCTION"], errors="coerce").fillna(0.0)

    product_col = df.get("PRODUCT", pd.Series(["O"] * len(df), index=df.index))

    is_oil = product_col.astype(str) == "O"
    is_gas = product_col.astype(str) == "G"

    oil_bbl = prod_num.where(is_oil, other=0.0)
    gas_mcf = prod_num.where(is_gas, other=0.0)
    water_bbl = pd.Series(0.0, index=df.index)

    # GOR = gas_mcf / oil_bbl, with special cases
    gor = pd.Series(float("nan"), index=df.index)
    # oil > 0 and gas >= 0 → ratio
    normal = oil_bbl > 0
    gor = gor.where(~normal, gas_mcf / oil_bbl)
    # oil == 0 and gas == 0 → NaN (already NaN)
    # oil > 0 and gas == 0 → 0.0
    zero_gas_pos_oil = (oil_bbl > 0) & (gas_mcf == 0)
    gor = gor.where(~zero_gas_pos_oil, 0.0)
    # oil == 0 → NaN (already NaN)

    # Water cut = water_bbl / (oil_bbl + water_bbl)
    total_liquid = oil_bbl + water_bbl
    water_cut = pd.Series(float("nan"), index=df.index)
    # total > 0 → compute
    pos_total = total_liquid > 0
    water_cut = water_cut.where(~pos_total, water_bbl / total_liquid)
    # both zero → NaN (already NaN)

    # Validate water_cut in [0, 1]
    wc_valid = water_cut.dropna()
    if ((wc_valid < 0) | (wc_valid > 1)).any():
        raise ValueError("water_cut values outside [0, 1] detected")

    df["gor"] = gor
    df["water_cut"] = water_cut
    return df


# ---------------------------------------------------------------------------
# FEA-03: Decline rate calculator
# ---------------------------------------------------------------------------


def add_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate column clipped to [-1.0, 10.0].

    Args:
        df: Partition DataFrame.

    Returns:
        DataFrame with ``decline_rate`` column added.

    Raises:
        KeyError: If ``PRODUCTION`` or ``production_date`` are absent.
    """
    if "PRODUCTION" not in df.columns:
        raise KeyError("PRODUCTION column absent")
    if "production_date" not in df.columns:
        raise KeyError("production_date column absent")

    if df.empty:
        df = df.copy()
        df["decline_rate"] = pd.Series(dtype="float64")
        return df

    group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]
    df = df.sort_values(["production_date"] if not group_cols else group_cols + ["production_date"])
    df = df.copy()

    prod = pd.to_numeric(df["PRODUCTION"], errors="coerce")

    if group_cols:
        prev = prod.groupby([df[c] for c in group_cols], group_keys=False).shift(1)
    else:
        prev = prod.shift(1)

    # decline = (prod_t - prod_{t-1}) / prod_{t-1}
    raw_decline = (prod - prev) / prev

    # When prev == 0 → NaN (TR-07d)
    raw_decline = raw_decline.where(prev != 0, other=float("nan"))

    # Clip to [-1.0, 10.0]
    decline_clipped = raw_decline.clip(lower=-1.0, upper=10.0)

    df["decline_rate"] = decline_clipped
    return df


# ---------------------------------------------------------------------------
# FEA-04: Rolling statistics calculator
# ---------------------------------------------------------------------------


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 3-month and 6-month rolling mean of PRODUCTION per group.

    Args:
        df: Partition DataFrame sorted by production_date.

    Returns:
        DataFrame with ``rolling_3m_production`` and ``rolling_6m_production``.
    """
    if df.empty:
        df = df.copy()
        df["rolling_3m_production"] = pd.Series(dtype="float64")
        df["rolling_6m_production"] = pd.Series(dtype="float64")
        return df

    group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]
    df = df.sort_values(["production_date"] if not group_cols else group_cols + ["production_date"])
    df = df.copy()

    prod = pd.to_numeric(df["PRODUCTION"], errors="coerce")

    if group_cols:
        grp = prod.groupby([df[c] for c in group_cols], group_keys=False)
        df["rolling_3m_production"] = grp.apply(lambda s: s.rolling(window=3, min_periods=1).mean())
        df["rolling_6m_production"] = grp.apply(lambda s: s.rolling(window=6, min_periods=1).mean())
    else:
        df["rolling_3m_production"] = prod.rolling(window=3, min_periods=1).mean()
        df["rolling_6m_production"] = prod.rolling(window=6, min_periods=1).mean()

    return df


# ---------------------------------------------------------------------------
# FEA-05: Lag feature calculator
# ---------------------------------------------------------------------------


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 1-, 3-, and 6-month lag features of PRODUCTION per group.

    Args:
        df: Partition DataFrame sorted by production_date.

    Returns:
        DataFrame with lag columns added.
    """
    if df.empty:
        df = df.copy()
        for lag in (1, 3, 6):
            df[f"lag_{lag}m_production"] = pd.Series(dtype="float64")
        return df

    group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]
    df = df.sort_values(["production_date"] if not group_cols else group_cols + ["production_date"])
    df = df.copy()

    prod = pd.to_numeric(df["PRODUCTION"], errors="coerce")

    for lag in (1, 3, 6):
        if group_cols:
            df[f"lag_{lag}m_production"] = prod.groupby(
                [df[c] for c in group_cols], group_keys=False
            ).shift(lag)
        else:
            df[f"lag_{lag}m_production"] = prod.shift(lag)

    return df


# ---------------------------------------------------------------------------
# FEA-06: Categorical encoder
# ---------------------------------------------------------------------------


def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Encode PRODUCT, TWN_DIR, RANGE_DIR as integer codes.

    Args:
        df: Partition DataFrame with categorical columns.

    Returns:
        DataFrame with encoded columns added (originals preserved).
    """
    df = df.copy()

    # PRODUCT: G=0, O=1 (alphabetical)
    if "PRODUCT" in df.columns:
        product_cat = pd.Categorical(df["PRODUCT"], categories=["G", "O"])
        codes = product_cat.codes.astype("int64")
        df["product_code"] = codes
    else:
        df["product_code"] = pd.array([-1] * len(df), dtype="int64")

    # TWN_DIR: N=0, S=1; nulls → -1
    if "TWN_DIR" in df.columns:
        twn_cat = pd.Categorical(df["TWN_DIR"], categories=["N", "S"])
        codes_twn = twn_cat.codes.astype("int64")
        df["twn_dir_code"] = codes_twn  # -1 for unrecognised/null
    else:
        df["twn_dir_code"] = pd.array([-1] * len(df), dtype="int64")

    # RANGE_DIR: E=0, W=1; nulls → -1
    if "RANGE_DIR" in df.columns:
        rng_cat = pd.Categorical(df["RANGE_DIR"], categories=["E", "W"])
        codes_rng = rng_cat.codes.astype("int64")
        df["range_dir_code"] = codes_rng
    else:
        df["range_dir_code"] = pd.array([-1] * len(df), dtype="int64")

    return df


# ---------------------------------------------------------------------------
# FEA-07: Features map_partitions orchestrator
# ---------------------------------------------------------------------------


def apply_feature_transforms(ddf: dd.DataFrame) -> dd.DataFrame:
    """Chain all feature engineering functions lazily via map_partitions.

    Args:
        ddf: Input Dask DataFrame (entity-indexed, sorted by production_date).

    Returns:
        Dask DataFrame with all feature columns added (lazy).
    """
    meta1 = add_cumulative_production(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(add_cumulative_production, meta=meta1)

    meta2 = add_ratios(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(add_ratios, meta=meta2)

    meta3 = add_decline_rate(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(add_decline_rate, meta=meta3)

    meta4 = add_rolling_features(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(add_rolling_features, meta=meta4)

    meta5 = add_lag_features(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(add_lag_features, meta=meta5)

    meta6 = encode_categoricals(ddf._meta.copy())  # noqa: SLF001
    ddf = ddf.map_partitions(encode_categoricals, meta=meta6)

    return ddf


# ---------------------------------------------------------------------------
# FEA-08: Output schema validator
# ---------------------------------------------------------------------------


def validate_features_schema(ddf: dd.DataFrame) -> None:
    """Assert all required feature columns are present in the Dask DataFrame.

    Args:
        ddf: Features Dask DataFrame.

    Raises:
        ValueError: If any required column is missing.
    """
    present = set(ddf.columns)
    missing = [c for c in REQUIRED_FEATURE_COLUMNS if c not in present]
    if missing:
        raise ValueError(f"Features DataFrame missing required columns: {missing}")


# ---------------------------------------------------------------------------
# FEA-09: Schema stability checker
# ---------------------------------------------------------------------------


def check_schema_stability(processed_dir: Path) -> None:
    """Assert that two sampled Parquet partition files have identical schemas.

    Args:
        processed_dir: Directory of processed Parquet files.

    Raises:
        ValueError: If schemas differ between sampled partitions.
    """
    parquet_files = sorted(processed_dir.glob("*.parquet"))
    if len(parquet_files) < 2:
        return  # Nothing to compare

    first = parquet_files[0]
    last = parquet_files[-1]

    schema_a = pq.read_schema(first)
    schema_b = pq.read_schema(last)

    cols_a = {f.name: str(f.type) for f in schema_a}
    cols_b = {f.name: str(f.type) for f in schema_b}

    if cols_a != cols_b:
        raise ValueError(
            f"Schema mismatch between {first.name} and {last.name}: "
            f"only-in-first={set(cols_a) - set(cols_b)}, "
            f"only-in-last={set(cols_b) - set(cols_a)}, "
            f"type-diffs={[(k, cols_a[k], cols_b[k]) for k in cols_a if k in cols_b and cols_a[k] != cols_b[k]]}"
        )


# ---------------------------------------------------------------------------
# FEA-10: Features Parquet writer
# ---------------------------------------------------------------------------


def write_features(ddf: dd.DataFrame, config: dict) -> None:
    """Write ML-ready Parquet files after validating schema.

    Args:
        ddf: Features Dask DataFrame.
        config: Pipeline configuration dict.
    """
    output_dir = Path(config["features"]["processed_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    validate_features_schema(ddf)

    n = ddf.npartitions
    n_partitions = max(10, min(n, 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    try:
        ddf.to_parquet(str(output_dir), write_index=True, overwrite=True)
        logger.info("Wrote features Parquet to %s (%d partitions)", output_dir, n_partitions)
    except Exception as exc:
        logger.error("Failed to write features Parquet: %s", exc)
        raise

    check_schema_stability(output_dir)


# ---------------------------------------------------------------------------
# FEA-11: Feature correctness validator
# ---------------------------------------------------------------------------


def validate_feature_correctness(df: pd.DataFrame) -> list[str]:
    """Run domain correctness checks on a partition and return error messages.

    Args:
        df: Partition DataFrame with all feature columns.

    Returns:
        List of error strings (empty if all checks pass).
    """
    errors: list[str] = []

    if df.empty:
        return errors

    # TR-01: PRODUCTION >= 0
    if "PRODUCTION" in df.columns:
        prod = pd.to_numeric(df["PRODUCTION"], errors="coerce")
        neg = prod[prod.notna() & (prod < 0)]
        if not neg.empty:
            errors.append(f"PRODUCTION < 0 in {len(neg)} rows")

    # TR-01: GOR >= 0
    if "gor" in df.columns:
        gor = pd.to_numeric(df["gor"], errors="coerce")
        neg_gor = gor[gor.notna() & (gor < 0)]
        if not neg_gor.empty:
            errors.append(f"gor < 0 in {len(neg_gor)} rows")

    # TR-10: water_cut in [0, 1]
    if "water_cut" in df.columns:
        wc = pd.to_numeric(df["water_cut"], errors="coerce")
        out_of_range = wc[wc.notna() & ((wc < 0) | (wc > 1))]
        if not out_of_range.empty:
            errors.append(f"water_cut outside [0, 1] in {len(out_of_range)} rows")

    # TR-03: cum_production monotonically non-decreasing
    if "cum_production" in df.columns:
        group_cols = [c for c in ["LEASE_KID", "PRODUCT"] if c in df.columns]
        cum = pd.to_numeric(df["cum_production"], errors="coerce")
        if group_cols:
            for _, grp in df.groupby(group_cols, sort=False):
                c_vals = pd.to_numeric(grp["cum_production"], errors="coerce")
                diffs = c_vals.diff().dropna()
                if (diffs < -1e-9).any():
                    errors.append("cum_production not monotonically non-decreasing for group")
                    break
        else:
            diffs = cum.diff().dropna()
            if (diffs < -1e-9).any():
                errors.append("cum_production not monotonically non-decreasing")

    # TR-07: decline_rate in [-1.0, 10.0]
    if "decline_rate" in df.columns:
        dr = pd.to_numeric(df["decline_rate"], errors="coerce")
        out_dr = dr[dr.notna() & ((dr < -1.0) | (dr > 10.0))]
        if not out_dr.empty:
            errors.append(f"decline_rate outside [-1.0, 10.0] in {len(out_dr)} rows")

    return errors


# ---------------------------------------------------------------------------
# FEA-12: Stage entry point
# ---------------------------------------------------------------------------


def features(config: dict, client: object) -> None:
    """Top-level entry point for the features stage.

    Args:
        config: Pipeline configuration dict.
        client: Dask distributed Client.
    """
    transform_dir = Path(config["features"]["transform_dir"])
    ddf = dd.read_parquet(str(transform_dir))

    ddf = apply_feature_transforms(ddf)
    write_features(ddf, config)

    # Validate feature correctness on a sample partition
    meta_errors = validate_feature_correctness(ddf._meta.copy())  # noqa: SLF001
    if meta_errors:
        for err in meta_errors:
            logger.warning("Feature correctness issue (meta): %s", err)

    logger.info("Features stage complete")
