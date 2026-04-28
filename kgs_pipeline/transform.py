"""Transform stage: parse dates, cast categoricals, validate bounds, deduplicate, gap-fill."""

import logging
import os

import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def parse_production_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert MONTH-YEAR column to production_date (datetime64[ns]); drop unparseable rows.

    Operates on a single partition. MONTH-YEAR format is "M-YYYY".
    Rows where month <= 0 (yearly/cumulative records) are dropped.
    Rows where MONTH-YEAR starts with "-" (negative month) are dropped before splitting.
    production_date is appended as the last column.
    """
    # Pre-filter: drop rows where MONTH-YEAR starts with "-" (negative month, e.g. "-1-2024")
    valid_format = ~df["MONTH-YEAR"].str.startswith("-", na=False)
    df = df[valid_format].copy()

    # Extract month and year using vectorized str accessor (ADR-002)
    parts = df["MONTH-YEAR"].str.split("-")
    month_str = parts.str[0]
    year_str = parts.str[1]

    numeric_mask = month_str.str.match(r"^\d+$", na=False) & year_str.str.match(r"^\d+$", na=False)
    df = df[numeric_mask].copy()

    if len(df) == 0:
        df["production_date"] = pd.Series(dtype="datetime64[ns]", index=df.index)
        cols = [c for c in df.columns if c != "production_date"] + ["production_date"]
        return df[cols]

    parts = df["MONTH-YEAR"].str.split("-")
    month_str = parts.str[0]
    year_str = parts.str[1]

    month = month_str.astype(int)
    year = year_str.astype(int)

    # Drop rows where month <= 0 (yearly record month=0)
    valid_mask = month >= 1
    df = df[valid_mask].copy()
    month = month[valid_mask]
    year = year[valid_mask]

    if len(df) == 0:
        df["production_date"] = pd.Series(dtype="datetime64[ns]", index=df.index)
        cols = [c for c in df.columns if c != "production_date"] + ["production_date"]
        return df[cols]

    df["production_date"] = pd.to_datetime({"year": year, "month": month, "day": 1}).astype(
        "datetime64[ns]"
    )

    # Ensure production_date is the last column (meta column order must match)
    cols = [c for c in df.columns if c != "production_date"] + ["production_date"]
    return df[cols]


def cast_categoricals(df: pd.DataFrame, data_dict: pd.DataFrame) -> pd.DataFrame:
    """Cast categorical columns to declared category sets; replace invalid values with pd.NA.

    Operates on a single partition.
    """
    cat_rows = data_dict[data_dict["dtype"] == "categorical"]

    for _, row in cat_rows.iterrows():
        col: str = row["column"]
        if col not in df.columns:
            continue
        cats_str: str = row["categories"]
        categories = [c.strip() for c in cats_str.split("|") if c.strip()]
        cat_dtype = pd.CategoricalDtype(categories=categories, ordered=False)

        # Replace values not in declared set with pd.NA before casting
        valid_mask = df[col].isin(categories)
        df = df.copy()
        df.loc[~valid_mask, col] = pd.NA
        df[col] = df[col].astype(cat_dtype)

    return df


def validate_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Replace physically invalid PRODUCTION values with np.nan.

    - Negative PRODUCTION → np.nan (all products)
    - PRODUCTION > 50,000 BBL where PRODUCT == "O" → np.nan (unit error)
    Operates on a single partition. Float null sentinel is np.nan (ADR-003).
    """
    df = df.copy()

    # Replace negative production
    neg_mask = df["PRODUCTION"] < 0
    df.loc[neg_mask, "PRODUCTION"] = np.nan

    # Replace oil production exceeding 50,000 BBL/month
    oil_mask = df["PRODUCT"] == "O"
    high_mask = df["PRODUCTION"] > 50_000
    df.loc[oil_mask & high_mask, "PRODUCTION"] = np.nan

    return df


def deduplicate(df: pd.DataFrame, entity_col: str, date_col: str) -> pd.DataFrame:
    """Drop duplicate (entity, production_date) rows, keeping first occurrence.

    Operates on a partition already indexed on entity_col. Resets index,
    deduplicates on (entity_col, date_col), then restores the index.
    """
    df = df.reset_index()
    df = df.drop_duplicates(subset=[entity_col, date_col], keep="first")
    df = df.set_index(entity_col)
    return df


def fill_date_gaps(df: pd.DataFrame, entity_col: str, date_col: str) -> pd.DataFrame:
    """Insert missing monthly rows per entity with PRODUCTION=0.0 and forward-filled metadata.

    Operates on a single partition indexed on entity_col.
    """
    if len(df) == 0:
        return df

    # Save column order from input BEFORE reset_index (entity_col is the index, not in columns).
    # This is the authoritative output column order — restored at the end.
    expected_cols = list(df.columns)

    df = df.reset_index()
    groups = []

    for entity_id, grp in df.groupby(entity_col, sort=False):
        grp = grp.sort_values(date_col).copy()
        date_min = grp[date_col].min()
        date_max = grp[date_col].max()

        full_range = pd.date_range(start=date_min, end=date_max, freq="MS")
        grp = grp.set_index(date_col).reindex(full_range)
        grp.index.name = date_col

        # Fill entity column and forward-fill other metadata
        grp[entity_col] = entity_id
        grp["PRODUCTION"] = grp["PRODUCTION"].fillna(0.0)
        grp = grp.ffill()

        grp = grp.reset_index()
        groups.append(grp)

    if not groups:
        return df.set_index(entity_col)[expected_cols]

    result = pd.concat(groups, ignore_index=True)
    # set_index first (removes entity_col from columns), then enforce expected_cols order.
    # This guarantees date_col stays in the same position as the input schema — never position 0.
    result = result.set_index(entity_col)
    result = result[expected_cols]
    return result


def apply_transformations(ddf: dd.DataFrame, data_dict: pd.DataFrame) -> dd.DataFrame:
    """Apply parse_production_date, cast_categoricals, validate_bounds to every partition
    via map_partitions.

    Meta is derived by calling the actual function on ddf._meta.iloc[0:0] (ADR-003).
    set_index, deduplicate, and fill_date_gaps are applied later in transform().
    """
    # --- parse_production_date ---
    meta_parse = parse_production_date(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(parse_production_date, meta=meta_parse)

    # --- cast_categoricals ---
    meta_cat = cast_categoricals(ddf._meta.iloc[0:0], data_dict)
    ddf = ddf.map_partitions(cast_categoricals, data_dict, meta=meta_cat)

    # --- validate_bounds ---
    meta_vb = validate_bounds(ddf._meta.iloc[0:0])
    ddf = ddf.map_partitions(validate_bounds, meta=meta_vb)

    return ddf


def transform(config: dict) -> None:
    """Orchestrate the full transform stage and write processed Parquet."""
    from kgs_pipeline.ingest import load_data_dictionary

    interim_dir: str = config["transform"]["interim_dir"]
    processed_dir: str = config["transform"]["processed_dir"]
    data_dict_path: str = config["transform"]["data_dict_path"]

    entity_col = "LEASE_KID"
    date_col = "production_date"

    logger.info("Transform: reading interim Parquet from %s", interim_dir)
    ddf = dd.read_parquet(interim_dir)
    data_dict = load_data_dictionary(data_dict_path)

    n = ddf.npartitions
    n_partitions = max(10, min(n, 50))
    logger.info("Transform: %d input partitions → %d output partitions", n, n_partitions)

    # Apply partition-level transformations (parse, cast, validate)
    ddf = apply_transformations(ddf, data_dict)

    # Sort by production_date within partitions before set_index (H1)
    ddf = ddf.map_partitions(
        lambda part: part.sort_values(date_col),
        meta=ddf._meta,
    )

    # set_index is the last structural operation before writing (H4)
    # sorted=False lets Dask handle the shuffle
    ddf = ddf.set_index(entity_col, sort=False, drop=True)

    # Deduplicate after set_index (H5); derive meta via ADR-003
    meta_dedup = deduplicate(ddf._meta.iloc[0:0], entity_col, date_col)
    ddf = ddf.map_partitions(deduplicate, entity_col, date_col, meta=meta_dedup)

    # fill_date_gaps after set_index (partition indexed on entity_col); derive meta via ADR-003
    meta_fill = fill_date_gaps(ddf._meta.iloc[0:0], entity_col, date_col)
    ddf = ddf.map_partitions(fill_date_gaps, entity_col, date_col, meta=meta_fill)

    # Repartition is the last operation before write (ADR-004, H3)
    ddf = ddf.repartition(npartitions=n_partitions)

    os.makedirs(processed_dir, exist_ok=True)
    logger.info("Transform: writing processed Parquet to %s", processed_dir)
    ddf.to_parquet(processed_dir, overwrite=True)
    logger.info("Transform: complete")
