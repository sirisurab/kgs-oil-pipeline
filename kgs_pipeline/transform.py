"""Transform stage: parse dates, clean values, deduplicate, cast categoricals, fill gaps."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)

_OIL_MAX_BBL = 50_000.0

# ---------------------------------------------------------------------------
# Task T-01: Parse and validate the production date column
# ---------------------------------------------------------------------------


def parse_production_date(df: pd.DataFrame) -> pd.DataFrame:
    """Derive production_date from MONTH-YEAR, dropping invalid and summary rows.

    Rows with month component 0 (yearly summary) or -1 (starting cumulative)
    are dropped. Rows with unparseable dates are dropped and counted.
    MONTH-YEAR column is retained.

    Args:
        df: Pandas partition with MONTH-YEAR column.

    Returns:
        Partition with production_date column added.

    Raises:
        KeyError: If MONTH-YEAR is absent.
    """
    if "MONTH-YEAR" not in df.columns:
        raise KeyError("'MONTH-YEAR' column is absent from DataFrame")

    def _is_summary(my: Any) -> bool:
        s = str(my)
        parts = s.split("-")
        if len(parts) < 2:
            return False
        month_str = parts[0]
        if not month_str.lstrip("-").isdigit():
            return False
        month = int(month_str)
        return month in (0, -1)

    summary_mask = df["MONTH-YEAR"].map(_is_summary)
    df = df[~summary_mask].copy()

    def _parse(my: Any) -> pd.Timestamp | None:
        s = str(my)
        parts = s.split("-")
        if len(parts) < 2:
            return None
        month_str = parts[0]
        year_str = parts[-1]
        if not month_str.isdigit() or not year_str.isdigit():
            return None
        try:
            return pd.Timestamp(year=int(year_str), month=int(month_str), day=1)
        except Exception:
            return None

    parsed = df["MONTH-YEAR"].map(_parse)
    bad_count = parsed.isna().sum()
    if bad_count > 0:
        logger.warning("Dropping %d rows with invalid MONTH-YEAR values", bad_count)

    df = df[parsed.notna()].copy()
    df["production_date"] = parsed[parsed.notna()].values
    return df


# ---------------------------------------------------------------------------
# Task T-02: Validate and clean production values
# ---------------------------------------------------------------------------


def clean_production_values(df: pd.DataFrame) -> pd.DataFrame:
    """Replace physically invalid production values with NA.

    Rules applied:
    - Negative PRODUCTION → NA (TR-01)
    - Zero PRODUCTION is preserved as 0.0 (TR-05)
    - PRODUCT=="O" and PRODUCTION > 50000 → NA (TR-02)

    Args:
        df: Pandas partition with PRODUCTION and PRODUCT columns.

    Returns:
        Cleaned partition with identical schema.

    Raises:
        KeyError: If PRODUCTION or PRODUCT are absent.
    """
    if "PRODUCTION" not in df.columns:
        raise KeyError("'PRODUCTION' column is absent from DataFrame")
    if "PRODUCT" not in df.columns:
        raise KeyError("'PRODUCT' column is absent from DataFrame")

    result = df.copy()
    prod = result["PRODUCTION"]

    # TR-01: negative values are physically invalid
    neg_mask = prod < 0
    neg_count = int(neg_mask.sum())
    if neg_count > 0:
        logger.warning("Replacing %d negative PRODUCTION values with NA (TR-01)", neg_count)
        result.loc[neg_mask, "PRODUCTION"] = pd.NA

    # TR-02: oil rate cap — only applies to oil rows
    oil_mask = result["PRODUCT"] == "O"
    try:
        prod_after = pd.to_numeric(result["PRODUCTION"], errors="coerce")
        high_oil_mask = oil_mask & (prod_after > _OIL_MAX_BBL)
    except Exception:
        high_oil_mask = pd.Series(False, index=result.index)

    high_count = int(high_oil_mask.sum())
    if high_count > 0:
        logger.warning(
            "Replacing %d oil PRODUCTION values > %s BBL/month with NA (TR-02)",
            high_count,
            _OIL_MAX_BBL,
        )
        result.loc[high_oil_mask, "PRODUCTION"] = pd.NA

    return result


# ---------------------------------------------------------------------------
# Task T-03: Deduplicate records
# ---------------------------------------------------------------------------


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows based on LEASE_KID / MONTH-YEAR / PRODUCT key.

    Args:
        df: Pandas partition.

    Returns:
        Deduplicated partition with same schema.

    Raises:
        KeyError: If any of the key columns are absent.
    """
    for col in ("LEASE_KID", "MONTH-YEAR", "PRODUCT"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    before = len(df)
    df = df.drop_duplicates(subset=["LEASE_KID", "MONTH-YEAR", "PRODUCT"], keep="first")
    dropped = before - len(df)
    if dropped > 0:
        logger.info("Deduplicate: dropped %d duplicate rows", dropped)
    return df.copy()


# ---------------------------------------------------------------------------
# Task T-04: Cast categorical columns
# ---------------------------------------------------------------------------

_CATEGORICAL_SPECS: dict[str, list[str]] = {
    "PRODUCT": ["O", "G"],
    "TWN_DIR": ["S", "N"],
    "RANGE_DIR": ["E", "W"],
}


def cast_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Cast PRODUCT, TWN_DIR, RANGE_DIR to declared CategoricalDtype.

    Values outside the declared set are replaced with NA before casting.

    Args:
        df: Pandas partition.

    Returns:
        Partition with categorical columns recast.

    Raises:
        KeyError: If any categorical column is absent.
    """
    for col in _CATEGORICAL_SPECS:
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    result = df.copy()
    for col, cats in _CATEGORICAL_SPECS.items():
        dtype = pd.CategoricalDtype(categories=cats)
        # Replace out-of-vocabulary values with NA
        valid_mask = result[col].isin(cats) | result[col].isna()
        result.loc[~valid_mask, col] = pd.NA
        result[col] = result[col].astype(dtype)

    return result


# ---------------------------------------------------------------------------
# Task T-05: Fill gaps for continuous well date ranges
# ---------------------------------------------------------------------------


def fill_date_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Insert zero-production rows for months missing in each LEASE_KID/PRODUCT group.

    For each group, the monthly range from first to last production_date is
    made complete. Missing months get PRODUCTION=0.0, all other columns NA.

    Uses vectorized grouped transformations (ADR-002).

    Args:
        df: Pandas partition with production_date, LEASE_KID, PRODUCT columns.

    Returns:
        Gap-filled partition with same schema.

    Raises:
        KeyError: If production_date, LEASE_KID, or PRODUCT are absent.
    """
    for col in ("production_date", "LEASE_KID", "PRODUCT"):
        if col not in df.columns:
            raise KeyError(f"'{col}' column is absent from DataFrame")

    if df.empty:
        return df.copy()

    filled_parts: list[pd.DataFrame] = []
    columns = df.columns.tolist()

    for (lease_kid, product), grp in df.groupby(
        ["LEASE_KID", "PRODUCT"], observed=True
    ):
        grp = grp.sort_values("production_date")
        min_date = grp["production_date"].min()
        max_date = grp["production_date"].max()

        full_range = pd.date_range(start=min_date, end=max_date, freq="MS")
        existing_dates = set(grp["production_date"].dt.to_period("M"))

        gap_rows = []
        for dt in full_range:
            if dt.to_period("M") not in existing_dates:
                row: dict[str, Any] = {c: pd.NA for c in columns}
                row["production_date"] = dt
                row["LEASE_KID"] = lease_kid
                row["PRODUCT"] = product
                row["PRODUCTION"] = 0.0
                gap_rows.append(row)

        if gap_rows:
            gaps_df = pd.DataFrame(gap_rows, columns=columns)
            grp = pd.concat([grp, gaps_df], ignore_index=True)

        filled_parts.append(grp)

    if not filled_parts:
        return df.copy()

    result = pd.concat(filled_parts, ignore_index=True)
    result = result.sort_values(["LEASE_KID", "PRODUCT", "production_date"]).reset_index(
        drop=True
    )
    return result


# ---------------------------------------------------------------------------
# Task T-06: Orchestrate transform stage
# ---------------------------------------------------------------------------


def transform(config: dict[str, Any]) -> dd.DataFrame:
    """Orchestrate the full transform stage.

    Reads interim Parquet, applies all transform functions via map_partitions,
    sorts by production_date, sets LEASE_KID as index, repartitions, and
    writes processed Parquet.

    Per ADR-003: meta for every map_partitions call is derived by calling the
    actual function on a zero-row copy of _meta.

    Args:
        config: Pipeline configuration dict.

    Returns:
        Lazy Dask DataFrame indexed on LEASE_KID.

    Raises:
        FileNotFoundError: If interim Parquet path does not exist or is empty.
    """
    transform_cfg = config["transform"]
    interim_path = Path(transform_cfg["interim_path"])
    processed_path = Path(transform_cfg["processed_path"])

    if not interim_path.exists() or not any(interim_path.iterdir()):
        raise FileNotFoundError(f"Interim Parquet path not found or empty: {interim_path}")

    ddf = dd.read_parquet(str(interim_path))

    # Derive meta using actual functions on zero-row copies (ADR-003)
    meta_ppd = parse_production_date(ddf._meta.copy())
    ddf = ddf.map_partitions(parse_production_date, meta=meta_ppd)

    meta_cpv = clean_production_values(ddf._meta.copy())
    ddf = ddf.map_partitions(clean_production_values, meta=meta_cpv)

    meta_dedup = deduplicate(ddf._meta.copy())
    ddf = ddf.map_partitions(deduplicate, meta=meta_dedup)

    meta_cat = cast_categoricals(ddf._meta.copy())
    ddf = ddf.map_partitions(cast_categoricals, meta=meta_cat)

    meta_fill = fill_date_gaps(ddf._meta.copy())
    ddf = ddf.map_partitions(fill_date_gaps, meta=meta_fill)

    # Sort within each partition by production_date (H1 — sort after transformations)
    def _sort_partition(partition: pd.DataFrame) -> pd.DataFrame:
        return partition.sort_values("production_date").reset_index(drop=True)

    meta_sorted = _sort_partition(ddf._meta.copy())
    ddf = ddf.map_partitions(_sort_partition, meta=meta_sorted)

    # set_index is last structural operation before write (H4)
    n_partitions = max(10, min(ddf.npartitions, 50))
    ddf = ddf.repartition(npartitions=n_partitions)
    ddf = ddf.set_index("LEASE_KID", sorted=False, drop=True)

    processed_path.mkdir(parents=True, exist_ok=True)
    ddf.to_parquet(str(processed_path), write_index=True, overwrite=True)

    logger.info("Transform stage complete → %s", processed_path)
    return ddf
