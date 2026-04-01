"""KGS data transform/cleaning module.

Loads interim Parquet, cleans data, and writes to data/processed/clean/.
"""

from pathlib import Path
from typing import Optional

import dask.dataframe as dd  # type: ignore[import-untyped]
import numpy as np
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import (
    IQR_MULTIPLIER,
    MAX_READ_PARTITIONS,
    PRODUCTION_UNIT_ERROR_THRESHOLD,
    STRING_COLS,
)
from kgs_pipeline.utils import setup_logging

logger = setup_logging(__name__)


# ---------------------------------------------------------------------------
# Task 01: Interim Parquet reader
# ---------------------------------------------------------------------------


def load_interim(interim_dir: str) -> dd.DataFrame:
    """Read all Parquet files from interim_dir into a Dask DataFrame.

    Args:
        interim_dir: Path to data/interim/.

    Returns:
        Repartitioned Dask DataFrame (npartitions <= 50).

    Raises:
        FileNotFoundError: If the directory does not exist.
        ValueError: If no Parquet files are found.
    """
    path = Path(interim_dir)
    if not path.exists():
        raise FileNotFoundError(f"Interim directory not found: {interim_dir}")
    parquet_files = list(path.glob("*.parquet")) + list(path.glob("**/*.parquet"))
    if not parquet_files:
        raise ValueError(f"No Parquet files found in: {interim_dir}")

    ddf = dd.read_parquet(interim_dir)
    n = ddf.npartitions
    return ddf.repartition(npartitions=min(n, MAX_READ_PARTITIONS))


# ---------------------------------------------------------------------------
# Task 02: Null handling
# ---------------------------------------------------------------------------


def _handle_nulls_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Per-partition null imputation logic."""
    numeric_cols = ["PRODUCTION", "WELLS", "LATITUDE", "LONGITUDE"]
    categorical_cols = ["LEASE", "OPERATOR", "COUNTY", "PRODUCT", "PRODUCING_ZONE", "FIELD"]
    identity_cols = ["LEASE_KID", "API_NUMBER", "MONTH-YEAR", "source_file"]

    # Drop rows where identity columns are null
    for col in identity_cols:
        if col in df.columns:
            df = df[df[col].notna()]

    # Impute numeric columns with partition-level median
    for col in numeric_cols:
        if col in df.columns:
            median_val = df[col].median()
            if pd.notna(median_val):
                df[col] = df[col].fillna(median_val)

    # Impute categorical columns with partition-level mode or "UNKNOWN"
    for col in categorical_cols:
        if col in df.columns:
            col_data = df[col]
            # Convert to plain strings to avoid StringDtype complexity
            str_vals = col_data.astype(str)
            valid_vals = str_vals[(str_vals != "nan") & (str_vals != "<NA>") & (str_vals != "None")]
            if len(valid_vals) > 0:
                mode_val = valid_vals.mode()
                fill_val = mode_val.iloc[0] if len(mode_val) > 0 else "UNKNOWN"
            else:
                fill_val = "UNKNOWN"
            df[col] = col_data.fillna(fill_val)

    return df


def handle_nulls(ddf: dd.DataFrame) -> dd.DataFrame:
    """Handle missing values using per-partition median/mode imputation.

    - Numeric columns: impute with partition-level median.
    - Categorical columns: impute with partition-level mode or 'UNKNOWN'.
    - Identity columns (LEASE_KID, API_NUMBER, MONTH-YEAR, source_file): drop null rows.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with nulls handled.
    """
    return ddf.map_partitions(_handle_nulls_partition, meta=ddf)


# ---------------------------------------------------------------------------
# Task 03: Duplicate removal
# ---------------------------------------------------------------------------


def remove_duplicates(ddf: dd.DataFrame) -> dd.DataFrame:
    """Remove duplicates on composite key (LEASE_KID, MONTH-YEAR, PRODUCT).

    Performs both partition-level and cross-partition deduplication.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with duplicates removed.
    """
    dedup_cols = ["LEASE_KID", "MONTH-YEAR", "PRODUCT"]

    # Partition-level dedup
    ddf = ddf.map_partitions(
        lambda df: df.drop_duplicates(subset=dedup_cols, keep="first"),
        meta=ddf,
    )

    # Cross-partition dedup (required since duplicates can span boundaries)
    df_full = ddf.compute()
    df_deduped = df_full.drop_duplicates(subset=dedup_cols, keep="first")
    n_partitions = min(max(1, len(df_deduped) // 500_000), 50)
    ddf_out = dd.from_pandas(df_deduped.reset_index(drop=True), npartitions=n_partitions)
    return ddf_out.repartition(npartitions=min(ddf_out.npartitions, 50))


# ---------------------------------------------------------------------------
# Task 04: Outlier capping
# ---------------------------------------------------------------------------


def cap_outliers(ddf: dd.DataFrame, iqr_multiplier: float = IQR_MULTIPLIER) -> dd.DataFrame:
    """Apply IQR outlier capping to PRODUCTION column.

    Physical lower bound: negatives → 0.0.
    IQR upper bound: values above Q3 + multiplier*(Q3-Q1) → upper fence.
    Logs WARNING for values > PRODUCTION_UNIT_ERROR_THRESHOLD.

    Args:
        ddf: Input Dask DataFrame.
        iqr_multiplier: IQR fence multiplier.

    Returns:
        Dask DataFrame with PRODUCTION capped.
    """
    # Compute global quantiles (only on PRODUCTION column)
    prod_series = ddf["PRODUCTION"].dropna()
    q1 = prod_series.quantile(0.25).compute()
    q3 = prod_series.quantile(0.75).compute()
    upper_fence = q3 + iqr_multiplier * (q3 - q1)

    def _cap_partition(df: pd.DataFrame) -> pd.DataFrame:
        if "PRODUCTION" not in df.columns:
            return df
        prod = df["PRODUCTION"].copy()
        # Physical lower bound
        prod = prod.clip(lower=0.0)
        # IQR upper bound
        prod = prod.clip(upper=upper_fence)
        df["PRODUCTION"] = prod

        # Log unit-error warnings
        if "PRODUCT" in df.columns:
            suspicious = df[
                (df["PRODUCT"] == "O") & (df["PRODUCTION"] > PRODUCTION_UNIT_ERROR_THRESHOLD)
            ]
            if len(suspicious) > 0:
                for _, row in suspicious.iterrows():
                    logger.warning(
                        "Suspicious PRODUCTION value %.1f for LEASE_KID=%s MONTH-YEAR=%s",
                        row["PRODUCTION"],
                        row.get("LEASE_KID", "?"),
                        row.get("MONTH-YEAR", "?"),
                    )
        return df

    return ddf.map_partitions(_cap_partition, meta=ddf)


# ---------------------------------------------------------------------------
# Task 05: String standardisation
# ---------------------------------------------------------------------------


def _standardise_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Apply uppercase + strip to string columns in a partition."""
    for col in STRING_COLS:
        if col in df.columns:
            s = df[col]
            if hasattr(s, "str"):
                df[col] = s.str.upper().str.strip()
    return df


def standardise_strings(ddf: dd.DataFrame) -> dd.DataFrame:
    """Standardise all string-typed columns to uppercase, stripped.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with string columns standardised.
    """
    return ddf.map_partitions(_standardise_partition, meta=ddf)


# ---------------------------------------------------------------------------
# Task 06: production_date derivation
# ---------------------------------------------------------------------------


def _parse_month_year(val: object) -> Optional[pd.Timestamp]:
    """Parse 'M-YYYY' string to a Timestamp for the first of the month."""
    if not isinstance(val, str) or pd.isna(val):
        return None
    parts = val.split("-")
    if len(parts) < 2:
        return None
    year_str = parts[-1]
    month_str = "-".join(parts[:-1])
    try:
        month = int(month_str)
        year = int(year_str)
        if month < 1 or month > 12:
            return None
        return pd.Timestamp(f"{year}-{month:02d}-01")
    except (ValueError, OverflowError):
        return None


def _derive_date_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Add production_date column and drop NaT rows."""
    df = df.copy()
    df["production_date"] = df["MONTH-YEAR"].apply(_parse_month_year)
    df = df[df["production_date"].notna()].reset_index(drop=True)
    return df


def derive_production_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add production_date (datetime64[ns]) derived from MONTH-YEAR.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with production_date column added.
    """
    meta = ddf._meta.copy()
    meta["production_date"] = pd.Series(dtype="datetime64[ns]")
    return ddf.map_partitions(_derive_date_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 07: Well completeness check
# ---------------------------------------------------------------------------


def check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame:
    """Produce completeness diagnostics per (LEASE_KID, PRODUCT).

    Computes expected vs actual monthly records and logs gaps.

    Args:
        ddf: Cleaned Dask DataFrame with production_date column.

    Returns:
        Pandas DataFrame with completeness stats per (LEASE_KID, PRODUCT).
    """
    df = ddf[["LEASE_KID", "PRODUCT", "production_date"]].compute()

    def _completeness(grp: pd.DataFrame) -> pd.Series:
        dates = grp["production_date"].dropna().sort_values()
        if len(dates) == 0:
            return pd.Series(
                {
                    "first_date": pd.NaT,
                    "last_date": pd.NaT,
                    "expected_months": 0,
                    "actual_months": 0,
                    "gap_months": 0,
                    "has_gaps": False,
                }
            )
        first = dates.min()
        last = dates.max()
        expected = (last.year - first.year) * 12 + (last.month - first.month) + 1
        actual = len(dates.unique())
        gap = expected - actual
        return pd.Series(
            {
                "first_date": first,
                "last_date": last,
                "expected_months": expected,
                "actual_months": actual,
                "gap_months": gap,
                "has_gaps": gap > 0,
            }
        )

    result = df.groupby(["LEASE_KID", "PRODUCT"]).apply(_completeness).reset_index()

    gap_rows = result[result["has_gaps"]]
    for _, row in gap_rows.iterrows():
        logger.warning(
            "Gap in LEASE_KID=%s PRODUCT=%s: %d missing months",
            row["LEASE_KID"],
            row["PRODUCT"],
            row["gap_months"],
        )
    return result


# ---------------------------------------------------------------------------
# Task 08: Data integrity spot-check
# ---------------------------------------------------------------------------


def spot_check_integrity(
    raw_dir: str,
    clean_ddf: dd.DataFrame,
    sample_n: int = 20,
) -> pd.DataFrame:
    """Randomly sample records and compare raw vs cleaned PRODUCTION values.

    Args:
        raw_dir: Directory containing original raw .txt files.
        clean_ddf: Cleaned Dask DataFrame.
        sample_n: Number of records to spot-check.

    Returns:
        DataFrame with columns: LEASE_KID, MONTH-YEAR, PRODUCT,
        raw_production, clean_production, match.
    """
    cols = ["LEASE_KID", "MONTH-YEAR", "PRODUCT", "PRODUCTION", "source_file"]
    available = [c for c in cols if c in clean_ddf.columns]
    clean_df = clean_ddf[available].compute()

    n = min(sample_n, len(clean_df))
    if n == 0:
        return pd.DataFrame(
            columns=[
                "LEASE_KID",
                "MONTH-YEAR",
                "PRODUCT",
                "raw_production",
                "clean_production",
                "match",
            ]
        )

    sample = clean_df.sample(n=n, random_state=42)
    records = []

    raw_cache: dict[str, pd.DataFrame] = {}
    for _, row in sample.iterrows():
        source = row.get("source_file", "")
        if source not in raw_cache:
            raw_path = Path(raw_dir) / str(source)
            if raw_path.exists():
                try:
                    raw_cache[source] = pd.read_csv(str(raw_path), dtype=str)
                    raw_cache[source].columns = [c.strip() for c in raw_cache[source].columns]
                except Exception:  # noqa: BLE001
                    raw_cache[source] = pd.DataFrame()
            else:
                raw_cache[source] = pd.DataFrame()

        raw_df = raw_cache[source]
        clean_prod = row.get("PRODUCTION", np.nan)

        if len(raw_df) > 0 and "LEASE_KID" in raw_df.columns:
            mask = (
                (raw_df["LEASE_KID"].str.strip() == str(row["LEASE_KID"]))
                & (raw_df["MONTH-YEAR"].str.strip() == str(row["MONTH-YEAR"]))
                & (raw_df["PRODUCT"].str.strip() == str(row["PRODUCT"]))
            )
            raw_rows = raw_df[mask]
            if len(raw_rows) > 0:
                raw_prod_str = raw_rows.iloc[0].get("PRODUCTION", np.nan)
                try:
                    raw_prod = float(raw_prod_str)
                except (ValueError, TypeError):
                    raw_prod = np.nan
            else:
                raw_prod = np.nan
        else:
            raw_prod = np.nan

        if pd.isna(raw_prod) or pd.isna(clean_prod):
            match = False
        else:
            match = abs(float(raw_prod) - float(clean_prod)) < 1e-6 or float(clean_prod) <= float(
                raw_prod
            )

        records.append(
            {
                "LEASE_KID": row["LEASE_KID"],
                "MONTH-YEAR": row["MONTH-YEAR"],
                "PRODUCT": row["PRODUCT"],
                "raw_production": raw_prod,
                "clean_production": clean_prod,
                "match": match,
            }
        )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Task 09: Transform orchestrator
# ---------------------------------------------------------------------------


def run_transform(
    interim_dir: str,
    output_dir: str,
    raw_dir: str,
    iqr_multiplier: float = IQR_MULTIPLIER,
) -> dd.DataFrame:
    """Main entry point for the transform stage.

    Chains: load → handle_nulls → remove_duplicates → cap_outliers →
            standardise_strings → derive_production_date → write → read back.

    Args:
        interim_dir: Path to interim Parquet files.
        output_dir: Path to data/processed/clean/.
        raw_dir: Path to data/raw/ (for spot-check).
        iqr_multiplier: IQR multiplier for outlier capping.

    Returns:
        Cleaned Dask DataFrame (not yet computed).
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ddf = load_interim(interim_dir)
    ddf = handle_nulls(ddf)
    ddf = remove_duplicates(ddf)
    ddf = cap_outliers(ddf, iqr_multiplier)
    ddf = standardise_strings(ddf)
    ddf = derive_production_date(ddf)

    # Estimate rows without full compute
    try:
        estimated_rows = (
            len(ddf) if hasattr(ddf, "__len__") else ddf.map_partitions(len).compute().sum()
        )
    except Exception:  # noqa: BLE001
        estimated_rows = ddf.npartitions * 500_000

    n_partitions = max(1, min(estimated_rows // 500_000, 200))
    ddf = ddf.repartition(npartitions=n_partitions)
    ddf.to_parquet(output_dir, write_index=False)

    ddf_clean = dd.read_parquet(output_dir)
    n_out = ddf_clean.npartitions
    ddf_clean = ddf_clean.repartition(npartitions=min(n_out, 50))

    # Diagnostic steps (logging only)
    try:
        check_well_completeness(ddf_clean)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Well completeness check failed: %s", exc)

    try:
        spot_check_integrity(raw_dir, ddf_clean)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Spot check failed: %s", exc)

    logger.info("Transform complete. Output: %s", output_dir)
    return ddf_clean


if __name__ == "__main__":
    import argparse

    from kgs_pipeline.config import CLEAN_DIR, INTERIM_DIR, RAW_DIR

    parser = argparse.ArgumentParser(description="KGS transform stage")
    parser.add_argument("--interim-dir", default=str(INTERIM_DIR))
    parser.add_argument("--output-dir", default=str(CLEAN_DIR))
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    parser.add_argument("--iqr-multiplier", type=float, default=IQR_MULTIPLIER)
    args = parser.parse_args()

    run_transform(args.interim_dir, args.output_dir, args.raw_dir, args.iqr_multiplier)
