"""Transform component: clean and standardise KGS interim data."""

from __future__ import annotations

import argparse
from pathlib import Path

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import config as _cfg
from kgs_pipeline.logging_utils import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Output meta helpers
# ---------------------------------------------------------------------------

# Columns remaining after standardise_columns (canonical lower-case names)
_STD_STRING_COLS = [
    "lease_kid",
    "lease_name",
    "api_number",
    "operator",
    "county",
    "field",
    "producing_zone",
    "product",
    "source_file",
]
_STD_FLOAT_COLS = ["production", "well_count", "latitude", "longitude"]

# Optional columns kept if present (not required downstream)
_OPTIONAL_COLS = ["DOR_CODE", "TOWNSHIP", "TWN_DIR", "RANGE", "RANGE_DIR", "SECTION", "SPOT"]

# Wide format columns after pivot
_WIDE_STRING_COLS = [
    "lease_kid",
    "api_number",
    "operator",
    "county",
    "field",
    "producing_zone",
    "source_file",
    "lease_name",
]
_WIDE_FLOAT_COLS = ["oil_bbl", "gas_mcf", "well_count", "latitude", "longitude"]


def _std_meta() -> pd.DataFrame:
    """Meta for standardise_columns output (includes production_date, product)."""
    d: dict[str, object] = {col: pd.array([], dtype=pd.StringDtype()) for col in _STD_STRING_COLS}
    d.update({col: pd.array([], dtype="float64") for col in _STD_FLOAT_COLS})
    d["production_date"] = pd.array([], dtype="datetime64[ns]")
    return pd.DataFrame(d)


def _wide_meta() -> pd.DataFrame:
    """Meta for pivot_products output."""
    d: dict[str, object] = {col: pd.array([], dtype=pd.StringDtype()) for col in _WIDE_STRING_COLS}
    d.update({col: pd.array([], dtype="float64") for col in _WIDE_FLOAT_COLS})
    d["production_date"] = pd.array([], dtype="datetime64[ns]")
    df = pd.DataFrame(d)
    # Reorder columns to match actual merge output order
    col_order = [
        "lease_kid",
        "production_date",
        "api_number",
        "operator",
        "county",
        "field",
        "producing_zone",
        "well_count",
        "latitude",
        "longitude",
        "source_file",
        "lease_name",
        "oil_bbl",
        "gas_mcf",
    ]
    # Only include columns that exist
    col_order = [c for c in col_order if c in df.columns]
    return df[col_order]


def _flagged_meta() -> pd.DataFrame:
    """Meta for flag_outliers output."""
    base = _wide_meta()
    base["is_outlier"] = pd.array([], dtype="bool")
    base["has_negative_production"] = pd.array([], dtype="bool")
    return base


# ---------------------------------------------------------------------------
# Task 01: Read and repartition interim Parquet
# ---------------------------------------------------------------------------


def read_interim(interim_dir: str) -> dd.DataFrame:
    """Read interim Parquet files and repartition to at most 50 partitions.

    Raises:
        FileNotFoundError: if *interim_dir* does not exist or has no Parquet files.
    """
    path = Path(interim_dir)
    if not path.exists():
        raise FileNotFoundError(f"Interim directory not found: {interim_dir}")
    if not list(path.glob("*.parquet")):
        raise FileNotFoundError(f"No Parquet files found in: {interim_dir}")

    ddf = dd.read_parquet(str(path))
    n = min(ddf.npartitions, 50)
    ddf = ddf.repartition(npartitions=n)

    logger.info(
        "Read interim Parquet",
        extra={"partitions": ddf.npartitions, "dir": interim_dir},
    )
    return ddf


# ---------------------------------------------------------------------------
# Task 02: Drop non-monthly rows and parse production date
# ---------------------------------------------------------------------------


def _parse_date_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Filter non-monthly rows and create production_date column."""
    if "MONTH_YEAR" not in pdf.columns:
        pdf["production_date"] = pd.NaT
        return pdf

    # Drop annual summary (month=0) and cumulative-start rows
    mask = ~(
        pdf["MONTH_YEAR"].astype(str).str.startswith("0-")
        | pdf["MONTH_YEAR"].astype(str).str.startswith("-1-")
    )
    pdf = pdf[mask].copy()

    def _parse_my(val: object) -> pd.Timestamp:
        s = str(val).strip()
        parts = s.split("-")
        try:
            month = int(parts[0])
            year = int(parts[-1])
            return pd.Timestamp(year=year, month=month, day=1)
        except (ValueError, IndexError):
            return pd.NaT  # type: ignore[return-value]

    pdf["production_date"] = pdf["MONTH_YEAR"].apply(_parse_my)
    pdf = pdf[pdf["production_date"].notna()].copy()
    pdf = pdf.drop(columns=["MONTH_YEAR"])
    return pdf


def parse_production_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Filter non-monthly rows and parse MONTH_YEAR → production_date column.

    Returns a Dask DataFrame without MONTH_YEAR and with production_date (datetime64[ns]).
    """
    # Build meta: drop MONTH_YEAR, add production_date
    existing_cols = [c for c in ddf._meta.columns if c != "MONTH_YEAR"]
    meta_dict: dict[str, object] = {}
    for col in existing_cols:
        meta_dict[col] = ddf._meta[col]
    meta_dict["production_date"] = pd.array([], dtype="datetime64[ns]")
    meta = pd.DataFrame(meta_dict)

    return ddf.map_partitions(_parse_date_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 03: Column standardisation and type casting
# ---------------------------------------------------------------------------


def _standardise_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, cast types per canonical schema."""
    rename_map = {
        "LEASE_KID": "lease_kid",
        "LEASE": "lease_name",
        "API_NUMBER": "api_number",
        "OPERATOR": "operator",
        "COUNTY": "county",
        "FIELD": "field",
        "PRODUCING_ZONE": "producing_zone",
        "PRODUCT": "product",
        "PRODUCTION": "production",
        "WELLS": "well_count",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
    }
    pdf = pdf.rename(columns={k: v for k, v in rename_map.items() if k in pdf.columns})

    # Drop optional administrative columns if present
    optional_drop = ["DOR_CODE", "TOWNSHIP", "TWN_DIR", "RANGE", "RANGE_DIR", "SECTION", "SPOT"]
    pdf = pdf.drop(columns=[c for c in optional_drop if c in pdf.columns])

    # Cast numeric columns
    for col in ["production", "well_count", "latitude", "longitude"]:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    # Cast string columns
    str_cols = [
        "lease_kid",
        "api_number",
        "operator",
        "county",
        "lease_name",
        "field",
        "producing_zone",
        "product",
        "source_file",
    ]
    for col in str_cols:
        if col in pdf.columns:
            pdf[col] = pdf[col].astype(pd.StringDtype())

    return pdf


def standardise_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """Rename and cast columns to the canonical processed schema."""
    # Build meta by applying to the meta DataFrame
    meta = _standardise_partition(ddf._meta.copy())
    return ddf.map_partitions(_standardise_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 04: Deduplicate records
# ---------------------------------------------------------------------------


def _dedup_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.drop_duplicates(subset=["lease_kid", "product", "production_date"], keep="first")


def deduplicate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Remove within-partition duplicates on (lease_kid, product, production_date).

    NOTE: Cross-partition duplicates are not removed here. The caller should
    repartition by lease before calling this function to ensure cross-partition
    duplicates land in the same partition.
    """
    before = ddf.npartitions
    result = ddf.map_partitions(_dedup_partition, meta=ddf._meta)
    logger.info("Deduplicated", extra={"input_partitions": before})
    return result


# ---------------------------------------------------------------------------
# Task 05: Pivot oil and gas production to wide format
# ---------------------------------------------------------------------------

_PIVOT_MERGE_KEY = [
    "lease_kid",
    "production_date",
    "api_number",
    "operator",
    "county",
    "field",
    "producing_zone",
    "well_count",
    "latitude",
    "longitude",
    "source_file",
]

# lease_name may also be in the merge key if present
_PIVOT_MERGE_KEY_OPTIONAL = ["lease_name"]


def _pivot_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Pivot long oil/gas rows to wide format."""
    merge_key = [c for c in _PIVOT_MERGE_KEY if c in pdf.columns]
    for c in _PIVOT_MERGE_KEY_OPTIONAL:
        if c in pdf.columns:
            merge_key.append(c)

    oil = pdf[pdf["product"] == "O"].copy()
    gas = pdf[pdf["product"] == "G"].copy()

    oil = oil.rename(columns={"production": "oil_bbl"})
    gas = gas.rename(columns={"production": "gas_mcf"})

    oil_cols = merge_key + ["oil_bbl"]
    gas_cols = merge_key + ["gas_mcf"]

    oil_subset = oil[[c for c in oil_cols if c in oil.columns]]
    gas_subset = gas[[c for c in gas_cols if c in gas.columns]]

    wide = oil_subset.merge(gas_subset, on=merge_key, how="outer")
    wide["oil_bbl"] = wide["oil_bbl"].fillna(0.0) if "oil_bbl" in wide.columns else 0.0
    wide["gas_mcf"] = wide["gas_mcf"].fillna(0.0) if "gas_mcf" in wide.columns else 0.0
    if "product" in wide.columns:
        wide = wide.drop(columns=["product"])

    return wide


def pivot_products(ddf: dd.DataFrame) -> dd.DataFrame:
    """Pivot long oil/gas format to wide format with oil_bbl and gas_mcf columns."""
    meta = _wide_meta()
    # Preserve optional columns in meta if present
    for c in _PIVOT_MERGE_KEY_OPTIONAL:
        if c in ddf._meta.columns and c not in meta.columns:
            meta[c] = pd.array([], dtype=pd.StringDtype())

    return ddf.map_partitions(_pivot_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 06: Outlier flagging
# ---------------------------------------------------------------------------


def _flag_partition(
    pdf: pd.DataFrame,
    oil_threshold: float,
    gas_threshold: float,
) -> pd.DataFrame:
    pdf = pdf.copy()
    oil = pd.to_numeric(pdf.get("oil_bbl", pd.Series(dtype="float64")), errors="coerce").fillna(0.0)
    gas = pd.to_numeric(pdf.get("gas_mcf", pd.Series(dtype="float64")), errors="coerce").fillna(0.0)

    pdf["is_outlier"] = (oil > oil_threshold) | (gas > gas_threshold)
    pdf["has_negative_production"] = (oil < 0) | (gas < 0)

    neg_count = int(pdf["has_negative_production"].sum())
    if neg_count:
        logger.warning(
            "Negative production values detected",
            extra={"count": neg_count},
        )
    return pdf


def flag_outliers(
    ddf: dd.DataFrame,
    oil_threshold: float = 50_000.0,
    gas_threshold: float = 500_000.0,
) -> dd.DataFrame:
    """Add is_outlier and has_negative_production boolean columns."""
    meta = ddf._meta.copy()
    meta["is_outlier"] = pd.array([], dtype="bool")
    meta["has_negative_production"] = pd.array([], dtype="bool")

    return ddf.map_partitions(_flag_partition, oil_threshold, gas_threshold, meta=meta)


# ---------------------------------------------------------------------------
# Task 07: Sort by well and date
# ---------------------------------------------------------------------------


def sort_by_well_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Sort the Dask DataFrame by (lease_kid, production_date) ascending.

    NOTE: Dask will add a shuffle step. The resulting DataFrame has a modified
    partition structure after the sort. The caller should repartition before writing.
    """
    logger.info("Sorting by lease_kid, production_date")
    return ddf.sort_values(["lease_kid", "production_date"])


# ---------------------------------------------------------------------------
# Task 08: Write processed Parquet output
# ---------------------------------------------------------------------------


def write_processed_parquet(ddf: dd.DataFrame, output_dir: str) -> int:
    """Write cleaned wide-format DataFrame to *output_dir* as Parquet.

    Returns the number of Parquet files written.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    n_partitions = max(1, ddf.npartitions // 5)
    ddf.repartition(npartitions=n_partitions).to_parquet(
        str(out), write_index=False, overwrite=True
    )

    file_count = len(list(out.glob("*.parquet")))
    logger.info(
        "Wrote processed Parquet",
        extra={"files": file_count, "output_dir": str(out)},
    )
    return file_count


# ---------------------------------------------------------------------------
# Task 09: Transform orchestrator and CLI
# ---------------------------------------------------------------------------


def run_transform(interim_dir: str, output_dir: str) -> int:
    """Chain all transform stages and write processed Parquet."""
    ddf = read_interim(interim_dir)
    ddf = parse_production_date(ddf)
    ddf = standardise_columns(ddf)
    ddf = deduplicate(ddf)
    ddf = pivot_products(ddf)
    ddf = flag_outliers(ddf)
    ddf = sort_by_well_date(ddf)
    return write_processed_parquet(ddf, output_dir)


def main() -> None:
    """CLI entry point for the transform stage."""
    parser = argparse.ArgumentParser(
        description="KGS transform: clean interim data to processed Parquet"
    )
    parser.add_argument("--interim-dir", default=_cfg.INTERIM_DATA_DIR)
    parser.add_argument("--output-dir", default=_cfg.PROCESSED_DATA_DIR)
    args = parser.parse_args()

    count = run_transform(args.interim_dir, args.output_dir)
    print(f"Transform wrote {count} Parquet file(s).")


if __name__ == "__main__":
    main()
