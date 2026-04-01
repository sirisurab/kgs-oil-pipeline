"""KGS raw file ingestion module.

Reads raw .txt files from data/raw/, validates schema, coerces types,
filters dates, and writes consolidated interim Parquet output.
"""

from pathlib import Path

import dask  # type: ignore[import-untyped]
import dask.dataframe as dd  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import REQUIRED_COLUMNS
from kgs_pipeline.utils import setup_logging

logger = setup_logging(__name__)


class SchemaError(Exception):
    """Raised when a raw DataFrame is missing required columns."""


# ---------------------------------------------------------------------------
# Task 01: Single-file reader
# ---------------------------------------------------------------------------


def read_raw_file(file_path: str) -> pd.DataFrame:
    """Read a single KGS raw .txt file into a Pandas DataFrame.

    Args:
        file_path: Path to the raw .txt file.

    Returns:
        DataFrame with all columns as strings plus a source_file column.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or has no data rows.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Raw file is empty: {file_path}")

    df = pd.read_csv(file_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    if len(df) == 0:
        raise ValueError(f"Raw file has no data rows: {file_path}")

    df["source_file"] = path.name
    return df


# ---------------------------------------------------------------------------
# Task 02: Schema validator
# ---------------------------------------------------------------------------


def validate_schema(df: pd.DataFrame, file_path: str) -> None:
    """Validate that df contains all required columns.

    Args:
        df: DataFrame to validate.
        file_path: Included in error message for diagnostics.

    Raises:
        SchemaError: If any required column is missing.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"Missing columns {missing} in file: {file_path}")


# ---------------------------------------------------------------------------
# Task 03: Type coercion
# ---------------------------------------------------------------------------


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce DataFrame columns to canonical types.

    Numeric columns: PRODUCTION, WELLS, LATITUDE, LONGITUDE → float64.
    All remaining columns → pd.StringDtype().

    Args:
        df: Raw DataFrame (all columns string dtype).

    Returns:
        DataFrame with coerced types.
    """
    numeric_cols = ["PRODUCTION", "WELLS", "LATITUDE", "LONGITUDE"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    string_cols = [c for c in df.columns if c not in numeric_cols]
    for col in string_cols:
        df[col] = df[col].astype(pd.StringDtype())

    return df


# ---------------------------------------------------------------------------
# Task 04: Date-range filter
# ---------------------------------------------------------------------------


def filter_date_range(df: pd.DataFrame, min_year: int = 2024) -> pd.DataFrame:
    """Filter to rows where MONTH-YEAR year >= min_year and month > 0.

    Args:
        df: DataFrame with MONTH-YEAR string column.
        min_year: Minimum year to retain (inclusive).

    Returns:
        Filtered DataFrame.
    """

    def _parse_month_year(val: object) -> tuple[int | None, int | None]:
        if not isinstance(val, str) or pd.isna(val):
            return None, None
        parts = val.split("-")
        year_str = parts[-1]
        if not year_str.isdigit():
            return None, None
        # Month is everything before the last "-" joined back
        # For "1-2024" → month_str="1", for "-1-2024" → month_str="-1"
        month_str = "-".join(parts[:-1])
        try:
            month = int(month_str)
            year = int(year_str)
        except ValueError:
            return None, None
        return month, year

    parsed = df["MONTH-YEAR"].apply(_parse_month_year)
    months = pd.array([x[0] for x in parsed], dtype=pd.Int64Dtype())
    years = pd.array([x[1] for x in parsed], dtype=pd.Int64Dtype())

    months_s = pd.Series(months)
    years_s = pd.Series(years)

    mask = years_s.notna() & (years_s >= min_year) & months_s.notna() & (months_s > 0)
    return df[mask.values].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Task 05: Parallel file ingestion pipeline
# ---------------------------------------------------------------------------


def _process_file(file_path: str) -> pd.DataFrame | None:
    """Pipeline for a single file: read → validate → coerce → filter."""
    try:
        df = read_raw_file(file_path)
        validate_schema(df, file_path)
        df = coerce_types(df)
        df = filter_date_range(df)
        return df
    except Exception as exc:  # noqa: BLE001
        logger.warning("Skipping file %s: %s", file_path, exc)
        return None


def run_ingest(raw_dir: str, output_dir: str) -> dd.DataFrame:
    """Main entry point for the ingest stage.

    Args:
        raw_dir: Directory containing raw .txt files.
        output_dir: Directory for interim Parquet output.

    Returns:
        Dask DataFrame of the written interim Parquet.

    Raises:
        ValueError: If no .txt files found, all files fail, or no rows survive.
    """
    txt_files = sorted(Path(raw_dir).glob("*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found in {raw_dir}")

    delayed_tasks = [dask.delayed(_process_file)(str(f)) for f in txt_files]
    results = dask.compute(*delayed_tasks, scheduler="threads")

    valid: list[pd.DataFrame] = [r for r in results if r is not None]
    if not valid:
        raise ValueError("All raw files failed ingestion")

    combined = pd.concat(valid, ignore_index=True)
    if len(combined) == 0:
        raise ValueError("No data rows survived ingestion")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    total_rows = len(combined)
    n_partitions = max(1, total_rows // 500_000)

    ddf = dd.from_pandas(combined, npartitions=n_partitions)
    ddf.to_parquet(output_dir, write_index=False)

    ddf_out = dd.read_parquet(output_dir)
    n_out = ddf_out.npartitions
    ddf_out = ddf_out.repartition(npartitions=min(n_out, 50))
    logger.info(
        "Ingest complete: %d rows from %d files → %s",
        total_rows,
        len(valid),
        output_dir,
    )
    return ddf_out


if __name__ == "__main__":
    import argparse

    from kgs_pipeline.config import INTERIM_DIR, RAW_DIR

    parser = argparse.ArgumentParser(description="KGS ingest stage")
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    parser.add_argument("--output-dir", default=str(INTERIM_DIR))
    args = parser.parse_args()

    run_ingest(args.raw_dir, args.output_dir)
