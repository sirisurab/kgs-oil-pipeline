"""Features component: compute ML-ready features from processed KGS data."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore[import-untyped]

from kgs_pipeline.config import config as _cfg
from kgs_pipeline.logging_utils import get_logger

logger = get_logger(__name__)

# Base processed schema columns (before feature engineering)
_BASE_COLUMNS = {
    "lease_kid",
    "api_number",
    "lease_name",
    "operator",
    "county",
    "field",
    "producing_zone",
    "production_date",
    "oil_bbl",
    "gas_mcf",
    "well_count",
    "latitude",
    "longitude",
    "is_outlier",
    "has_negative_production",
    "source_file",
}


# ---------------------------------------------------------------------------
# Task 01: Read processed Parquet
# ---------------------------------------------------------------------------


def read_processed(processed_dir: str) -> dd.DataFrame:
    """Read processed Parquet files; repartition to at most 50 partitions.

    Raises:
        FileNotFoundError: if *processed_dir* does not exist or has no Parquet files.
    """
    path = Path(processed_dir)
    if not path.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")
    if not list(path.glob("*.parquet")):
        raise FileNotFoundError(f"No Parquet files found in: {processed_dir}")

    ddf = dd.read_parquet(str(path))
    n = min(ddf.npartitions, 50)
    ddf = ddf.repartition(npartitions=n)

    logger.info(
        "Read processed Parquet",
        extra={"partitions": ddf.npartitions, "dir": processed_dir},
    )
    return ddf


# ---------------------------------------------------------------------------
# Task 02: water_bbl, GOR, water cut
# ---------------------------------------------------------------------------


def add_water_column(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add water_bbl column (default 0.0 float64) — KGS data has no water records."""
    meta = ddf._meta.copy()
    meta["water_bbl"] = pd.array([], dtype="float64")
    return ddf.map_partitions(
        lambda pdf: pdf.assign(water_bbl=0.0),
        meta=meta,
    )


def _compute_gor_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    oil = pd.to_numeric(pdf["oil_bbl"], errors="coerce")
    gas = pd.to_numeric(pdf["gas_mcf"], errors="coerce")
    # GOR = gas / oil; NaN when oil == 0
    gor = gas / oil.replace(0, np.nan)
    pdf = pdf.copy()
    pdf["gor"] = gor.astype("float64")
    return pdf


def compute_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute gas-oil ratio: gas_mcf / oil_bbl. NaN when oil_bbl == 0."""
    meta = ddf._meta.copy()
    meta["gor"] = pd.array([], dtype="float64")
    return ddf.map_partitions(_compute_gor_partition, meta=meta)


def _compute_water_cut_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    water = pd.to_numeric(pdf.get("water_bbl", 0.0), errors="coerce").fillna(0.0)
    oil = pd.to_numeric(pdf["oil_bbl"], errors="coerce").fillna(0.0)
    denom = oil + water
    water_cut = water / denom.replace(0, np.nan)
    pdf = pdf.copy()
    pdf["water_cut"] = water_cut.astype("float64")
    return pdf


def compute_water_cut(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute water cut: water_bbl / (oil_bbl + water_bbl). NaN when both zero."""
    meta = ddf._meta.copy()
    meta["water_cut"] = pd.array([], dtype="float64")
    return ddf.map_partitions(_compute_water_cut_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 03: Cumulative production per lease
# ---------------------------------------------------------------------------


def compute_cumulative(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add cum_oil, cum_gas, cum_water columns (monotonically non-decreasing per lease).

    Uses Dask groupby cumsum which operates lazily. Data must be sorted by
    (lease_kid, production_date) for correctness.
    """
    cum_oil = ddf.groupby("lease_kid")["oil_bbl"].cumsum()
    cum_gas = ddf.groupby("lease_kid")["gas_mcf"].cumsum()
    cum_water = ddf.groupby("lease_kid")["water_bbl"].cumsum()

    result = ddf.assign(cum_oil=cum_oil, cum_gas=cum_gas, cum_water=cum_water)
    logger.info("Computed cumulative production columns")
    return result


# ---------------------------------------------------------------------------
# Task 04: Decline rate
# ---------------------------------------------------------------------------


def _compute_decline_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute per-lease decline rate within a partition.

    NOTE: cross-partition first-rows of a lease will be NaN (accepted limitation).
    Data should be sorted by (lease_kid, production_date) before this step.
    """
    pdf = pdf.copy()
    decline_rates: list[pd.Series] = []

    for _, group in pdf.groupby("lease_kid", sort=False):
        group = group.sort_values("production_date")
        oil = pd.to_numeric(group["oil_bbl"], errors="coerce")
        # pct_change handles NaN correctly; divide-by-zero → NaN
        raw = oil.pct_change()
        # When prior value was 0 (inf or nan from pct_change), set NaN explicitly
        raw = raw.where(~(oil.shift(1) == 0), other=np.nan)
        raw = raw.clip(-1.0, 10.0)
        decline_rates.append(raw.rename("decline_rate"))

    if decline_rates:
        combined = pd.concat(decline_rates)
        pdf["decline_rate"] = combined.reindex(pdf.index).astype("float64")
    else:
        pdf["decline_rate"] = np.nan
    return pdf


def compute_decline_rate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute oil production decline rate, clipped to [-1.0, 10.0].

    NaN for first row of each lease and when prior oil_bbl == 0 (TR-07d).
    """
    meta = ddf._meta.copy()
    meta["decline_rate"] = pd.array([], dtype="float64")
    return ddf.map_partitions(_compute_decline_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 05: Rolling averages and lag features
# ---------------------------------------------------------------------------


def _rolling_lag_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling windows and lag-1 per lease within partition."""
    pdf = pdf.copy()
    roll3_oil: list[pd.Series] = []
    roll6_oil: list[pd.Series] = []
    roll3_gas: list[pd.Series] = []
    roll6_gas: list[pd.Series] = []
    roll3_water: list[pd.Series] = []
    roll6_water: list[pd.Series] = []
    lag1_oil: list[pd.Series] = []
    lag1_gas: list[pd.Series] = []
    lag1_water: list[pd.Series] = []

    for _, group in pdf.groupby("lease_kid", sort=False):
        group = group.sort_values("production_date")
        idx = group.index

        oil = pd.to_numeric(group["oil_bbl"], errors="coerce")
        gas = pd.to_numeric(group["gas_mcf"], errors="coerce")
        water = pd.to_numeric(group["water_bbl"], errors="coerce")

        roll3_oil.append(oil.rolling(3, min_periods=3).mean().rename("oil_bbl_roll3"))
        roll6_oil.append(oil.rolling(6, min_periods=6).mean().rename("oil_bbl_roll6"))
        roll3_gas.append(gas.rolling(3, min_periods=3).mean().rename("gas_mcf_roll3"))
        roll6_gas.append(gas.rolling(6, min_periods=6).mean().rename("gas_mcf_roll6"))
        roll3_water.append(water.rolling(3, min_periods=3).mean().rename("water_bbl_roll3"))
        roll6_water.append(water.rolling(6, min_periods=6).mean().rename("water_bbl_roll6"))
        lag1_oil.append(oil.shift(1).rename("oil_bbl_lag1"))
        lag1_gas.append(gas.shift(1).rename("gas_mcf_lag1"))
        lag1_water.append(water.shift(1).rename("water_bbl_lag1"))

    def _concat_or_nan(series_list: list[pd.Series], name: str, index: pd.Index) -> pd.Series:
        if series_list:
            return pd.concat(series_list).reindex(index).astype("float64")
        return pd.Series(np.nan, index=index, name=name, dtype="float64")

    idx = pdf.index
    pdf["oil_bbl_roll3"] = _concat_or_nan(roll3_oil, "oil_bbl_roll3", idx)
    pdf["oil_bbl_roll6"] = _concat_or_nan(roll6_oil, "oil_bbl_roll6", idx)
    pdf["gas_mcf_roll3"] = _concat_or_nan(roll3_gas, "gas_mcf_roll3", idx)
    pdf["gas_mcf_roll6"] = _concat_or_nan(roll6_gas, "gas_mcf_roll6", idx)
    pdf["water_bbl_roll3"] = _concat_or_nan(roll3_water, "water_bbl_roll3", idx)
    pdf["water_bbl_roll6"] = _concat_or_nan(roll6_water, "water_bbl_roll6", idx)
    pdf["oil_bbl_lag1"] = _concat_or_nan(lag1_oil, "oil_bbl_lag1", idx)
    pdf["gas_mcf_lag1"] = _concat_or_nan(lag1_gas, "gas_mcf_lag1", idx)
    pdf["water_bbl_lag1"] = _concat_or_nan(lag1_water, "water_bbl_lag1", idx)
    return pdf


def compute_rolling_and_lag_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute 3/6-month rolling averages and lag-1 features per lease."""
    meta = ddf._meta.copy()
    for col in [
        "oil_bbl_roll3",
        "oil_bbl_roll6",
        "gas_mcf_roll3",
        "gas_mcf_roll6",
        "water_bbl_roll3",
        "water_bbl_roll6",
        "oil_bbl_lag1",
        "gas_mcf_lag1",
        "water_bbl_lag1",
    ]:
        meta[col] = pd.array([], dtype="float64")
    return ddf.map_partitions(_rolling_lag_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 06: Derived production-rate features
# ---------------------------------------------------------------------------


def _rate_features_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute per-day rates and months_since_first_prod."""
    pdf = pdf.copy()
    dates = pd.to_datetime(pdf["production_date"])
    days_in_month = dates.dt.days_in_month.astype("float64")

    oil = pd.to_numeric(pdf["oil_bbl"], errors="coerce")
    gas = pd.to_numeric(pdf["gas_mcf"], errors="coerce")

    pdf["oil_bbl_per_day"] = (oil / days_in_month).astype("float64")
    pdf["gas_mcf_per_day"] = (gas / days_in_month).astype("float64")

    # Months since first non-zero oil production per lease
    months_list: list[pd.Series] = []
    for lease_id, group in pdf.groupby("lease_kid", sort=False):
        group = group.sort_values("production_date")
        positive_oil = group[pd.to_numeric(group["oil_bbl"], errors="coerce") > 0]
        if positive_oil.empty:
            months_list.append(
                pd.Series(
                    np.nan, index=group.index, name="months_since_first_prod", dtype="float64"
                )
            )
        else:
            first_date = positive_oil["production_date"].min()
            first_date = pd.Timestamp(first_date)
            grp_dates = pd.to_datetime(group["production_date"])
            months = (
                (grp_dates.dt.year - first_date.year) * 12 + (grp_dates.dt.month - first_date.month)
            ).astype("float64")
            months_list.append(months.rename("months_since_first_prod"))

    if months_list:
        combined = pd.concat(months_list).reindex(pdf.index)
        pdf["months_since_first_prod"] = combined.astype("float64")
    else:
        pdf["months_since_first_prod"] = np.nan

    return pdf


def compute_rate_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Compute oil/gas per-day rates and months since first production."""
    meta = ddf._meta.copy()
    for col in ["oil_bbl_per_day", "gas_mcf_per_day", "months_since_first_prod"]:
        meta[col] = pd.array([], dtype="float64")
    return ddf.map_partitions(_rate_features_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 07: Write features Parquet
# ---------------------------------------------------------------------------


def write_features_parquet(ddf: dd.DataFrame, output_dir: str) -> int:
    """Write feature-enriched DataFrame to *output_dir* as Parquet."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    n_partitions = max(1, ddf.npartitions // 5)
    ddf.repartition(npartitions=n_partitions).to_parquet(
        str(out), write_index=False, overwrite=True
    )

    file_count = len(list(out.glob("*.parquet")))
    logger.info(
        "Wrote features Parquet",
        extra={
            "files": file_count,
            "output_dir": str(out),
            "feature_columns": len(ddf.columns),
        },
    )
    return file_count


# ---------------------------------------------------------------------------
# Task 08: Metadata manifest writer
# ---------------------------------------------------------------------------


def write_manifest(
    ddf: dd.DataFrame,
    output_dir: str,
    processing_start: datetime,
) -> Path:
    """Write a JSON metadata manifest to {output_dir}/manifest.json.

    NOTE: Calls len(ddf) which triggers a compute — intentional for manifest purposes.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    schema = {col: str(dtype) for col, dtype in zip(ddf.columns, ddf.dtypes)}
    feature_cols = [c for c in ddf.columns if c not in _BASE_COLUMNS]

    # Intentional materialise for counts
    record_count = len(ddf)
    min_date = ddf["production_date"].min().compute()
    max_date = ddf["production_date"].max().compute()

    processing_end = datetime.now(tz=timezone.utc)

    manifest = {
        "schema": schema,
        "feature_columns": feature_cols,
        "record_count": record_count,
        "partition_count": ddf.npartitions,
        "processing_start": processing_start.isoformat(),
        "processing_end": processing_end.isoformat(),
        "min_production_date": pd.Timestamp(min_date).isoformat() if pd.notna(min_date) else None,
        "max_production_date": pd.Timestamp(max_date).isoformat() if pd.notna(max_date) else None,
    }

    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote manifest", extra={"path": str(manifest_path)})
    return manifest_path


# ---------------------------------------------------------------------------
# Task 09: Features orchestrator and CLI
# ---------------------------------------------------------------------------


def run_features(processed_dir: str, output_dir: str) -> Path:
    """Chain all feature engineering stages and write output Parquet + manifest."""
    processing_start = datetime.now(tz=timezone.utc)

    ddf = read_processed(processed_dir)
    ddf = add_water_column(ddf)
    ddf = compute_gor(ddf)
    ddf = compute_water_cut(ddf)
    ddf = compute_cumulative(ddf)
    ddf = compute_decline_rate(ddf)
    ddf = compute_rolling_and_lag_features(ddf)
    ddf = compute_rate_features(ddf)
    write_features_parquet(ddf, output_dir)

    # Re-read fresh for manifest
    ddf_manifest = dd.read_parquet(output_dir)
    manifest_path = write_manifest(ddf_manifest, output_dir, processing_start)
    return manifest_path


def main() -> None:
    """CLI entry point for the features stage."""
    parser = argparse.ArgumentParser(
        description="KGS features: compute ML-ready features from processed data"
    )
    parser.add_argument("--processed-dir", default=_cfg.PROCESSED_DATA_DIR)
    parser.add_argument("--output-dir", default=_cfg.FEATURES_DATA_DIR)
    args = parser.parse_args()

    manifest = run_features(args.processed_dir, args.output_dir)
    print(f"Features complete. Manifest: {manifest}")


if __name__ == "__main__":
    main()
