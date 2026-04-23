"""Unit and integration tests for kgs_pipeline/features.py."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.features import (
    add_cumulative_production,
    add_decline_rate,
    add_ratio_features,
    add_rolling_features,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_well_df(
    lease_kid: int = 1001,
    months: int = 6,
    production_values: list[float] | None = None,
    product: str = "O",
    start: str = "2024-01-01",
) -> pd.DataFrame:
    """Build a simple single-well DataFrame with production_date."""
    if production_values is None:
        production_values = [100.0 + i * 10 for i in range(months)]
    n = len(production_values)
    dates = pd.date_range(start=start, periods=n, freq="MS")
    return pd.DataFrame(
        {
            "LEASE_KID": lease_kid,
            "PRODUCT": pd.Categorical([product] * n, categories=["O", "G"]),
            "production_date": dates,
            "PRODUCTION": pd.array(production_values, dtype="Float64"),
            "MONTH-YEAR": [f"{d.month}-{d.year}" for d in dates],
        }
    )


# ---------------------------------------------------------------------------
# Task F-01: add_cumulative_production
# ---------------------------------------------------------------------------


class TestAddCumulativeProduction:
    def test_monotonically_nondecreasing(self) -> None:
        df = _make_well_df(production_values=[50.0, 60.0, 70.0, 80.0])
        result = add_cumulative_production(df)
        cum = result["cum_production"].tolist()
        for i in range(1, len(cum)):
            assert cum[i] >= cum[i - 1], f"Not non-decreasing at index {i}"

    def test_shut_in_month_flat_cumulative(self) -> None:
        df = _make_well_df(production_values=[100.0, 0.0, 100.0])
        result = add_cumulative_production(df)
        cum = result["cum_production"].tolist()
        # After shut-in, cumulative should equal prior month's value (flat)
        assert float(cum[1]) == float(cum[0])  # flat at shut-in

    def test_first_month_zero_gives_zero_cumulative(self) -> None:
        df = _make_well_df(production_values=[0.0, 100.0, 100.0])
        result = add_cumulative_production(df)
        assert float(result.iloc[0]["cum_production"]) == 0.0

    def test_resumes_after_shut_in(self) -> None:
        df = _make_well_df(production_values=[100.0, 0.0, 50.0])
        result = add_cumulative_production(df)
        cum = result["cum_production"].tolist()
        assert float(cum[2]) == 150.0  # 100 + 0 + 50

    def test_raises_keyerror_missing_production(self) -> None:
        df = pd.DataFrame(
            {
                "LEASE_KID": [1001],
                "PRODUCT": ["O"],
                "production_date": [pd.Timestamp("2024-01-01")],
            }
        )
        with pytest.raises(KeyError):
            add_cumulative_production(df)


# ---------------------------------------------------------------------------
# Task F-02: add_ratio_features
# ---------------------------------------------------------------------------


class TestAddRatioFeatures:
    def _make_oil_gas_df(
        self,
        oil: float,
        gas: float,
        lease_kid: int = 1001,
    ) -> pd.DataFrame:
        """Build a DataFrame with one oil row and one gas row for the same lease/date."""
        date = pd.Timestamp("2024-01-01")
        return pd.DataFrame(
            {
                "LEASE_KID": [lease_kid, lease_kid],
                "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
                "production_date": [date, date],
                "PRODUCTION": pd.array([oil, gas], dtype="Float64"),
                "MONTH-YEAR": ["1-2024", "1-2024"],
            }
        )

    def test_gor_oil_pos_gas_zero_returns_zero(self) -> None:
        df = self._make_oil_gas_df(oil=100.0, gas=0.0)
        result = add_ratio_features(df)
        oil_row = result[result["PRODUCT"] == "O"].iloc[0]
        assert float(oil_row["gor"]) == 0.0

    def test_gor_oil_zero_gas_pos_returns_nan(self) -> None:
        df = self._make_oil_gas_df(oil=0.0, gas=500.0)
        result = add_ratio_features(df)
        oil_row = result[result["PRODUCT"] == "O"].iloc[0]
        assert np.isnan(float(oil_row["gor"]))

    def test_gor_both_zero_no_exception(self) -> None:
        df = self._make_oil_gas_df(oil=0.0, gas=0.0)
        result = add_ratio_features(df)
        assert "gor" in result.columns

    def test_water_cut_is_nan(self) -> None:
        df = self._make_oil_gas_df(oil=100.0, gas=200.0)
        result = add_ratio_features(df)
        assert result["water_cut"].isna().all()

    def test_raises_keyerror_missing_product(self) -> None:
        df = pd.DataFrame({"PRODUCTION": [100.0]})
        with pytest.raises(KeyError, match="PRODUCT"):
            add_ratio_features(df)


# ---------------------------------------------------------------------------
# Task F-03: add_decline_rate
# ---------------------------------------------------------------------------


class TestAddDeclineRate:
    def test_clipped_below_minus_one(self) -> None:
        # From 100 to 10 = -90% change = -0.9 (no clipping needed)
        df = _make_well_df(production_values=[100.0, 10.0])
        result = add_decline_rate(df)
        assert result.iloc[1]["decline_rate"] == pytest.approx(-0.9)

    def test_clipped_above_ten(self) -> None:
        # From 1 to 100 = +9900% change → clipped to 10
        df = _make_well_df(production_values=[1.0, 100.0])
        result = add_decline_rate(df)
        assert result.iloc[1]["decline_rate"] == pytest.approx(10.0)

    def test_within_bounds_passes_through(self) -> None:
        # From 100 to 80 = -20% change
        df = _make_well_df(production_values=[100.0, 80.0])
        result = add_decline_rate(df)
        assert result.iloc[1]["decline_rate"] == pytest.approx(-0.2)

    def test_two_consecutive_zeros_no_extreme(self) -> None:
        df = _make_well_df(production_values=[100.0, 0.0, 0.0])
        result = add_decline_rate(df)
        rate = float(result.iloc[2]["decline_rate"])
        # Must be in [-1, 10] — no unclipped extreme before clipping
        assert -1.0 <= rate <= 10.0

    def test_raises_keyerror_missing_column(self) -> None:
        df = pd.DataFrame(
            {
                "LEASE_KID": [1001],
                "PRODUCT": ["O"],
                "production_date": [pd.Timestamp("2024-01-01")],
            }
        )
        with pytest.raises(KeyError):
            add_decline_rate(df)


# ---------------------------------------------------------------------------
# Task F-04: add_rolling_features
# ---------------------------------------------------------------------------


class TestAddRollingFeatures:
    def test_rolling_3m_and_6m_values(self) -> None:
        vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
        df = _make_well_df(production_values=vals)
        result = add_rolling_features(df)
        # 3m at index 2 = mean(10,20,30) = 20
        assert result.iloc[2]["rolling_3m"] == pytest.approx(20.0)
        # 6m at index 5 = mean(10,20,30,40,50,60) = 35
        assert result.iloc[5]["rolling_6m"] == pytest.approx(35.0)

    def test_insufficient_history_gives_nan(self) -> None:
        df = _make_well_df(months=2, production_values=[10.0, 20.0])
        result = add_rolling_features(df)
        # 3m window with only 2 months → NaN
        assert result["rolling_3m"].isna().all()

    def test_lag_1_equals_prior_month(self) -> None:
        vals = [10.0, 20.0, 30.0]
        df = _make_well_df(production_values=vals)
        result = add_rolling_features(df)
        # lag_1 at month 3 = value at month 2
        assert float(result.iloc[2]["lag_1"]) == 20.0

    def test_lag_1_first_month_is_nan(self) -> None:
        df = _make_well_df(months=3, production_values=[10.0, 20.0, 30.0])
        result = add_rolling_features(df)
        assert pd.isna(result.iloc[0]["lag_1"])

    def test_raises_keyerror_missing_production(self) -> None:
        df = pd.DataFrame({"production_date": [pd.Timestamp("2024-01-01")]})
        with pytest.raises(KeyError):
            add_rolling_features(df)


# ---------------------------------------------------------------------------
# Task F-05: features (integration)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFeatures:
    def test_returns_dask_dataframe(self, tmp_path: Path) -> None:
        from kgs_pipeline.features import features

        processed = _write_processed(tmp_path)
        output = tmp_path / "features_out"
        cfg = {
            "features": {
                "processed_path": str(processed),
                "output_path": str(output),
            }
        }
        result = features(cfg)
        assert isinstance(result, dd.DataFrame)

    def test_all_feature_columns_present(self, tmp_path: Path) -> None:
        from kgs_pipeline.features import features

        processed = _write_processed(tmp_path)
        output = tmp_path / "features_out"
        cfg = {
            "features": {
                "processed_path": str(processed),
                "output_path": str(output),
            }
        }
        features(cfg)

        read_back = dd.read_parquet(str(output)).compute()
        for col in [
            "cum_production",
            "gor",
            "water_cut",
            "decline_rate",
            "rolling_3m",
            "rolling_6m",
            "lag_1",
        ]:
            assert col in read_back.columns, f"Missing feature column: {col}"

    def test_parquet_readable_after_features(self, tmp_path: Path) -> None:
        from kgs_pipeline.features import features

        processed = _write_processed(tmp_path)
        output = tmp_path / "features_out"
        cfg = {
            "features": {
                "processed_path": str(processed),
                "output_path": str(output),
            }
        }
        features(cfg)
        ddf = dd.read_parquet(str(output))
        assert isinstance(ddf, dd.DataFrame)

    def test_raises_when_processed_missing(self, tmp_path: Path) -> None:
        from kgs_pipeline.features import features

        cfg = {
            "features": {
                "processed_path": str(tmp_path / "no_such"),
                "output_path": str(tmp_path / "out"),
            }
        }
        with pytest.raises(FileNotFoundError):
            features(cfg)


def _write_processed(tmp_path: Path) -> Path:
    """Write minimal processed Parquet (transform output) for features tests."""
    from kgs_pipeline.transform import (
        parse_production_date,
        clean_production_values,
        deduplicate,
        cast_categoricals,
        fill_date_gaps,
    )
    from kgs_pipeline.ingest import build_dtype_map, parse_raw_file
    from tests.conftest import make_raw_csv_file

    raw = tmp_path / "raw_for_features"
    raw.mkdir(exist_ok=True)
    make_raw_csv_file(
        raw / "f1.txt", month_years=["1-2024", "2-2024", "3-2024", "4-2024", "5-2024", "6-2024"]
    )

    dtype_map = build_dtype_map(Path("references/kgs_monthly_data_dictionary.csv"))
    df = parse_raw_file(raw / "f1.txt", dtype_map)

    df = parse_production_date(df)
    df = clean_production_values(df)
    df = deduplicate(df)
    df = cast_categoricals(df)
    df = fill_date_gaps(df)
    df = df.sort_values("production_date").reset_index(drop=True)

    processed = tmp_path / "processed_for_features"
    processed.mkdir(exist_ok=True)

    ddf = dd.from_pandas(df.set_index("LEASE_KID"), npartitions=2)
    ddf.to_parquet(str(processed), write_index=True, overwrite=True)
    return processed
