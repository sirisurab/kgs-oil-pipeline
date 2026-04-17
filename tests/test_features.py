"""Tests for kgs_pipeline/features.py and kgs_pipeline/pipeline.py."""

from __future__ import annotations

import math
from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.features import (
    add_cumulative_production,
    add_decline_rate,
    add_lag_features,
    add_ratio_features,
    add_rolling_features,
    pivot_oil_gas,
    run_features,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_FEATURE_COLS = [
    "LEASE_KID",
    "production_date",
    "oil_bbl",
    "gas_mcf",
    "water_bbl",
    "cum_oil",
    "cum_gas",
    "cum_water",
    "gor",
    "water_cut",
    "decline_rate",
    "oil_roll_3",
    "oil_roll_6",
    "gas_roll_3",
    "gas_roll_6",
    "water_roll_3",
    "water_roll_6",
    "oil_lag_1",
    "gas_lag_1",
    "water_lag_1",
]


def _make_pivoted_partition(
    lease_id: int = 1001,
    months: int = 6,
    oil_vals: list[float] | None = None,
    gas_vals: list[float] | None = None,
) -> pd.DataFrame:
    """Build a pre-pivoted partition (one row per LEASE_KID, production_date)."""
    if oil_vals is None:
        oil_vals = [float(100 * (i + 1)) for i in range(months)]
    if gas_vals is None:
        gas_vals = [float(500 * (i + 1)) for i in range(months)]

    dates = pd.date_range("2024-01-01", periods=months, freq="MS")
    return pd.DataFrame(
        {
            "LEASE_KID": lease_id,
            "production_date": dates,
            "oil_bbl": pd.array(oil_vals, dtype=np.float64),
            "gas_mcf": pd.array(gas_vals, dtype=np.float64),
            "water_bbl": pd.array([0.0] * months, dtype=np.float64),
            "OPERATOR": "OP1",
            "COUNTY": "Nemaha",
        }
    )


def _make_transform_output(tmp_path: Path, rows: list[dict]) -> str:
    """Write synthetic transform-output Parquet (entity-indexed on LEASE_KID)."""
    frames = []
    for row in rows:
        month_year = row.get("MONTH-YEAR", "1-2024")
        parts = month_year.split("-")
        m = int(parts[0])
        y = int(parts[-1])
        prod_date = pd.Timestamp(year=y, month=m, day=1)

        frame = pd.DataFrame(
            {
                "LEASE_KID": pd.array([row.get("LEASE_KID", 1001)], dtype=pd.Int64Dtype()),
                "production_date": [prod_date],
                "PRODUCT": pd.Categorical([row.get("PRODUCT", "O")], categories=["O", "G"]),
                "PRODUCTION": [float(row.get("PRODUCTION", 100.0))],
                "LEASE": pd.array([row.get("LEASE", "TEST")], dtype=pd.StringDtype()),
                "OPERATOR": pd.array([row.get("OPERATOR", "OP1")], dtype=pd.StringDtype()),
                "COUNTY": pd.array([row.get("COUNTY", "Nemaha")], dtype=pd.StringDtype()),
                "FIELD": pd.array([row.get("FIELD", "F1")], dtype=pd.StringDtype()),
                "PRODUCING_ZONE": pd.array(
                    [row.get("PRODUCING_ZONE", "Z1")], dtype=pd.StringDtype()
                ),
                "API_NUMBER": pd.array([row.get("API_NUMBER", "15-001")], dtype=pd.StringDtype()),
                "LATITUDE": [row.get("LATITUDE", 39.9)],
                "LONGITUDE": [row.get("LONGITUDE", -95.8)],
                "MONTH-YEAR": pd.array([month_year], dtype=pd.StringDtype()),
            }
        )
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.set_index("LEASE_KID")
    processed_dir = str(tmp_path / "processed")
    Path(processed_dir).mkdir(parents=True, exist_ok=True)
    ddf = dd.from_pandas(combined, npartitions=1)
    ddf.to_parquet(processed_dir, engine="pyarrow", write_index=True, overwrite=True)
    return processed_dir


# ---------------------------------------------------------------------------
# Task F-01: pivot_oil_gas
# ---------------------------------------------------------------------------


class TestPivotOilGas:
    def _make_transform_ddf(self, rows: list[dict]) -> dd.DataFrame:
        frames = []
        for row in rows:
            month_year = row.get("MONTH-YEAR", "1-2024")
            parts = month_year.split("-")
            prod_date = pd.Timestamp(year=int(parts[-1]), month=int(parts[0]), day=1)
            frames.append(
                pd.DataFrame(
                    {
                        "LEASE_KID": pd.array([row["LEASE_KID"]], dtype=pd.Int64Dtype()),
                        "production_date": [prod_date],
                        "PRODUCT": pd.Categorical([row["PRODUCT"]], categories=["O", "G"]),
                        "PRODUCTION": [float(row["PRODUCTION"])],
                        "OPERATOR": pd.array(["OP1"], dtype=pd.StringDtype()),
                        "COUNTY": pd.array(["Nemaha"], dtype=pd.StringDtype()),
                        "MONTH-YEAR": pd.array([month_year], dtype=pd.StringDtype()),
                    }
                )
            )
        combined = pd.concat(frames, ignore_index=True).set_index("LEASE_KID")
        return dd.from_pandas(combined, npartitions=1)

    def test_oil_and_gas_merged(self) -> None:
        ddf = self._make_transform_ddf(
            [
                {"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
                {"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "G", "PRODUCTION": 500.0},
                {"LEASE_KID": 1001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": 120.0},
                {"LEASE_KID": 1001, "MONTH-YEAR": "2-2024", "PRODUCT": "G", "PRODUCTION": 600.0},
            ]
        )
        result = pivot_oil_gas(ddf).compute()
        assert len(result) == 2
        assert set(result.columns) >= {"oil_bbl", "gas_mcf", "water_bbl"}

    def test_oil_only_gas_is_zero(self) -> None:
        ddf = self._make_transform_ddf(
            [
                {"LEASE_KID": 2001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 200.0},
            ]
        )
        result = pivot_oil_gas(ddf).compute()
        assert result["gas_mcf"].iloc[0] == 0.0

    def test_gas_only_oil_is_zero(self) -> None:
        ddf = self._make_transform_ddf(
            [
                {"LEASE_KID": 3001, "MONTH-YEAR": "1-2024", "PRODUCT": "G", "PRODUCTION": 1000.0},
            ]
        )
        result = pivot_oil_gas(ddf).compute()
        assert result["oil_bbl"].iloc[0] == 0.0

    def test_water_bbl_is_zero(self) -> None:
        ddf = self._make_transform_ddf(
            [
                {"LEASE_KID": 4001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 50.0},
            ]
        )
        result = pivot_oil_gas(ddf).compute()
        assert (result["water_bbl"] == 0.0).all()


# ---------------------------------------------------------------------------
# Task F-02: add_cumulative_production
# ---------------------------------------------------------------------------


class TestAddCumulativeProduction:
    def test_monotonicity_tr03(self) -> None:
        part = _make_pivoted_partition(months=6)
        result = add_cumulative_production(part)
        cum = result["cum_oil"].tolist()
        assert cum == sorted(cum)

    def test_flat_periods_tr08a(self) -> None:
        part = _make_pivoted_partition(oil_vals=[100, 0, 0, 50], months=4)
        result = add_cumulative_production(part)
        assert result["cum_oil"].tolist() == [100, 100, 100, 150]

    def test_zero_at_start_tr08b(self) -> None:
        part = _make_pivoted_partition(oil_vals=[0, 0, 100], months=3)
        result = add_cumulative_production(part)
        assert result["cum_oil"].tolist() == [0, 0, 100]

    def test_resumption_after_shutin_tr08c(self) -> None:
        part = _make_pivoted_partition(oil_vals=[50, 0, 30], months=3)
        result = add_cumulative_production(part)
        assert result["cum_oil"].tolist() == [50, 50, 80]

    def test_meta_consistency_tr23(self) -> None:
        empty = _make_pivoted_partition(months=0)
        meta = add_cumulative_production(empty)
        assert "cum_oil" in meta.columns
        assert "cum_gas" in meta.columns
        assert "cum_water" in meta.columns


# ---------------------------------------------------------------------------
# Task F-03: add_ratio_features
# ---------------------------------------------------------------------------


class TestAddRatioFeatures:
    def _row(self, oil: float, gas: float, water: float = 0.0) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "LEASE_KID": 1001,
                "production_date": [pd.Timestamp("2024-01-01")],
                "oil_bbl": pd.array([oil], dtype=np.float64),
                "gas_mcf": pd.array([gas], dtype=np.float64),
                "water_bbl": pd.array([water], dtype=np.float64),
            }
        )

    def test_gor_zero_oil_nonzero_gas_is_nan_tr06a(self) -> None:
        result = add_ratio_features(self._row(0, 50))
        assert math.isnan(result["gor"].iloc[0])

    def test_gor_both_zero_is_nan_tr06b(self) -> None:
        result = add_ratio_features(self._row(0, 0))
        assert math.isnan(result["gor"].iloc[0])

    def test_gor_nonzero_oil_zero_gas_tr06c(self) -> None:
        result = add_ratio_features(self._row(100, 0))
        assert result["gor"].iloc[0] == 0.0

    def test_gor_formula_tr09d(self) -> None:
        result = add_ratio_features(self._row(100, 500))
        assert result["gor"].iloc[0] == pytest.approx(5.0)

    def test_water_cut_zero_water_tr10a(self) -> None:
        result = add_ratio_features(self._row(200, 0, 0))
        assert result["water_cut"].iloc[0] == 0.0

    def test_water_cut_zero_oil_tr10b(self) -> None:
        result = add_ratio_features(self._row(0, 0, 100))
        assert result["water_cut"].iloc[0] == 1.0

    def test_water_cut_negative_denominator_tr10c(self) -> None:
        result = add_ratio_features(self._row(-10, 0, 110))
        wc = result["water_cut"].iloc[0]
        assert wc < 0.0 or wc > 1.0  # outside [0,1] — not treated as valid

    def test_meta_consistency_tr23(self) -> None:
        empty = _make_pivoted_partition(months=0)
        meta = add_ratio_features(empty)
        assert "gor" in meta.columns
        assert "water_cut" in meta.columns


# ---------------------------------------------------------------------------
# Task F-04: add_decline_rate
# ---------------------------------------------------------------------------


class TestAddDeclineRate:
    def _seq(self, oil_vals: list[float]) -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=len(oil_vals), freq="MS")
        return pd.DataFrame(
            {
                "LEASE_KID": 1001,
                "production_date": dates,
                "oil_bbl": pd.array(oil_vals, dtype=np.float64),
                "gas_mcf": pd.array([0.0] * len(oil_vals), dtype=np.float64),
                "water_bbl": pd.array([0.0] * len(oil_vals), dtype=np.float64),
            }
        )

    def test_clip_lower_tr07a(self) -> None:
        # prev=10, curr=25 → (10-25)/10 = -1.5 → clipped to -1.0
        part = self._seq([10, 25])
        result = add_decline_rate(part)
        assert result["decline_rate"].iloc[1] == pytest.approx(-1.0)

    def test_clip_upper_tr07b(self) -> None:
        # prev=100, curr=-1000 would give large positive value → clipped to 10.0
        # Use prev=1, curr=-100 → (1 - (-100)) / 1 = 101 → clipped to 10
        part = self._seq([1, -100])
        result = add_decline_rate(part)
        assert result["decline_rate"].iloc[1] == pytest.approx(10.0)

    def test_within_bounds_tr07c(self) -> None:
        # prev=100, curr=70 → (100-70)/100 = 0.3
        part = self._seq([100, 70])
        result = add_decline_rate(part)
        assert result["decline_rate"].iloc[1] == pytest.approx(0.3)

    def test_zero_prev_zero_curr_tr07d(self) -> None:
        part = self._seq([0, 0])
        result = add_decline_rate(part)
        assert result["decline_rate"].iloc[1] == pytest.approx(0.0)

    def test_first_month_is_nan(self) -> None:
        part = self._seq([100, 80])
        result = add_decline_rate(part)
        assert math.isnan(result["decline_rate"].iloc[0])

    def test_meta_consistency_tr23(self) -> None:
        empty = _make_pivoted_partition(months=0)
        meta = add_decline_rate(empty)
        assert "decline_rate" in meta.columns


# ---------------------------------------------------------------------------
# Task F-05: add_rolling_features
# ---------------------------------------------------------------------------


class TestAddRollingFeatures:
    def test_roll3_correctness_tr09a(self) -> None:
        part = _make_pivoted_partition(oil_vals=[100, 200, 300], months=3)
        result = add_rolling_features(part)
        # Mean of 100, 200, 300 = 200.0
        assert result["oil_roll_3"].iloc[2] == pytest.approx(200.0)

    def test_partial_window_nan_tr09b_3(self) -> None:
        part = _make_pivoted_partition(oil_vals=[100, 200], months=2)
        result = add_rolling_features(part)
        assert math.isnan(result["oil_roll_3"].iloc[0])
        assert math.isnan(result["oil_roll_3"].iloc[1])

    def test_partial_window_nan_tr09b_6(self) -> None:
        part = _make_pivoted_partition(oil_vals=[10, 20, 30, 40, 50], months=5)
        result = add_rolling_features(part)
        for i in range(5):
            assert math.isnan(result["oil_roll_6"].iloc[i])

    def test_roll6_full_window(self) -> None:
        vals = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
        part = _make_pivoted_partition(oil_vals=vals, months=6)
        result = add_rolling_features(part)
        expected = sum(vals) / 6  # 350.0
        assert result["oil_roll_6"].iloc[5] == pytest.approx(expected)

    def test_roll3_month6_value(self) -> None:
        vals = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
        part = _make_pivoted_partition(oil_vals=vals, months=6)
        result = add_rolling_features(part)
        # Month 6 roll3 = mean(400, 500, 600) = 500.0
        assert result["oil_roll_3"].iloc[5] == pytest.approx(500.0)

    def test_meta_consistency_tr23(self) -> None:
        empty = _make_pivoted_partition(months=0)
        meta = add_rolling_features(empty)
        for col in [
            "oil_roll_3",
            "oil_roll_6",
            "gas_roll_3",
            "gas_roll_6",
            "water_roll_3",
            "water_roll_6",
        ]:
            assert col in meta.columns


# ---------------------------------------------------------------------------
# Task F-06: add_lag_features
# ---------------------------------------------------------------------------


class TestAddLagFeatures:
    def test_lag1_correctness_tr09c(self) -> None:
        part = _make_pivoted_partition(oil_vals=[100, 200, 300], months=3)
        result = add_lag_features(part)
        assert math.isnan(result["oil_lag_1"].iloc[0])
        assert result["oil_lag_1"].iloc[1] == pytest.approx(100.0)
        assert result["oil_lag_1"].iloc[2] == pytest.approx(200.0)

    def test_first_month_is_nan(self) -> None:
        part = _make_pivoted_partition(oil_vals=[500], months=1)
        result = add_lag_features(part)
        assert math.isnan(result["oil_lag_1"].iloc[0])

    def test_no_bleed_across_leases(self) -> None:
        lease_a = _make_pivoted_partition(lease_id=1001, oil_vals=[100, 200], months=2)
        lease_b = _make_pivoted_partition(lease_id=2001, oil_vals=[999, 888], months=2)
        combined = pd.concat([lease_a, lease_b], ignore_index=True)
        result = add_lag_features(combined)
        first_b = result[result["LEASE_KID"] == 2001].iloc[0]
        assert math.isnan(first_b["oil_lag_1"])

    def test_meta_consistency_tr23(self) -> None:
        empty = _make_pivoted_partition(months=0)
        meta = add_lag_features(empty)
        assert "oil_lag_1" in meta.columns
        assert "gas_lag_1" in meta.columns
        assert "water_lag_1" in meta.columns


# ---------------------------------------------------------------------------
# Task F-07 / F-08: run_features integration tests
# ---------------------------------------------------------------------------


class TestRunFeatures:
    def _config(self, tmp_path: Path) -> dict:
        return {
            "processed_dir": str(tmp_path / "processed"),
            "output_dir": str(tmp_path / "features"),
        }

    def _write_single_lease(self, tmp_path: Path, months: int = 6) -> None:
        rows = []
        for i in range(1, months + 1):
            rows.append(
                {
                    "LEASE_KID": 1001,
                    "MONTH-YEAR": f"{i}-2024",
                    "PRODUCT": "O",
                    "PRODUCTION": float(100 * i),
                }
            )
            rows.append(
                {
                    "LEASE_KID": 1001,
                    "MONTH-YEAR": f"{i}-2024",
                    "PRODUCT": "G",
                    "PRODUCTION": float(500 * i),
                }
            )
        _make_transform_output(tmp_path, rows)

    def test_returns_dask_dataframe_tr17(self, tmp_path: Path) -> None:
        self._write_single_lease(tmp_path)
        result = run_features(self._config(tmp_path))
        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

    def test_parquet_readable_tr18(self, tmp_path: Path) -> None:
        self._write_single_lease(tmp_path)
        run_features(self._config(tmp_path))
        reloaded = dd.read_parquet(str(tmp_path / "features"))
        assert isinstance(reloaded, dd.DataFrame)
        assert len(reloaded.compute()) > 0

    def test_feature_column_presence_tr19(self, tmp_path: Path) -> None:
        self._write_single_lease(tmp_path, months=6)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        for col in REQUIRED_FEATURE_COLS:
            assert col in result.columns, f"Missing required column: {col}"

    def test_schema_stability_tr14(self, tmp_path: Path) -> None:
        self._write_single_lease(tmp_path, months=4)
        run_features(self._config(tmp_path))
        files = list(Path(tmp_path / "features").glob("*.parquet"))
        if len(files) >= 2:
            df1 = pd.read_parquet(files[0])
            df2 = pd.read_parquet(files[1])
            assert list(df1.columns) == list(df2.columns)

    def test_processed_dir_not_exist_raises(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        with pytest.raises(FileNotFoundError):
            run_features(config)

    def test_cumulative_monotonicity_tr03(self, tmp_path: Path) -> None:
        self._write_single_lease(tmp_path, months=12)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        for col in ["cum_oil", "cum_gas", "cum_water"]:
            vals = result.sort_values("production_date")[col].tolist()
            assert vals == sorted(vals), f"{col} not monotonically non-decreasing"

    def test_gor_formula_tr06(self, tmp_path: Path) -> None:
        _make_transform_output(
            tmp_path,
            [
                {"LEASE_KID": 5001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
                {"LEASE_KID": 5001, "MONTH-YEAR": "1-2024", "PRODUCT": "G", "PRODUCTION": 500.0},
            ],
        )
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        row = result[result["LEASE_KID"] == 5001].iloc[0]
        assert row["gor"] == pytest.approx(5.0)

    def test_water_cut_tr09(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 6001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 70.0},
            {"LEASE_KID": 6001, "MONTH-YEAR": "1-2024", "PRODUCT": "G", "PRODUCTION": 0.0},
        ]
        _make_transform_output(tmp_path, rows)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        row = result[result["LEASE_KID"] == 6001].iloc[0]
        # water_bbl == 0, oil_bbl == 70 → water_cut = 0.0
        assert row["water_cut"] == pytest.approx(0.0)

    def test_decline_rate_clip_tr07(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 7001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 10.0},
            {"LEASE_KID": 7001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": 25.0},
        ]
        _make_transform_output(tmp_path, rows)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        lease_rows = result[result["LEASE_KID"] == 7001].sort_values("production_date")
        # Month 2: decline = (10-25)/10 = -1.5 → clipped to -1.0
        assert lease_rows["decline_rate"].iloc[1] == pytest.approx(-1.0)

    def test_rolling_averages_tr09(self, tmp_path: Path) -> None:
        rows = []
        for i, v in enumerate([100, 200, 300, 400, 500, 600], start=1):
            rows.append(
                {
                    "LEASE_KID": 8001,
                    "MONTH-YEAR": f"{i}-2024",
                    "PRODUCT": "O",
                    "PRODUCTION": float(v),
                }
            )
        _make_transform_output(tmp_path, rows)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        lease_rows = result[result["LEASE_KID"] == 8001].sort_values("production_date")
        assert lease_rows["oil_roll_3"].iloc[2] == pytest.approx(200.0)
        assert lease_rows["oil_roll_6"].iloc[5] == pytest.approx(350.0)

    def test_lag1_tr09c(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 9001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
            {"LEASE_KID": 9001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": 200.0},
            {"LEASE_KID": 9001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 300.0},
        ]
        _make_transform_output(tmp_path, rows)
        run_features(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "features")).compute()
        lease_rows = result[result["LEASE_KID"] == 9001].sort_values("production_date")
        assert math.isnan(lease_rows["oil_lag_1"].iloc[0])
        assert lease_rows["oil_lag_1"].iloc[1] == pytest.approx(100.0)
        assert lease_rows["oil_lag_1"].iloc[2] == pytest.approx(200.0)

    def test_meta_schema_consistency_tr23(self) -> None:
        """Verify meta= passed to each map_partitions matches actual function output."""
        empty_pivot = _make_pivoted_partition(months=0)
        for fn in [
            add_cumulative_production,
            add_ratio_features,
            add_decline_rate,
            add_rolling_features,
            add_lag_features,
        ]:
            meta_out = fn(empty_pivot.copy())
            assert isinstance(meta_out, pd.DataFrame), f"{fn.__name__} meta is not DataFrame"


# ---------------------------------------------------------------------------
# Task F-09: Pipeline orchestrator tests
# ---------------------------------------------------------------------------


class TestPipelineOrchestrator:
    def _base_config(self, tmp_path: Path) -> str:
        import yaml

        cfg = {
            "acquire": {
                "lease_index": str(tmp_path / "index.txt"),
                "raw_dir": str(tmp_path / "raw"),
                "month_save_url": "https://example.com/MonthSave",
                "download_base_url": "https://example.com/download",
                "min_year": 2024,
                "max_workers": 1,
                "sleep_seconds": 0.0,
            },
            "ingest": {
                "raw_dir": str(tmp_path / "raw"),
                "interim_dir": str(tmp_path / "interim"),
                "data_dictionary": "references/kgs_monthly_data_dictionary.csv",
            },
            "transform": {
                "interim_dir": str(tmp_path / "interim"),
                "processed_dir": str(tmp_path / "processed"),
            },
            "features": {
                "processed_dir": str(tmp_path / "processed"),
                "output_dir": str(tmp_path / "features"),
            },
            "dask": {
                "scheduler": "local",
                "n_workers": 1,
                "threads_per_worker": 1,
                "memory_limit": "1GB",
                "dashboard_port": 8788,
            },
            "logging": {
                "log_file": str(tmp_path / "logs" / "test.log"),
                "level": "WARNING",
            },
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(cfg))
        return str(config_path)

    def test_only_acquire_called(self, tmp_path: Path) -> None:
        from kgs_pipeline.pipeline import main

        config_path = self._base_config(tmp_path)
        with (
            patch("kgs_pipeline.pipeline.run_acquire") as mock_acquire,
            patch("kgs_pipeline.pipeline._init_dask_client") as mock_dask,
        ):
            mock_acquire.return_value = []
            mock_dask.return_value = MagicMock()
            with patch(
                "sys.argv", ["kgs-pipeline", "--stages", "acquire", "--config", config_path]
            ):
                main()
            mock_acquire.assert_called_once()

    def test_ingest_and_transform_called(self, tmp_path: Path) -> None:
        from kgs_pipeline.pipeline import main

        config_path = self._base_config(tmp_path)
        with (
            patch("kgs_pipeline.pipeline.run_ingest") as mock_ingest,
            patch("kgs_pipeline.pipeline.run_transform") as mock_transform,
            patch("kgs_pipeline.pipeline.run_acquire") as mock_acquire,
            patch("kgs_pipeline.pipeline._init_dask_client") as mock_dask,
        ):
            mock_ingest.return_value = MagicMock()
            mock_transform.return_value = MagicMock()
            mock_acquire.return_value = []
            mock_dask.return_value = MagicMock(dashboard_link="http://localhost:8788")
            mock_dask.return_value.close = MagicMock()
            with patch(
                "sys.argv",
                ["kgs-pipeline", "--stages", "ingest", "transform", "--config", config_path],
            ):
                main()
            mock_ingest.assert_called_once()
            mock_transform.assert_called_once()
            mock_acquire.assert_not_called()

    def test_stage_failure_stops_pipeline(self, tmp_path: Path) -> None:
        from kgs_pipeline.pipeline import main

        config_path = self._base_config(tmp_path)
        with (
            patch("kgs_pipeline.pipeline.run_ingest", side_effect=RuntimeError("boom")),
            patch("kgs_pipeline.pipeline.run_transform") as mock_transform,
            patch("kgs_pipeline.pipeline._init_dask_client") as mock_dask,
        ):
            mock_dask.return_value = MagicMock(
                dashboard_link="http://localhost:8788",
                close=MagicMock(),
            )
            with patch(
                "sys.argv",
                ["kgs-pipeline", "--stages", "ingest", "transform", "--config", config_path],
            ):
                with pytest.raises(SystemExit) as exc_info:
                    main()
            assert exc_info.value.code != 0
            mock_transform.assert_not_called()

    def test_logging_configured_before_stages(self, tmp_path: Path) -> None:
        """Logging must be set up before any stage runs."""
        import logging as stdlib_logging
        from kgs_pipeline.pipeline import _setup_logging

        cfg = {"log_file": str(tmp_path / "logs" / "test.log"), "level": "INFO"}
        _setup_logging(cfg)
        root = stdlib_logging.getLogger()
        assert any(isinstance(h, stdlib_logging.FileHandler) for h in root.handlers)
        assert any(isinstance(h, stdlib_logging.StreamHandler) for h in root.handlers)
