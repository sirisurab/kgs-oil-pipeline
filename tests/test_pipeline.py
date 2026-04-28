"""Tests for kgs_pipeline/pipeline.py (Tasks P-01 to P-04)."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from kgs_pipeline.pipeline import init_dask_client, load_config, main, setup_logging

DATA_DICT_PATH = "references/kgs_monthly_data_dictionary.csv"

_FULL_CONFIG = {
    "acquire": {
        "lease_index_path": "data/external/oil_leases_2020_present.txt",
        "raw_dir": "data/raw",
        "target_year": 2024,
        "max_workers": 2,
    },
    "ingest": {
        "raw_dir": "data/raw",
        "interim_dir": "data/interim",
        "data_dict_path": DATA_DICT_PATH,
    },
    "transform": {
        "interim_dir": "data/interim",
        "processed_dir": "data/processed/transform",
        "data_dict_path": DATA_DICT_PATH,
    },
    "features": {
        "processed_dir": "data/processed/transform",
        "output_dir": "data/processed/features",
        "data_dict_path": DATA_DICT_PATH,
    },
    "dask": {
        "scheduler": "local",
        "n_workers": 1,
        "threads_per_worker": 1,
        "memory_limit": "1GB",
        "dashboard_port": 8787,
    },
    "logging": {
        "log_file": "logs/pipeline.log",
        "level": "INFO",
    },
}


def _write_config(tmp_path: Path, config: dict) -> str:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(config), encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# Task P-01: load_config
# ---------------------------------------------------------------------------


def test_load_config_has_required_sections(tmp_path: Path) -> None:
    path = _write_config(tmp_path, _FULL_CONFIG)
    config = load_config(path)
    for key in ("acquire", "ingest", "transform", "features", "dask", "logging"):
        assert key in config


def test_load_config_missing_section_detectable(tmp_path: Path) -> None:
    bad_config = {k: v for k, v in _FULL_CONFIG.items() if k != "dask"}
    path = _write_config(tmp_path, bad_config)
    with pytest.raises(KeyError):
        load_config(path)


# ---------------------------------------------------------------------------
# Task P-02: setup_logging
# ---------------------------------------------------------------------------


def test_setup_logging_has_stream_and_file_handler(tmp_path: Path) -> None:
    config = {
        **_FULL_CONFIG,
        "logging": {
            "log_file": str(tmp_path / "logs" / "pipeline.log"),
            "level": "INFO",
        },
    }
    root = logging.getLogger()
    # Remove pre-existing handlers to isolate test
    original_handlers = root.handlers[:]
    root.handlers.clear()

    setup_logging(config)

    has_stream = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    )
    has_file = any(isinstance(h, logging.FileHandler) for h in root.handlers)
    assert has_stream
    assert has_file

    # Restore
    root.handlers = original_handlers


def test_setup_logging_creates_log_dir(tmp_path: Path) -> None:
    log_file = str(tmp_path / "newdir" / "pipeline.log")
    config = {**_FULL_CONFIG, "logging": {"log_file": log_file, "level": "INFO"}}
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()

    setup_logging(config)

    assert Path(log_file).parent.exists()

    # Cleanup handlers
    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            h.close()
            root.removeHandler(h)
    root.handlers = original_handlers


def test_setup_logging_file_handler_path(tmp_path: Path) -> None:
    log_file = str(tmp_path / "logs" / "test.log")
    config = {**_FULL_CONFIG, "logging": {"log_file": log_file, "level": "DEBUG"}}
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()

    setup_logging(config)

    file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) >= 1
    assert any(log_file in h.baseFilename for h in file_handlers)

    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            h.close()
            root.removeHandler(h)
    root.handlers = original_handlers


# ---------------------------------------------------------------------------
# Task P-03: init_dask_client
# ---------------------------------------------------------------------------


def test_init_dask_client_local_returns_client() -> None:
    config = {
        **_FULL_CONFIG,
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 18787,
        },
    }
    import distributed

    real_client_cls = distributed.Client

    with patch("kgs_pipeline.pipeline.distributed.LocalCluster"):
        with patch("kgs_pipeline.pipeline.distributed.Client") as mock_client_cls:
            mock_client_instance = MagicMock(spec=real_client_cls)
            mock_client_instance.dashboard_link = "http://localhost:18787/status"
            mock_client_cls.return_value = mock_client_instance
            client = init_dask_client(config)
    assert client is mock_client_instance


def test_init_dask_client_logs_dashboard(caplog: pytest.LogCaptureFixture) -> None:
    config = {
        **_FULL_CONFIG,
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 18787,
        },
    }
    import distributed

    real_client_cls = distributed.Client

    with patch("kgs_pipeline.pipeline.distributed.LocalCluster"):
        with patch("kgs_pipeline.pipeline.distributed.Client") as mock_client_cls:
            mock_client_instance = MagicMock(spec=real_client_cls)
            mock_client_instance.dashboard_link = "http://localhost:18787/status"
            mock_client_cls.return_value = mock_client_instance
            with caplog.at_level(logging.INFO, logger="kgs_pipeline.pipeline"):
                init_dask_client(config)
    assert any("dashboard" in r.message.lower() for r in caplog.records)


def test_init_dask_client_url_connects_to_address() -> None:
    config = {
        **_FULL_CONFIG,
        "dask": {
            "scheduler": "tcp://scheduler-host:8786",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
    }
    import distributed

    real_client_cls = distributed.Client

    captured_args: list = []

    def fake_client(*args: object, **kwargs: object) -> MagicMock:
        captured_args.extend(args)
        m = MagicMock(spec=real_client_cls)
        m.dashboard_link = "http://scheduler-host:8787/status"
        return m

    with patch("kgs_pipeline.pipeline.distributed.Client", side_effect=fake_client):
        init_dask_client(config)

    assert "tcp://scheduler-host:8786" in captured_args


# ---------------------------------------------------------------------------
# Task P-04: main (CLI entry point)
# ---------------------------------------------------------------------------


def test_main_calls_stages_in_order(tmp_path: Path) -> None:
    call_order: list[str] = []

    def make_fake(name: str):
        def fake(config: dict) -> None:
            call_order.append(name)

        return fake

    mock_client = MagicMock()
    mock_client.close = MagicMock()

    with patch("kgs_pipeline.pipeline.load_config", return_value=_FULL_CONFIG):
        with patch("kgs_pipeline.pipeline.setup_logging"):
            with patch("kgs_pipeline.pipeline.init_dask_client", return_value=mock_client):
                with patch("kgs_pipeline.pipeline._ingest_mod.ingest", make_fake("ingest")):
                    with patch(
                        "kgs_pipeline.pipeline._transform_mod.transform", make_fake("transform")
                    ):
                        with patch(
                            "kgs_pipeline.pipeline._features_mod.features", make_fake("features")
                        ):
                            main(stages=["ingest", "transform", "features"])

    assert call_order == ["ingest", "transform", "features"]


def test_main_stops_on_stage_failure(tmp_path: Path) -> None:
    call_order: list[str] = []

    def fake_ingest(config: dict) -> None:
        call_order.append("ingest")
        raise RuntimeError("ingest failed")

    def fake_transform(config: dict) -> None:
        call_order.append("transform")

    mock_client = MagicMock()
    mock_client.close = MagicMock()

    with patch("kgs_pipeline.pipeline.load_config", return_value=_FULL_CONFIG):
        with patch("kgs_pipeline.pipeline.setup_logging"):
            with patch("kgs_pipeline.pipeline.init_dask_client", return_value=mock_client):
                with patch("kgs_pipeline.pipeline._ingest_mod.ingest", fake_ingest):
                    with patch("kgs_pipeline.pipeline._transform_mod.transform", fake_transform):
                        with pytest.raises(RuntimeError):
                            main(stages=["ingest", "transform"])

    assert "ingest" in call_order
    assert "transform" not in call_order


def test_main_no_dask_for_acquire_only(tmp_path: Path) -> None:
    acquire_called = [False]

    def fake_acquire(config: dict) -> list:
        acquire_called[0] = True
        return []

    with patch("kgs_pipeline.pipeline.load_config", return_value=_FULL_CONFIG):
        with patch("kgs_pipeline.pipeline.setup_logging"):
            with patch("kgs_pipeline.pipeline.init_dask_client") as mock_dask:
                with patch("kgs_pipeline.pipeline._acquire_mod.acquire", fake_acquire):
                    main(stages=["acquire"])

    mock_dask.assert_not_called()
    assert acquire_called[0]


def test_main_none_stages_runs_all_four(tmp_path: Path) -> None:
    call_order: list[str] = []

    def make_fake(name: str):
        def fake(config: dict) -> None:
            call_order.append(name)

        return fake

    mock_client = MagicMock()
    mock_client.close = MagicMock()

    with patch("kgs_pipeline.pipeline.load_config", return_value=_FULL_CONFIG):
        with patch("kgs_pipeline.pipeline.setup_logging"):
            with patch("kgs_pipeline.pipeline.init_dask_client", return_value=mock_client):
                with patch("kgs_pipeline.pipeline._acquire_mod.acquire", make_fake("acquire")):
                    with patch("kgs_pipeline.pipeline._ingest_mod.ingest", make_fake("ingest")):
                        with patch(
                            "kgs_pipeline.pipeline._transform_mod.transform", make_fake("transform")
                        ):
                            with patch(
                                "kgs_pipeline.pipeline._features_mod.features",
                                make_fake("features"),
                            ):
                                main(stages=None)

    assert set(call_order) == {"acquire", "ingest", "transform", "features"}


@pytest.mark.integration
def test_full_pipeline_end_to_end_no_exceptions(tmp_path: Path) -> None:
    """TR-27: full ingest→transform→features on real data without unhandled exceptions."""
    _CANONICAL_COLS = [
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
        "MONTH-YEAR",
        "PRODUCT",
        "WELLS",
        "PRODUCTION",
    ]
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"

    header = ",".join(_CANONICAL_COLS)
    row = (
        "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
        "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
        "1-2024,O,2,161.8"
    )
    (raw_dir / "lp001.txt").write_text(header + "\n" + row + "\n", encoding="utf-8")

    from kgs_pipeline.features import features as run_features
    from kgs_pipeline.ingest import ingest as run_ingest
    from kgs_pipeline.transform import transform as run_transform

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    run_transform(
        {
            "transform": {
                "interim_dir": str(interim_dir),
                "processed_dir": str(processed_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    run_features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )

    import dask.dataframe as dd

    df = dd.read_parquet(str(features_dir)).compute()
    assert "cum_oil" in df.columns
