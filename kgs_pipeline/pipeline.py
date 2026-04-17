"""Pipeline entry point: orchestrates all stages in sequence."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_ALL_STAGES = ["acquire", "ingest", "transform", "features"]


def _setup_logging(log_file: str, level: str) -> None:
    """Set up dual-channel logging to console and file (ADR-006)."""
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    root.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(numeric_level)
    root.addHandler(file_handler)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        prog="kgs-pipeline",
        description="KGS oil and gas production data pipeline",
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=_ALL_STAGES,
        default=None,
        metavar="STAGE",
        help="Stages to run (default: all). Choices: acquire ingest transform features",
    )
    return parser.parse_args(argv)


def _load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """Load config.yaml and return as a dict."""
    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _write_pipeline_report(
    report: dict[str, Any],
    output_path: Path,
) -> None:
    """Write the pipeline run summary JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info("Pipeline report written to %s", output_path)


def main(argv: list[str] | None = None) -> None:
    """Top-level pipeline entry point.

    Reads config.yaml, sets up logging, initializes Dask distributed scheduler
    (after acquire), runs requested stages in order, and writes pipeline report.

    Args:
        argv: Optional list of CLI args (uses sys.argv if None).
    """
    args = _parse_args(argv)
    stages_to_run = args.stages if args.stages is not None else _ALL_STAGES

    # Load configuration
    cfg = _load_config()

    # Set up logging
    log_cfg = cfg.get("logging", {})
    _setup_logging(
        log_file=log_cfg.get("log_file", "logs/pipeline.log"),
        level=log_cfg.get("level", "INFO"),
    )
    logger.info("Pipeline starting — stages: %s", stages_to_run)

    run_start = time.time()
    run_timestamp = datetime.now(tz=timezone.utc).isoformat()

    stages_run: list[str] = []
    stages_succeeded: list[str] = []
    stages_failed: list[str] = []
    stage_timings: dict[str, float] = {}

    client: Any = None

    try:
        for stage in stages_to_run:
            if stage not in _ALL_STAGES:
                logger.warning("Unknown stage '%s' — skipping", stage)
                continue

            stages_run.append(stage)
            stage_start = time.time()
            logger.info("Stage '%s' starting", stage)

            try:
                if stage == "acquire":
                    _run_acquire_stage(cfg)

                    # After acquire, initialize Dask distributed scheduler for CPU stages
                    if any(s in stages_to_run for s in ["ingest", "transform", "features"]):
                        client = _init_dask_client(cfg)

                elif stage == "ingest":
                    if client is None:
                        client = _init_dask_client(cfg)
                    _run_ingest_stage(cfg, client)

                elif stage == "transform":
                    if client is None:
                        client = _init_dask_client(cfg)
                    _run_transform_stage(cfg, client)

                elif stage == "features":
                    if client is None:
                        client = _init_dask_client(cfg)
                    _run_features_stage(cfg, client)

                elapsed = time.time() - stage_start
                stage_timings[stage] = elapsed
                stages_succeeded.append(stage)
                logger.info("Stage '%s' completed in %.2fs", stage, elapsed)

            except Exception as exc:
                elapsed = time.time() - stage_start
                stage_timings[stage] = elapsed
                stages_failed.append(stage)
                logger.error("Stage '%s' failed after %.2fs: %s", stage, elapsed, exc)
                # Stop execution on stage failure
                break

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

        total_duration = time.time() - run_start

        report: dict[str, Any] = {
            "run_timestamp": run_timestamp,
            "stages_run": stages_run,
            "stages_succeeded": stages_succeeded,
            "stages_failed": stages_failed,
            "total_duration_seconds": round(total_duration, 3),
            "stage_timings": {k: round(v, 3) for k, v in stage_timings.items()},
        }

        report_path = Path(cfg.get("features", {}).get("output_dir", "data/processed")) / "pipeline_report.json"
        try:
            _write_pipeline_report(report, report_path)
        except Exception as exc:
            logger.error("Failed to write pipeline report: %s", exc)

        logger.info(
            "Pipeline finished — succeeded=%s, failed=%s, total=%.2fs",
            stages_succeeded,
            stages_failed,
            total_duration,
        )


def _init_dask_client(cfg: dict[str, Any]) -> Any:
    """Initialize and return a Dask distributed Client based on config."""
    import distributed

    dask_cfg = cfg.get("dask", {})
    scheduler = dask_cfg.get("scheduler", "local")

    if scheduler == "local":
        cluster = distributed.LocalCluster(
            n_workers=dask_cfg.get("n_workers", 2),
            threads_per_worker=dask_cfg.get("threads_per_worker", 1),
            memory_limit=dask_cfg.get("memory_limit", "2GB"),
            dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
        )
        client = distributed.Client(cluster)
    else:
        client = distributed.Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def _run_acquire_stage(cfg: dict[str, Any]) -> None:
    """Run the acquire stage."""
    from kgs_pipeline.acquire import AcquireConfig, run_acquire

    acq_cfg = cfg.get("acquire", {})
    config = AcquireConfig(
        index_path=Path(acq_cfg["index_path"]),
        output_dir=Path(acq_cfg["output_dir"]),
        min_year=int(acq_cfg.get("min_year", 2024)),
        max_workers=int(acq_cfg.get("max_workers", 5)),
        request_timeout=int(acq_cfg.get("request_timeout", 30)),
        monthsave_base_url=acq_cfg.get(
            "monthsave_base_url",
            "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave",
        ),
        sleep_seconds=float(acq_cfg.get("sleep_seconds", 0.5)),
    )
    summary = run_acquire(config)
    logger.info("Acquire summary: %s", summary)


def _run_ingest_stage(cfg: dict[str, Any], client: Any) -> None:
    """Run the ingest stage."""
    from kgs_pipeline.ingest import IngestConfig, run_ingest

    ing_cfg = cfg.get("ingest", {})
    config = IngestConfig(
        input_dir=Path(ing_cfg.get("input_dir", "data/raw")),
        output_dir=Path(ing_cfg.get("output_dir", "data/interim")),
        dict_path=Path(ing_cfg.get("dict_path", "references/kgs_monthly_data_dictionary.csv")),
    )
    summary = run_ingest(config, client)
    logger.info("Ingest summary: %s", summary)


def _run_transform_stage(cfg: dict[str, Any], client: Any) -> None:
    """Run the transform stage."""
    from kgs_pipeline.transform import TransformConfig, run_transform

    tr_cfg = cfg.get("transform", {})
    config = TransformConfig(
        input_dir=Path(tr_cfg.get("input_dir", "data/interim")),
        output_dir=Path(tr_cfg.get("output_dir", "data/interim/transformed")),
        dict_path=Path(tr_cfg.get("dict_path", "references/kgs_monthly_data_dictionary.csv")),
    )
    summary = run_transform(config, client)
    logger.info("Transform summary: %s", summary)


def _run_features_stage(cfg: dict[str, Any], client: Any) -> None:
    """Run the features stage."""
    from kgs_pipeline.features import FeaturesConfig, run_features

    ft_cfg = cfg.get("features", {})
    config = FeaturesConfig(
        input_dir=Path(ft_cfg.get("input_dir", "data/interim/transformed")),
        output_dir=Path(ft_cfg.get("output_dir", "data/processed")),
        manifest_path=Path(
            ft_cfg.get("manifest_path", "data/processed/feature_manifest.json")
        ),
    )
    summary = run_features(config, client)
    logger.info("Features summary: %s", summary)


if __name__ == "__main__":
    main()
