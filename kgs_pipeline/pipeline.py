"""Pipeline orchestrator: coordinates all four pipeline stages."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import yaml
from dask.distributed import Client

# Stage runners — imported at module level so tests can patch them cleanly
from kgs_pipeline.acquire import run_acquire
from kgs_pipeline.features import run_features
from kgs_pipeline.ingest import run_ingest
from kgs_pipeline.transform import run_transform

logger = logging.getLogger(__name__)

_ALL_STAGES = ["acquire", "ingest", "transform", "features"]


def _setup_logging(log_cfg: dict) -> None:
    """Configure dual-channel logging (console + file) from config.logging section."""
    log_file = log_cfg.get("log_file", "logs/pipeline.log")
    level_str = log_cfg.get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid adding duplicate handlers on repeated calls (e.g. in tests)
    if root.handlers:
        root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(fmt)
    root.addHandler(console)

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)


def _init_dask_client(dask_cfg: dict) -> Client:
    """Initialize a Dask distributed client from config.dask settings."""
    from dask.distributed import LocalCluster

    scheduler = dask_cfg.get("scheduler", "local")
    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=int(dask_cfg.get("n_workers", 4)),
            threads_per_worker=int(dask_cfg.get("threads_per_worker", 1)),
            memory_limit=str(dask_cfg.get("memory_limit", "3GB")),
            dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
        )
        client = Client(cluster)
    else:
        client = Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def main() -> None:
    """CLI entry point for the KGS pipeline."""
    parser = argparse.ArgumentParser(description="KGS Oil & Gas Production Pipeline")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=_ALL_STAGES,
        default=_ALL_STAGES,
        help="Stages to run (default: all four in order)",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        config: dict = yaml.safe_load(f)

    _setup_logging(config.get("logging", {}))
    logger.info("Pipeline starting — stages: %s", args.stages)

    stages_to_run: list[str] = args.stages

    # Acquire (I/O-bound, no distributed client)
    if "acquire" in stages_to_run:
        logger.info("Starting acquire stage")
        t0 = time.time()
        run_acquire(config["acquire"])
        logger.info("Acquire completed in %.1fs", time.time() - t0)

    # Initialize Dask distributed client after acquire, before ingest
    client = None
    if any(s in stages_to_run for s in ["ingest", "transform", "features"]):
        try:
            client = _init_dask_client(config.get("dask", {}))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dask client init failed (%s) — continuing without cluster", exc)

    try:
        if "ingest" in stages_to_run:
            logger.info("Starting ingest stage")
            t0 = time.time()
            try:
                run_ingest(config["ingest"])
                logger.info("Ingest completed in %.1fs", time.time() - t0)
            except Exception as exc:
                logger.error("Ingest failed: %s", exc)
                raise SystemExit(1) from exc

        if "transform" in stages_to_run:
            logger.info("Starting transform stage")
            t0 = time.time()
            try:
                run_transform(config["transform"])
                logger.info("Transform completed in %.1fs", time.time() - t0)
            except Exception as exc:
                logger.error("Transform failed: %s", exc)
                raise SystemExit(1) from exc

        if "features" in stages_to_run:
            logger.info("Starting features stage")
            t0 = time.time()
            try:
                run_features(config["features"])
                logger.info("Features completed in %.1fs", time.time() - t0)
            except Exception as exc:
                logger.error("Features failed: %s", exc)
                raise SystemExit(1) from exc

    finally:
        if client is not None:
            try:
                client.close()  # type: ignore[attr-defined]
            except Exception:
                pass

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
