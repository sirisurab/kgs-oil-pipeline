"""Pipeline orchestrator — run all stages in order."""

from __future__ import annotations

import logging
import time
from typing import Any

from kgs_pipeline.config import load_config

logger = logging.getLogger(__name__)


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def _init_dask_client(config: dict) -> Any:
    """Initialise Dask distributed client from config.

    Returns:
        Dask distributed Client.
    """
    from distributed import Client, LocalCluster  # type: ignore[import-untyped]

    dask_cfg = config.get("dask", {})
    scheduler = dask_cfg.get("scheduler", "local")
    n_workers = int(dask_cfg.get("n_workers", 2))
    threads_per_worker = int(dask_cfg.get("threads_per_worker", 2))
    memory_limit = dask_cfg.get("memory_limit", "2GB")
    dashboard_port = int(dask_cfg.get("dashboard_port", 8787))

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            dashboard_address=f":{dashboard_port}",
        )
        client = Client(cluster)
    else:
        client = Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def run_pipeline(stages: list[str] | None = None, config_path: str | None = None) -> None:
    """Run the full KGS pipeline or a subset of stages.

    Args:
        stages: Ordered list of stage names to run. Defaults to all four.
        config_path: Path to config.yaml. Defaults to repo-root config.yaml.
    """
    config = load_config(config_path) if config_path else load_config()
    log_cfg = config.get("logging", {})
    _setup_logging(log_cfg.get("level", "INFO"))

    if stages is None:
        stages = ["acquire", "ingest", "transform", "features"]

    client: Any = None

    for stage in stages:
        t0 = time.time()
        logger.info("=== Starting stage: %s ===", stage)
        try:
            if stage == "acquire":
                from kgs_pipeline.acquire import acquire  # noqa: PLC0415
                acquire(config)

            elif stage == "ingest":
                if client is None:
                    client = _init_dask_client(config)
                from kgs_pipeline.ingest import ingest  # noqa: PLC0415
                ingest(config, client)

            elif stage == "transform":
                if client is None:
                    client = _init_dask_client(config)
                from kgs_pipeline.transform import transform  # noqa: PLC0415
                transform(config, client)

            elif stage == "features":
                if client is None:
                    client = _init_dask_client(config)
                from kgs_pipeline.features import features  # noqa: PLC0415
                features(config, client)

            else:
                logger.error("Unknown stage: %s", stage)
                raise ValueError(f"Unknown stage: {stage}")

            elapsed = time.time() - t0
            logger.info("=== Stage '%s' completed in %.1fs ===", stage, elapsed)

        except Exception as exc:
            logger.error("Stage '%s' failed: %s", stage, exc, exc_info=True)
            raise

    if client is not None:
        client.close()


def main() -> None:
    """CLI entry point registered in pyproject.toml."""
    import argparse

    parser = argparse.ArgumentParser(description="KGS Production Data Pipeline")
    parser.add_argument(
        "--stages",
        nargs="*",
        default=None,
        help="Stages to run (acquire ingest transform features). Default: all.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml. Default: repo-root config.yaml.",
    )
    args = parser.parse_args()
    run_pipeline(stages=args.stages, config_path=args.config)


if __name__ == "__main__":
    main()
