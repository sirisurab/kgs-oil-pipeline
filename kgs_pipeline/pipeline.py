"""Pipeline orchestration: logging setup, Dask scheduler init, stage chaining, CLI."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from kgs_pipeline.config import load_config

if TYPE_CHECKING:
    import distributed

logger = logging.getLogger(__name__)

_ALL_STAGES = ["acquire", "ingest", "transform", "features"]


# ---------------------------------------------------------------------------
# Task P-02: Logging setup
# ---------------------------------------------------------------------------


def setup_logging(config: dict[str, Any]) -> None:
    """Configure root logger with console and file handlers.

    Args:
        config: Full pipeline config dict containing a 'logging' section.

    Raises:
        KeyError: If 'logging', 'log_file', or 'level' keys are absent.
    """
    if "logging" not in config:
        raise KeyError("'logging' section is absent from config")
    log_cfg = config["logging"]
    if "log_file" not in log_cfg:
        raise KeyError("'log_file' key is absent from config['logging']")
    if "level" not in log_cfg:
        raise KeyError("'level' key is absent from config['logging']")

    log_file = Path(log_cfg["log_file"])
    level_str: str = log_cfg["level"].upper()
    level = getattr(logging, level_str, logging.INFO)

    log_file.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers on repeated calls (e.g. in tests)
    if not any(isinstance(h, logging.FileHandler) for h in root.handlers):
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Task P-03: Dask scheduler initialization
# ---------------------------------------------------------------------------


def init_scheduler(config: dict[str, Any]) -> distributed.Client:
    """Initialize Dask distributed scheduler and return a Client.

    Creates a local cluster when config["dask"]["scheduler"] == "local",
    or connects to a remote scheduler URL otherwise.

    Args:
        config: Full pipeline config dict containing a 'dask' section.

    Returns:
        distributed.Client connected to the scheduler.

    Raises:
        KeyError: If 'dask' or required sub-keys are absent from config.
    """
    from distributed import Client, LocalCluster  # type: ignore[import]

    if "dask" not in config:
        raise KeyError("'dask' section is absent from config")
    dask_cfg = config["dask"]

    for key in ("scheduler", "n_workers", "threads_per_worker", "memory_limit"):
        if key not in dask_cfg:
            raise KeyError(f"'dask.{key}' is absent from config")

    scheduler: str = dask_cfg["scheduler"]
    n_workers: int = dask_cfg["n_workers"]
    threads_per_worker: int = dask_cfg["threads_per_worker"]
    memory_limit: str = dask_cfg["memory_limit"]

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
        )
        client: distributed.Client = Client(cluster)
    else:
        client = Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


# ---------------------------------------------------------------------------
# Task P-04: Pipeline entry point and stage orchestration
# ---------------------------------------------------------------------------


def main(stages: list[str] | None = None) -> None:
    """Run the KGS production pipeline for specified stages (or all).

    Order: acquire → ingest → transform → features.
    Scheduler is initialized after acquire and before ingest.

    Args:
        stages: Optional list of stage names to run. None/empty → all stages.
    """
    config_path = Path("config.yaml")
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {config_path.resolve()}")

    config = load_config(config_path)
    setup_logging(config)

    run_stages = stages if stages else _ALL_STAGES
    logger.info("Pipeline starting — stages: %s", run_stages)

    client = None

    for stage in run_stages:
        t0 = time.monotonic()
        try:
            if stage == "acquire":
                from kgs_pipeline.acquire import acquire

                acquire(config)

                # Initialize Dask scheduler after acquire, before ingest (P-04 step 4)
                if any(s in run_stages for s in ("ingest", "transform", "features")):
                    client = init_scheduler(config)

            elif stage == "ingest":
                from kgs_pipeline.ingest import ingest

                ingest(config)

            elif stage == "transform":
                from kgs_pipeline.transform import transform

                transform(config)

            elif stage == "features":
                from kgs_pipeline.features import features

                features(config)

            else:
                logger.warning("Unknown stage '%s' — skipping", stage)
                continue

            elapsed = time.monotonic() - t0
            logger.info("Stage '%s' completed in %.1fs", stage, elapsed)

        except Exception as exc:
            elapsed = time.monotonic() - t0
            logger.error(
                "Stage '%s' failed after %.1fs: %s",
                stage,
                elapsed,
                exc,
                exc_info=True,
            )
            raise

    if client is not None:
        client.close()

    logger.info("Pipeline complete.")


def _cli() -> None:
    """CLI entry point registered in pyproject.toml."""
    parser = argparse.ArgumentParser(
        prog="kgs-pipeline",
        description="KGS Oil & Gas Production Pipeline",
    )
    parser.add_argument(
        "--stages",
        nargs="*",
        choices=_ALL_STAGES,
        default=None,
        help="Stages to run (default: all)",
    )
    args = parser.parse_args()
    main(stages=args.stages)


if __name__ == "__main__":
    _cli()
