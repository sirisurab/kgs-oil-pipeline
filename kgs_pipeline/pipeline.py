"""
Pipeline orchestration, configuration loading, logging setup, Dask client
initialization, and CLI entry point.

Design decisions per ADRs and build-env-manifest:
- All settings from config.yaml — no hardcoded configurables (build-env-manifest).
- Logging setup before any stage runs, dual-channel (ADR-006).
- Dask distributed client initialized between acquire and ingest (build-env-manifest
  "Dask scheduler initialization").
- Stages acquire, ingest, transform, features run in order; stage failure stops
  execution and downstream stages do not run (build-env-manifest "Pipeline entry point").
- CLI entry point registered in pyproject.toml (build-env-manifest).
- Python 3.11+ (ADR-007).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Required top-level config sections (build-env-manifest "Configuration structure")
_REQUIRED_SECTIONS = ("acquire", "ingest", "transform", "features", "dask", "logging")


# ---------------------------------------------------------------------------
# Task P1: Configuration loader
# ---------------------------------------------------------------------------


def load_config(config_path: str | Path = "config.yaml") -> dict[str, Any]:
    """Load config.yaml and validate required top-level sections.

    No default values are injected — all values come from config.yaml
    per build-env-manifest "Configuration structure".

    Parameters
    ----------
    config_path:
        Path to config.yaml.

    Returns
    -------
    dict
        Parsed configuration with all six required top-level sections.

    Raises
    ------
    FileNotFoundError
        If config_path does not exist.
    KeyError
        If any required section is missing, naming the missing section.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")

    with path.open("r") as fh:
        cfg: dict[str, Any] = yaml.safe_load(fh) or {}

    for section in _REQUIRED_SECTIONS:
        if section not in cfg:
            raise KeyError(f"Required configuration section '{section}' is missing from {path}.")

    return cfg


# ---------------------------------------------------------------------------
# Task P2: Logging setup
# ---------------------------------------------------------------------------


def setup_logging(logging_config: dict[str, Any]) -> None:
    """Configure dual-channel logging (console + file) per ADR-006.

    - Log level and file path read from the logging section of config.yaml.
    - Parent directory of log file created if absent (ADR-006).
    - Must be called exactly once at pipeline startup; stages do not configure
      their own handlers.

    Parameters
    ----------
    logging_config:
        The 'logging' section of the parsed config dict.
    """
    log_file = Path(logging_config["log_file"])
    level_name = logging_config["level"].upper()
    level = getattr(logging, level_name, logging.INFO)

    # Create log directory if absent (ADR-006)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicate output on re-calls in tests
    root_logger.handlers.clear()

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    logging.getLogger(__name__).info("Logging initialized: level=%s, file=%s", level_name, log_file)


# ---------------------------------------------------------------------------
# Task P3: Dask client initialization
# ---------------------------------------------------------------------------


def init_dask_client(dask_config: dict[str, Any]) -> Any:
    """Initialize the Dask distributed scheduler client.

    Per build-env-manifest "Dask scheduler initialization":
    - If dask.scheduler == "local" → initialize local distributed cluster with
      n_workers, threads_per_worker, memory_limit, dashboard_port from config.
    - If dask.scheduler is a URL → connect to the remote scheduler.
    - Dashboard URL is logged after initialization.

    Stages reuse this client; they do not start their own cluster.

    Parameters
    ----------
    dask_config:
        The 'dask' section of the parsed config dict.

    Returns
    -------
    dask.distributed.Client
        Initialized client handle.
    """
    from dask.distributed import Client, LocalCluster

    scheduler = dask_config["scheduler"]

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=dask_config["n_workers"],
            threads_per_worker=dask_config["threads_per_worker"],
            memory_limit=dask_config["memory_limit"],
            dashboard_address=f":{dask_config['dashboard_port']}",
        )
        client = Client(cluster)
    else:
        # Remote scheduler URL
        client = Client(scheduler)

    dashboard_url = client.dashboard_link
    logging.getLogger(__name__).info("Dask dashboard available at: %s", dashboard_url)
    return client


# ---------------------------------------------------------------------------
# Task P4: Pipeline orchestrator and CLI entry point
# ---------------------------------------------------------------------------

_ALL_STAGES = ("acquire", "ingest", "transform", "features")


def run_pipeline(
    stages: list[str] | None = None,
    config_path: str | Path = "config.yaml",
) -> None:
    """Run pipeline stages in order: acquire → ingest → transform → features.

    Per build-env-manifest "Pipeline entry point":
    - Defaults to all four stages when stages is None.
    - Sets up logging before any stage runs.
    - Initializes Dask client between acquire and ingest.
    - Runs each stage with per-stage timing and error logging.
    - Stops on stage failure; downstream stages do not run.

    Parameters
    ----------
    stages:
        Optional list of stage names to run. If None, all four stages run.
    config_path:
        Path to config.yaml.
    """
    config = load_config(config_path)
    setup_logging(config["logging"])

    requested = list(stages) if stages else list(_ALL_STAGES)

    # Validate stage names
    unknown = [s for s in requested if s not in _ALL_STAGES]
    if unknown:
        raise ValueError(f"Unknown stage(s): {unknown}. Valid stages: {_ALL_STAGES}")

    # Ordered stages to run
    ordered = [s for s in _ALL_STAGES if s in requested]

    client: Any = None
    acquire_done = False

    for stage in ordered:
        if stage == "ingest" and "acquire" not in ordered and client is None:
            # Acquire was not requested; still need Dask client before ingest
            client = init_dask_client(config["dask"])
        elif stage == "ingest" and acquire_done and client is None:
            # Acquire just ran; initialize Dask client now
            client = init_dask_client(config["dask"])

        logger.info("Starting stage: %s", stage)
        t_start = time.monotonic()
        try:
            _run_stage(stage, config)
            elapsed = time.monotonic() - t_start
            logger.info("Stage '%s' completed in %.2fs.", stage, elapsed)
        except Exception as exc:
            elapsed = time.monotonic() - t_start
            logger.error("Stage '%s' failed after %.2fs: %s", stage, elapsed, exc, exc_info=True)
            raise

        if stage == "acquire":
            acquire_done = True
            # Initialize Dask client after acquire, before ingest (build-env-manifest)
            if "ingest" in ordered and client is None:
                client = init_dask_client(config["dask"])

    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _run_stage(stage: str, config: dict[str, Any]) -> None:
    """Dispatch to the appropriate stage function."""
    if stage == "acquire":
        from kgs_pipeline.acquire import acquire

        acquire(config)
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
        raise ValueError(f"Unknown stage: {stage!r}")


def main() -> None:
    """CLI entry point: kgs-pipeline [--stages STAGE [STAGE ...]] [--config CONFIG]."""
    parser = argparse.ArgumentParser(description="KGS oil production data pipeline.")
    parser.add_argument(
        "--stages",
        nargs="*",
        choices=list(_ALL_STAGES),
        default=None,
        metavar="STAGE",
        help=(
            "Stages to run (acquire ingest transform features). "
            "Defaults to all four when not specified."
        ),
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml).",
    )
    args = parser.parse_args()
    run_pipeline(stages=args.stages, config_path=args.config)


if __name__ == "__main__":
    main()
