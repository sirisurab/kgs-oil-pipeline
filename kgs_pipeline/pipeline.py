"""Pipeline orchestration: config loading, logging, Dask scheduler, CLI entry point."""

import argparse
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import distributed
import yaml

import kgs_pipeline.acquire as _acquire_mod
import kgs_pipeline.features as _features_mod
import kgs_pipeline.ingest as _ingest_mod
import kgs_pipeline.transform as _transform_mod

logger = logging.getLogger(__name__)

_REQUIRED_SECTIONS = {"acquire", "ingest", "transform", "features", "dask", "logging"}
_ALL_STAGES = ["acquire", "ingest", "transform", "features"]
_DASK_STAGES = {"ingest", "transform", "features"}


def load_config(config_path: str) -> dict:
    """Read config.yaml and return its contents as a dict."""
    with open(config_path) as fh:
        config: dict = yaml.safe_load(fh)
    missing = _REQUIRED_SECTIONS - set(config.keys())
    if missing:
        raise KeyError(f"config.yaml missing required sections: {missing}")
    return config


def setup_logging(config: dict) -> None:
    """Configure dual-channel logging (console + file) from config settings."""
    log_file: str = config["logging"]["log_file"]
    level_str: str = config["logging"]["level"]
    level = getattr(logging, level_str.upper(), logging.INFO)

    log_dir = os.path.dirname(log_file)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    ):
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        root.addHandler(ch)

    if not any(isinstance(h, logging.FileHandler) for h in root.handlers):
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)


def init_dask_client(config: dict) -> distributed.Client:
    """Initialize a Dask distributed client (local cluster or remote scheduler)."""
    dask_cfg = config["dask"]
    scheduler: str = dask_cfg["scheduler"]

    if scheduler == "local":
        cluster = distributed.LocalCluster(
            n_workers=dask_cfg["n_workers"],
            threads_per_worker=dask_cfg["threads_per_worker"],
            memory_limit=dask_cfg["memory_limit"],
            dashboard_address=f":{dask_cfg['dashboard_port']}",
        )
        client = distributed.Client(cluster)
    else:
        client = distributed.Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def main(stages: list[str] | None = None, *, _from_cli: bool = False) -> None:
    """CLI entry point for the KGS pipeline.

    Runs the requested stages in order. Defaults to all four when stages is None.
    Initializes the Dask distributed scheduler only when a CPU-bound stage is present.
    Stops on stage failure — downstream stages are not run.
    """
    config_path = "config.yaml"
    if _from_cli:
        parser = argparse.ArgumentParser(description="KGS Oil Production Pipeline")
        parser.add_argument(
            "stages",
            nargs="*",
            help="Stage(s) to run: acquire ingest transform features (default: all)",
        )
        parser.add_argument(
            "--config",
            default="config.yaml",
            help="Path to config.yaml (default: config.yaml)",
        )
        parsed = parser.parse_args()
        requested: list[str] = parsed.stages if parsed.stages else _ALL_STAGES
        config_path = parsed.config
    else:
        requested = stages if stages else _ALL_STAGES

    config = load_config(config_path)
    setup_logging(config)

    logger.info("Pipeline starting: stages=%s", requested)

    client: distributed.Client | None = None
    needs_dask = any(s in _DASK_STAGES for s in requested)
    if needs_dask:
        client = init_dask_client(config)

    try:
        for stage in requested:
            # Look up function through module reference at call time so
            # unittest.mock.patch can intercept the attribute.
            fn: Callable[[dict[Any, Any]], Any]
            if stage == "acquire":
                fn = _acquire_mod.acquire
            elif stage == "ingest":
                fn = _ingest_mod.ingest
            elif stage == "transform":
                fn = _transform_mod.transform
            elif stage == "features":
                fn = _features_mod.features
            else:
                raise ValueError(f"Unknown stage: {stage}")

            t0 = time.time()
            logger.info("Stage '%s' starting", stage)
            try:
                fn(config)
                elapsed = time.time() - t0
                logger.info("Stage '%s' completed in %.1fs", stage, elapsed)
            except Exception as exc:
                elapsed = time.time() - t0
                logger.error("Stage '%s' failed after %.1fs: %s", stage, elapsed, exc)
                raise
    finally:
        if client is not None:
            client.close()


def _cli_entry() -> None:
    """Entry point invoked by the console script."""
    main(_from_cli=True)


if __name__ == "__main__":
    _cli_entry()
