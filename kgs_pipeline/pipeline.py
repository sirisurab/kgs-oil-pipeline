"""Pipeline orchestrator CLI for the KGS oil production data pipeline.

CLI entry point that runs all four pipeline stages (acquire → ingest →
transform → features) in sequence with structured logging and metadata tracking.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from kgs_pipeline.acquire import run_acquire_pipeline
from kgs_pipeline.config import CONFIG
from kgs_pipeline.features import run_features_pipeline
from kgs_pipeline.ingest import run_ingest_pipeline
from kgs_pipeline.transform import run_transform_pipeline
from kgs_pipeline.utils import ensure_dir, setup_logging

logger = setup_logging("pipeline", CONFIG.logs_dir, CONFIG.log_level)

VALID_STAGES = ["acquire", "ingest", "transform", "features"]


def setup_pipeline_logging() -> logging.Logger:
    """Configure and return the pipeline root logger.

    Returns:
        Configured pipeline logger.
    """
    return setup_logging("pipeline", CONFIG.logs_dir, CONFIG.log_level)


def run_pipeline(
    stages: list[str],
    years: list[int] | None,
    workers: int,
) -> dict:
    """Execute pipeline stages in sequence.

    Args:
        stages: List of stage names to run, or ["all"] to run all.
        years: Optional list of years to process (informational for acquire).
        workers: Number of Dask workers.

    Returns:
        Pipeline run metadata dict.
    """

    if stages == ["all"]:
        stages_to_run = VALID_STAGES
    else:
        stages_to_run = [s for s in VALID_STAGES if s in stages]

    start_time = datetime.now(timezone.utc)
    metadata: dict = {
        "start_time": start_time.isoformat(),
        "end_time": None,
        "stages": {},
        "pipeline_metadata_path": str(Path("data/pipeline_metadata.json")),
        "years": years,
        "workers": workers,
    }

    stage_results: dict[str, list[Path]] = {}

    for stage in VALID_STAGES:
        if stage not in stages_to_run:
            continue

        stage_start = datetime.now(timezone.utc)
        logger.info("Starting stage: %s", stage)

        try:
            if stage == "acquire":
                result = run_acquire_pipeline(max_workers=workers)
                stage_results["acquire"] = result or []
            elif stage == "ingest":
                result = run_ingest_pipeline()
                stage_results["ingest"] = result or []
            elif stage == "transform":
                result = run_transform_pipeline()
                stage_results["transform"] = result or []
            elif stage == "features":
                result = run_features_pipeline()
                stage_results["features"] = result or []

            stage_end = datetime.now(timezone.utc)
            files = stage_results.get(stage, [])
            metadata["stages"][stage] = {
                "status": "success",
                "start_time": stage_start.isoformat(),
                "end_time": stage_end.isoformat(),
                "elapsed_seconds": (stage_end - stage_start).total_seconds(),
                "files_written": len(files),
            }
            logger.info("Stage '%s' completed successfully. Files written: %d", stage, len(files))

        except Exception as exc:
            stage_end = datetime.now(timezone.utc)
            logger.error("Stage '%s' failed: %s", stage, exc, exc_info=True)
            metadata["stages"][stage] = {
                "status": "failed",
                "start_time": stage_start.isoformat(),
                "end_time": stage_end.isoformat(),
                "elapsed_seconds": (stage_end - stage_start).total_seconds(),
                "error": str(exc),
            }

    end_time = datetime.now(timezone.utc)
    metadata["end_time"] = end_time.isoformat()
    metadata["total_elapsed_seconds"] = (end_time - start_time).total_seconds()

    return metadata


def main() -> None:
    """CLI entry point for the KGS pipeline."""
    parser = argparse.ArgumentParser(
        description="KGS Oil Production Data Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--stages",
        type=str,
        default="all",
        help="Comma-separated list of stages to run: acquire,ingest,transform,features,all",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help=(
            f"Comma-separated list of years to process "
            f"(default: {CONFIG.year_start}–{CONFIG.year_end})"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=CONFIG.dask_n_workers,
        help="Number of Dask workers",
    )
    args = parser.parse_args()

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    years: list[int] | None = None
    if args.years:
        try:
            years = [int(y.strip()) for y in args.years.split(",") if y.strip()]
        except ValueError:
            logger.error("Invalid years argument: %s", args.years)
            sys.exit(1)

    metadata = run_pipeline(stages=stages, years=years, workers=args.workers)

    ensure_dir(Path("data"))
    metadata_path = Path("data/pipeline_metadata.json")
    metadata_path.write_text(json.dumps(metadata, indent=2, default=str))
    logger.info("Pipeline metadata written to %s", metadata_path)

    any_failed = any(
        s.get("status") == "failed" for s in metadata.get("stages", {}).values()
    )
    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
