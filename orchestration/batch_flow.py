import os
from pathlib import Path
import subprocess
from typing import List

from prefect import flow, get_run_logger, task


PROJECT_ROOT = Path("/app")


def _run(cmd: List[str], cwd: Path = PROJECT_ROOT) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


@task(name="batch_to_bronze", retries=2, retry_delay_seconds=60)
def batch_to_bronze() -> None:
    _run(["python", "processing/spark_batch_to_bronze.py"])


@task(name="rebuild_silver_gold", retries=2, retry_delay_seconds=60)
def rebuild_silver_gold() -> None:
    _run(["python", "processing/rebuild_silver_gold.py"])


@flow(name="lakehouse-batch-flow", retries=1, retry_delay_seconds=120)
def run_batch_pipeline() -> None:
    logger = get_run_logger()
    logger.info("Start batch pipeline")

    batch_to_bronze()

    run_full_rebuild = os.getenv("RUN_FULL_REBUILD", "false").lower() == "true"
    if run_full_rebuild:
        rebuild_silver_gold()
    else:
        logger.info("Skip full rebuild (set RUN_FULL_REBUILD=true to enable)")

    logger.info("Batch pipeline done")


if __name__ == "__main__":
    run_batch_pipeline()
