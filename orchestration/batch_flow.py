import os
import subprocess
import sys
from pathlib import Path
from typing import List

from prefect import flow, get_run_logger, task


PROJECT_ROOT = Path("/app")


def _run(cmd: List[str], cwd: Path = PROJECT_ROOT) -> None:
    """Chạy command và stream output real-time"""
    logger = get_run_logger()
    logger.info(f"Executing: {' '.join(cmd)} in {cwd}")

    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True
    )

    # Log stdout
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.info(line)

    # Log stderr (Spark logs go to stderr)
    if result.stderr:
        for line in result.stderr.strip().split("\n")[-20:]:  # Last 20 lines only
            logger.warning(line)

    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd,
            output=result.stdout,
            stderr=result.stderr
        )


@task(name="batch_to_bronze", retries=2, retry_delay_seconds=60)
def batch_to_bronze() -> None:
    """Đọc CSV từ workspace và ghi vào Bronze (Delta Lake trên MinIO)"""
    logger = get_run_logger()

    # Kiểm tra CSV files trước khi chạy Spark
    data_dir = os.getenv("BATCH_DATA_DIR", "/app/infra/workspace")
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")] if os.path.isdir(data_dir) else []

    if not csv_files:
        logger.warning(f"⚠️ Không tìm thấy file CSV trong {data_dir}. Bỏ qua batch ingest.")
        return

    logger.info(f"📂 Tìm thấy {len(csv_files)} file CSV: {csv_files}")
    _run([sys.executable, "processing/spark_batch_to_bronze.py"])
    logger.info("✅ Batch → Bronze hoàn tất!")


@task(name="rebuild_silver_gold", retries=2, retry_delay_seconds=60)
def rebuild_silver_gold() -> None:
    """Rebuild lại Silver + Gold layers từ Bronze data"""
    _run([sys.executable, "processing/rebuild_silver_gold.py"])


@flow(name="lakehouse-batch-flow", retries=1, retry_delay_seconds=120)
def run_batch_pipeline() -> None:
    logger = get_run_logger()
    logger.info("🚀 Start batch pipeline")

    batch_to_bronze()

    run_full_rebuild = os.getenv("RUN_FULL_REBUILD", "false").lower() == "true"
    if run_full_rebuild:
        rebuild_silver_gold()
    else:
        logger.info("ℹ️ Skip full rebuild (set RUN_FULL_REBUILD=true to enable)")

    logger.info("✅ Batch pipeline done")


if __name__ == "__main__":
    run_batch_pipeline()
