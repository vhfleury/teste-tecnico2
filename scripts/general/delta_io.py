"""Delta Lake read/write and idempotency helpers for the lakehouse."""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

MARKERS_DIR = "_markers"


def write_delta_partition(df: DataFrame, base_dir: str, ingest_date: str) -> None:
    """Atomically overwrite one ``ingest_date`` partition of a Delta table.

    Args:
        df: DataFrame with the partition's rows, without the
            ``ingest_date`` column.
        base_dir: Base directory of the Delta table (the whole table,
            not the partition path).
        ingest_date: Ingestion date in `YYYY-MM-DD` format.
    """
    log.info("Writing Delta partition ingest_date=%s to %s", ingest_date, base_dir)
    (
        df.withColumn("ingest_date", F.lit(ingest_date))
        .coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingest_date = '{ingest_date}'")
        .partitionBy("ingest_date")
        .save(base_dir)
    )


def read_delta_partition(spark: SparkSession, base_dir: str, ingest_date: str) -> DataFrame:
    """Read one ``ingest_date`` partition of a Delta table.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        base_dir: Base directory of the Delta table (the whole table,
            not the partition path).
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The partition's rows, without the ``ingest_date`` column.
    """
    return (
        spark.read.format("delta")
        .load(base_dir)
        .filter(F.col("ingest_date") == ingest_date)
        .drop("ingest_date")
    )


def _marker_path(base_dir: str, ingest_date: str) -> str:
    """Build the processed-marker path for a Delta partition.

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The marker file path for that partition.
    """
    return os.path.join(base_dir, MARKERS_DIR, ingest_date)


def delta_partition_processed(base_dir: str, ingest_date: str) -> bool:
    """Check whether a Delta partition was already written successfully.

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        True if the partition's processed marker exists.
    """
    return os.path.exists(_marker_path(base_dir, ingest_date))


def mark_delta_partition_processed(base_dir: str, ingest_date: str) -> None:
    """Write the processed marker after a successful Delta commit.

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.
    """
    marker = _marker_path(base_dir, ingest_date)
    os.makedirs(os.path.dirname(marker), exist_ok=True)
    with open(marker, "w", encoding="utf-8"):
        pass
    log.info("Delta partition marker written: %s", marker)
