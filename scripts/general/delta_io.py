"""Delta Lake write and idempotency helpers for the gold layer.

Gold tables are Delta, so two staging conventions do not apply here:

- Spark does not write a ``_SUCCESS`` marker on Delta commits, so the
  cheap pre-Spark skip used by the DAGs (`partition_processed`) is
  replaced by a manual marker under ``<base_dir>/_markers/``.
- Idempotency does not depend on the marker: every write replaces only
  its own ``ingest_date`` partition atomically via ``replaceWhere``, so
  a stale or missing marker can never corrupt the table.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

MARKERS_DIR = "_markers"


def write_delta_partition(df: DataFrame, base_dir: str, ingest_date: str) -> None:
    """Atomically overwrite one ``ingest_date`` partition of a Delta table.

    The partition column is added here, after `enforce_table_config`
    ran (the contract check drops undeclared columns, and partition
    columns only materialize at write time).

    Args:
        df: DataFrame already validated against the table config.
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


def _marker_path(base_dir: str, ingest_date: str) -> str:
    """Build the processed-marker path for a gold partition.

    The marker lives in an underscore-prefixed directory so Delta and
    Spark readers ignore it (like ``_delta_log``).

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The marker file path for that partition.
    """
    return os.path.join(base_dir, MARKERS_DIR, ingest_date)


def gold_partition_processed(base_dir: str, ingest_date: str) -> bool:
    """Check whether a gold partition was already written successfully.

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        True if the partition's processed marker exists.
    """
    return os.path.exists(_marker_path(base_dir, ingest_date))


def mark_gold_partition_processed(base_dir: str, ingest_date: str) -> None:
    """Write the processed marker after a successful Delta commit.

    Args:
        base_dir: Base directory of the Delta table.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.
    """
    marker = _marker_path(base_dir, ingest_date)
    os.makedirs(os.path.dirname(marker), exist_ok=True)
    with open(marker, "w", encoding="utf-8"):
        pass
    log.info("Gold partition marker written: %s", marker)
