"""Data-quality split and reporting helpers shared by every pipeline.

The staging engine splits validated rows by their ``quality_ok`` flag:
only approved rows reach the staging layer, rejected rows are written
to the quarantine layer (``lakehouse/quarantine/<source>/``) with their
``dq_observations`` reasons. This module owns that split and the log
alert that reports a quarantined partition: how many records were
rejected, why, and their share of the total. Nothing here ever changes
values.
"""
from __future__ import annotations

import logging
import os

from general.delta_io import read_delta_partition
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)


def split_by_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split validated rows into approved and rejected by ``quality_ok``.

    Args:
        df: DataFrame already flagged by the validations (must carry
            the ``quality_ok`` column).

    Returns:
        A tuple ``(approved, rejected)``: rows with ``quality_ok``
        True and everything else. A null flag counts as rejected, so
        no row can slip into staging unflagged.
    """
    quality_ok = F.coalesce(F.col("quality_ok"), F.lit(False))
    return df.filter(quality_ok), df.filter(~quality_ok)


def summarize_rejections(rejected: DataFrame) -> list[tuple[str, int]]:
    """Count rejected rows per ``dq_observations`` reason.

    A row rejected for more than one reason (``;``-separated) counts
    once per reason.

    Args:
        rejected: DataFrame with the quarantined rows.

    Returns:
        ``(reason, count)`` pairs sorted by count (highest first),
        then by reason.
    """
    rows = (
        rejected.select(F.explode(F.split("dq_observations", ";")).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    return sorted(
        ((row["reason"], row["count"]) for row in rows),
        key=lambda item: (-item[1], item[0]),
    )


def report_quality_partition(
    spark: SparkSession,
    source: str,
    staging_dir: str,
    quarantine_dir: str,
    ingest_date: str,
) -> dict:
    """Read a partition's staging/quarantine tables and log the alert.

    The alert reports the number of inconsistent (quarantined)
    records, the count per rejection reason and the percentage over
    the partition's total (staging + quarantine). It only reads the
    persisted layers, so the task can be re-run independently of the
    transform that wrote them.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        source: Source name, used only for logging.
        staging_dir: Base directory of the source's staging table.
        quarantine_dir: Base directory of the source's quarantine
            table (may not exist yet for partitions processed before
            the quarantine split).
        ingest_date: Ingestion date in ``YYYY-MM-DD`` format.

    Returns:
        Metrics about the partition: total/rejected record counts,
        rejected percentage and count per reason.
    """
    approved = read_delta_partition(spark, staging_dir, ingest_date).count()

    if os.path.exists(os.path.join(quarantine_dir, "_delta_log")):
        rejected_df = read_delta_partition(spark, quarantine_dir, ingest_date)
        rejected = rejected_df.count()
    else:
        log.info("Quarantine table not found at %s - nothing was rejected yet", quarantine_dir)
        rejected_df = None
        rejected = 0

    total = approved + rejected
    rejected_pct = (rejected / total * 100) if total else 0.0

    reasons: list[tuple[str, int]] = []
    if rejected:
        reasons = summarize_rejections(rejected_df)
        log.warning(
            "DATA QUALITY ALERT for %s (ingest_date=%s): %d inconsistent record(s) "
            "quarantined - %.2f%% of %d total record(s)",
            source,
            ingest_date,
            rejected,
            rejected_pct,
            total,
        )
        for reason, count in reasons:
            log.warning(
                "  reason - %s: %d record(s) (%.2f%% of total)",
                reason,
                count,
                count / total * 100,
            )
    else:
        log.info(
            "Data quality check for %s (ingest_date=%s): no inconsistent records "
            "out of %d total record(s)",
            source,
            ingest_date,
            total,
        )

    return {
        "layer": "quarantine",
        "source": source,
        "path": quarantine_dir,
        "ingest_date": ingest_date,
        "records_total": total,
        "records_rejected": rejected,
        "rejected_pct": round(rejected_pct, 2),
        "reasons": dict(reasons),
    }
