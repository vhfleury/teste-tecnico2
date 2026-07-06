"""Data-quality split and reporting helpers shared by every pipeline."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
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
    """Count rejected rows per ``dq_observations`` reason (once per reason).

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


def rejection_metrics(rejected: DataFrame, approved_count: int) -> dict:
    """Measure a partition's rejected rows before they are discarded.

    Args:
        rejected: DataFrame with the rows about to be discarded.
        approved_count: Number of rows written to staging, used to
            compute the partition total.

    Returns:
        Metrics about the rejections: rejected record count,
        percentage over the partition total and count per reason.
    """
    rejected_count = rejected.count()
    total = approved_count + rejected_count
    rejected_pct = (rejected_count / total * 100) if total else 0.0
    reasons = summarize_rejections(rejected) if rejected_count else []
    return {
        "records_rejected": rejected_count,
        "rejected_pct": round(rejected_pct, 2),
        "reasons": dict(reasons),
    }


def log_rejection_alert(metrics: dict) -> None:
    """Log the data-quality alert from a staging transform's metrics.

    Args:
        metrics: Transform metrics with ``source``, ``ingest_date``,
            ``records_out``, ``records_rejected``, ``rejected_pct``
            and ``reasons``.
    """
    source = metrics["source"]
    ingest_date = metrics["ingest_date"]
    rejected = metrics["records_rejected"]
    total = metrics["records_out"] + rejected

    if not rejected:
        log.info(
            "Data quality check for %s (ingest_date=%s): no inconsistent records "
            "out of %d total record(s)",
            source,
            ingest_date,
            total,
        )
        return

    log.warning(
        "DATA QUALITY ALERT for %s (ingest_date=%s): %d inconsistent record(s) "
        "discarded - %.2f%% of %d total record(s)",
        source,
        ingest_date,
        rejected,
        metrics["rejected_pct"],
        total,
    )
    for reason, count in metrics["reasons"].items():
        log.warning(
            "  reason - %s: %d record(s) (%.2f%% of total)",
            reason,
            count,
            count / total * 100,
        )
