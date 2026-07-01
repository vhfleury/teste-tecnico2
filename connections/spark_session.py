"""Factory for SparkSession instances used by the pipelines (local mode)."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local SparkSession configured for small datasets."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run_spark(app_name: str, function, *args):
    """Create a SparkSession, run ``function(spark, *args)`` and ensure shutdown.

    Returns whatever ``function`` returns, always stopping the session in the
    ``finally`` block so a failing task doesn't leak the JVM process.
    """
    spark = get_spark(app_name)
    try:
        result = function(spark, *args)
    finally:
        spark.stop()
    return result
