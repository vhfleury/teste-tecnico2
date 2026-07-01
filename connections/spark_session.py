"""Factory for local SparkSession instances used by the pipelines."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create or reuse a local SparkSession for small datasets.

    Args:
        app_name: Name shown for the Spark application in the UI
            and logs.

    Returns:
        A SparkSession configured to run in local mode.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run_spark(app_name: str, function, *args):
    """Create a SparkSession, run a function and ensure shutdown.

    The session is always stopped in the ``finally`` block, even
    when ``function`` raises, so a failing task doesn't leak the
    JVM process.

    Args:
        app_name: Name passed to `get_spark` for the Spark
            application.
        function: Callable invoked as `function(spark, *args)`.
        *args: Extra positional arguments forwarded to `function`.

    Returns:
        Whatever `function` returns.
    """
    spark = get_spark(app_name)
    try:
        result = function(spark, *args)
    finally:
        spark.stop()
    return result
