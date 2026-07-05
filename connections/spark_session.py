"""Factory for local SparkSession instances used by the pipelines."""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def get_spark(
    app_name: str, master: str = "local[*]", *, enable_delta: bool = False
) -> SparkSession:
    """Create or reuse a local SparkSession for small datasets.

    ``getOrCreate`` reuses any session already alive in the process and
    ignores new configs, so never mix Delta and non-Delta sessions in
    the same process (DAG tasks are safe: `run_spark` creates and stops
    one session per call).

    Args:
        app_name: Name shown for the Spark application in the UI
            and logs.
        master: Spark master URL. Tests pass `local[1]` to avoid
            spawning one Python worker per core on Windows.
        enable_delta: When True, register the Delta Lake SQL extension
            and catalog. Every lakehouse layer is Delta, so all DAG
            tasks pass True; unit tests exercise pure transforms and
            keep it off. Requires the Delta jars baked into the image
            by the Dockerfile.

    Returns:
        A SparkSession configured to run in local mode.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Ephemeral per-task sessions need no UI; disabling it also stops
        # concurrent tasks from fighting over ports 4040+.
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if enable_delta:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    return builder.getOrCreate()


def run_spark(app_name: str, function, *args, enable_delta: bool = False):
    """Create a SparkSession, run a function and ensure shutdown.

    The session is always stopped in the ``finally`` block, even
    when ``function`` raises, so a failing task doesn't leak the
    JVM process.

    Args:
        app_name: Name passed to `get_spark` for the Spark
            application.
        function: Callable invoked as `function(spark, *args)`.
        *args: Extra positional arguments forwarded to `function`.
        enable_delta: Forwarded to `get_spark`; enables Delta Lake
            support for the lakehouse reads and writes.

    Returns:
        Whatever `function` returns.
    """
    log.info("Creating SparkSession '%s' (enable_delta=%s)", app_name, enable_delta)
    spark = get_spark(app_name, enable_delta=enable_delta)
    try:
        result = function(spark, *args)
    finally:
        spark.stop()
        log.info("SparkSession '%s' stopped", app_name)
    return result
