"""extraction_motoristas pipeline: ingestion of the driver registry.

First stage, one task:

* ``extract_to_raw`` — reads the source JSON (unchanged) and writes it to
  the raw layer.

The staging stage (``transform_to_staging``) will be added next.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import statics
from .extraction_motoristas_parser import raw_path

log = logging.getLogger(__name__)


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read motoristas.json and write it to the raw layer, unchanged.

    The source is a JSON array, so the file is read with ``multiLine``
    enabled. Every primitive is read as a string
    (``primitivesAsString``), faithful to the source, so no dirty
    value (e.g. a malformed phone number) is masked by an early cast.
    Typing happens in the staging stage.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
    """
    log.info("Starting extraction - reading JSON from %s", statics.MOTORISTAS_JSON)
    df = (
        spark.read.option("multiLine", True)
        .option("primitivesAsString", True)
        .json(statics.MOTORISTAS_JSON)
    )
    df = df.withColumn("source_file", F.lit(statics.MOTORISTAS_JSON)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("JSON read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}
