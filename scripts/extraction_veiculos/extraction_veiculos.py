"""extraction_veiculos pipeline: raw -> staging for the vehicle registry.

Two stages, one task each:

* ``extract_to_raw``      — reads the raw CSV (unchanged) and writes it to
  the raw layer.
* ``transform_to_staging`` — reads the raw layer, cleans/validates it and
  writes the result to the staging layer.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import statics
from .extraction_veiculos_parser import clean_and_validate, raw_path, staging_path

log = logging.getLogger(__name__)


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read veiculos.csv and write it to the raw layer, unchanged.

    All columns are read as strings, faithful to the source, so no
    dirty value (e.g. a negative mileage) is masked by an early
    cast. Typing happens in the staging stage.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
    """
    log.info("Starting extraction - reading CSV from %s", statics.VEICULOS_CSV)
    df = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .csv(statics.VEICULOS_CSV)
    )
    df = df.withColumn("source_file", F.lit(statics.VEICULOS_CSV)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("CSV read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}


def transform_to_staging(spark: SparkSession, ingest_date: str) -> dict:
    """Read the raw layer, clean/validate it and write staging.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to locate the raw partition and to partition the
            staging layer.

    Returns:
        Metrics about the write: layer name, destination path,
        input/output record counts and how many were flagged for
        quality.
    """
    source = raw_path(ingest_date)
    log.info("Starting transform - reading raw layer from %s", source)
    raw = spark.read.parquet(source)
    total_in = raw.count()
    log.info("Raw layer read: %d records", total_in)

    df = clean_and_validate(raw)

    destination = staging_path(ingest_date)
    log.info("Writing staging layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)

    total_out = df.count()
    flagged = df.filter(~F.col("quality_ok")).count()

    dropped = total_in - total_out
    if dropped:
        log.info("%d record(s) dropped (missing key or duplicate)", dropped)

    if flagged:
        reasons = (
            df.filter(~F.col("quality_ok"))
            .select(F.explode(F.split("dq_observations", ";")).alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
        for row in sorted(reasons, key=lambda r: r["reason"]):
            log.info("  quality - %s: %d", row["reason"], row["count"])

    log.info(
        "Transform finished - %d in staging, %d flagged for quality",
        total_out,
        flagged,
    )
    return {
        "layer": "staging",
        "path": destination,
        "records_in": total_in,
        "records_out": total_out,
        "records_flagged": flagged,
    }
