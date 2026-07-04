"""veiculos pipeline: the vehicle registry.

Two stages, one task each:

* ``extract_to_raw``      — reads the source CSV (unchanged) and writes it
  to the raw layer.
* ``transform_to_staging`` — reads the raw layer, cleans/validates it
  (`clean_and_validate`), applies the table-config treatments and writes
  the result to the staging layer.
"""
from __future__ import annotations

import logging
import os

from data_quality.validation import apply_table_validations, enforce_table_config
from general.utils import (
    DATA_DIR,
    load_table_config,
    raw_dir,
    raw_path,
    staging_dir,
    staging_path,
)
from parser.treatment import apply_derived_columns, apply_table_treatments
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SOURCE = "veiculos"

VEICULOS_CSV = os.path.join(DATA_DIR, SOURCE, f"{SOURCE}.csv")
RAW_DIR = raw_dir(SOURCE)
STAGING_DIR = staging_dir(SOURCE)

# Table config (contract) of the staging table written by this pipeline.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"staging_{SOURCE}.json")


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read veiculos.csv and write it to the raw layer, unchanged.

    Every column is read as a string (no schema inference), faithful
    to the source, so no dirty value (e.g. a negative mileage) is
    masked by an early cast. Typing happens in the staging stage.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
    """
    log.info("Starting extraction - reading CSV from %s", VEICULOS_CSV)
    df = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .csv(VEICULOS_CSV)
    )
    df = df.withColumn("source_file", F.lit(VEICULOS_CSV)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("CSV read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(RAW_DIR, ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}


def clean_and_validate(raw: DataFrame, config: dict) -> DataFrame:
    """Standardize, validate and derive columns as the config declares.

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be unit-tested with synthetic data. Every rule
    lives in the table config: treatments/cast/dedup first, then the
    quarantine validations (invalid values are nulled but the row
    keeps its primary key), and finally the derived columns.

    Args:
        raw: Raw vehicle registry DataFrame, as read from the raw
            layer.
        config: Parsed table config (the staging contract).

    Returns:
        The cleaned DataFrame with quarantine flags and a
        `processed_at` timestamp column.
    """
    df = apply_table_treatments(raw, config)
    df = apply_table_validations(df, config)
    df = apply_derived_columns(df, config)
    return df.withColumn("processed_at", F.current_timestamp())


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
    source = raw_path(RAW_DIR, ingest_date)
    log.info("Starting transform - reading raw layer from %s", source)
    raw = spark.read.parquet(source)
    total_in = raw.count()
    log.info("Raw layer read: %d records", total_in)

    # The table config is the staging contract: declared treatments and
    # validations are applied and wrong columns/types abort the write.
    config = load_table_config(TABLE_CONFIG)
    df = clean_and_validate(raw, config)
    df = enforce_table_config(df, config)
    log.info(
        "Treatments applied and schema validated against table config '%s'",
        config["table_name"],
    )

    destination = staging_path(STAGING_DIR, ingest_date)
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
