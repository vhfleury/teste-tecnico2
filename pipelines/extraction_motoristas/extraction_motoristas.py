"""extraction_motoristas pipeline: the driver registry.

Two stages, one task each:

* ``extract_to_raw``      — reads the source JSON (unchanged) and writes it
  to the raw layer.
* ``transform_to_staging`` — reads the raw layer, cleans/validates it
  (`clean_and_validate`), applies the table-config treatments and writes
  the result to the staging layer.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from parser.parser_cnh import cnh_category_is_valid, cnh_is_valid
from parser.parser_cpf import cpf_is_valid
from parser.treatment import apply_table_treatments, deduplicate_by_key, trim_columns
from parser.validation import VALID_DRIVER_STATUS, apply_quarantine, enforce_table_config
from scripts.utils import (
    DATA_DIR,
    load_table_config,
    raw_dir,
    raw_path,
    staging_dir,
    staging_path,
)

log = logging.getLogger(__name__)

SOURCE = "motoristas"

MOTORISTAS_JSON = os.path.join(DATA_DIR, SOURCE, f"{SOURCE}.json")
RAW_DIR = raw_dir(SOURCE)
STAGING_DIR = staging_dir(SOURCE)

# Table config (contract) of the staging table written by this pipeline.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"staging_{SOURCE}.json")


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
    log.info("Starting extraction - reading JSON from %s", MOTORISTAS_JSON)
    df = (
        spark.read.option("multiLine", True)
        .option("primitivesAsString", True)
        .json(MOTORISTAS_JSON)
    )
    df = df.withColumn("source_file", F.lit(MOTORISTAS_JSON)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("JSON read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(RAW_DIR, ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}


def clean_and_validate(raw: DataFrame) -> DataFrame:
    """Standardize types, deduplicate and flag data-quality issues.

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be unit-tested with synthetic data. Uses a
    quarantine strategy: an invalid value (e.g. a CPF failing its
    check digits) is not dropped - the primary key is preserved (so
    joins with `viagens` keep working), the bad value is nulled out
    and the reason is recorded in `dq_observations`.

    Args:
        raw: Raw driver registry DataFrame, as read from the raw
            layer.

    Returns:
        The cleaned DataFrame with quarantine flags and a
        `processed_at` timestamp column.
    """
    df = trim_columns(
        raw,
        [
            "motorista_id",
            "nome",
            "cpf",
            "cnh",
            "categoria_cnh",
            "telefone",
            "base_operacional",
            "status",
        ],
    )
    df = (
        df.withColumn("motorista_id", F.upper(F.col("motorista_id")))
        .withColumn("categoria_cnh", F.upper(F.col("categoria_cnh")))
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("validade_cnh", F.to_date("validade_cnh", "yyyy-MM-dd"))
        .withColumn("data_admissao", F.to_date("data_admissao", "yyyy-MM-dd"))
    )

    df = deduplicate_by_key(df, ["motorista_id"])

    checks = {
        "missing_name": F.col("nome").isNotNull() & (F.col("nome") != ""),
        "invalid_cpf": cpf_is_valid(F.col("cpf")),
        "invalid_cnh": cnh_is_valid(F.col("cnh")),
        "invalid_cnh_category": cnh_category_is_valid(F.col("categoria_cnh")),
        "invalid_status": F.col("status").isin(VALID_DRIVER_STATUS),
        "missing_cnh_expiry": F.col("validade_cnh").isNotNull(),
        "missing_admission_date": F.col("data_admissao").isNotNull(),
    }
    df = apply_quarantine(df, checks)

    # Quarantine: null out invalid values but keep the row (preserves the PK for joins).
    df = (
        df.withColumn("nome", F.when(checks["missing_name"], F.col("nome")))
        .withColumn("cpf", F.when(checks["invalid_cpf"], F.col("cpf")))
        .withColumn("cnh", F.when(checks["invalid_cnh"], F.col("cnh")))
    )

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

    # The table config is the staging contract: declared treatments are applied
    # and wrong columns/types abort the write.
    config = load_table_config(TABLE_CONFIG)
    df = apply_table_treatments(clean_and_validate(raw), config)
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
