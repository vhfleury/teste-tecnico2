"""Generic staging pipeline driven by source and table config.

This module owns the shared raw -> staging flow for declarative
sources. Source-specific rules stay in ``staging_<source>.json``;
this runner only knows how to read each raw input format, add runtime
metadata, apply the generic treatment/validation engine and write the
staging partition. Both layers are Delta tables: every write replaces
only its own ``ingest_date`` partition and sets the processed marker
right after the commit.
"""
from __future__ import annotations

import logging
import os
from copy import deepcopy
from typing import Any

from data_quality.validation import apply_table_validations, enforce_table_config
from general.delta_io import (
    mark_delta_partition_processed,
    read_delta_partition,
    write_delta_partition,
)
from general.utils import (
    DATA_DIR,
    layer_dir,
    load_table_config,
)
from parser.treatment import apply_derived_columns, apply_table_treatments
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

PIPELINES_DIR = os.path.dirname(__file__)

# Single registry of the declarative sources. ``active`` controls DAG
# generation: a source can stay registered (configs, fixtures, tests)
# while not being scheduled.
STAGING_SOURCES: dict[str, dict[str, Any]] = {
    "veiculos": {
        "active": True,
        "format": "csv",
        "path": os.path.join(DATA_DIR, "veiculos", "veiculos.csv"),
        "read_options": {"header": True, "encoding": "UTF-8"},
        "description": "Vehicle registry ingestion and standardization (CSV -> raw -> staging)",
        "log_label": "CSV",
    },
    "viagens": {
        "active": True,
        "format": "csv",
        "path": os.path.join(DATA_DIR, "viagens", "viagens.csv"),
        "read_options": {"header": True, "encoding": "UTF-8"},
        "description": "Trips fact ingestion and standardization (CSV -> raw -> staging)",
        "log_label": "CSV",
    },
    "motoristas": {
        "active": True,
        "format": "json",
        "path": os.path.join(DATA_DIR, "motoristas", "motoristas.json"),
        "read_options": {"multiLine": True, "primitivesAsString": True},
        "description": "Driver registry ingestion and standardization (JSON -> raw -> staging)",
        "log_label": "JSON",
    },
    "posicoes": {
        "active": True,
        "format": "parquet",
        "path": os.path.join(DATA_DIR, "rastreamento", "posicoes.parquet"),
        "read_options": {},
        "description": "Vehicle tracking positions ingestion and standardization",
        "log_label": "Parquet",
    },
}


def active_sources() -> tuple[str, ...]:
    """List the registered sources enabled for DAG generation.

    Returns:
        Names of the sources whose ``active`` flag is True, in
        registry order.
    """
    return tuple(name for name, config in STAGING_SOURCES.items() if config["active"])


def get_source_config(source: str) -> dict[str, Any]:
    """Return a copy of the registered source config.

    Args:
        source: Source name registered in ``STAGING_SOURCES``.

    Returns:
        Source config with input format, path and read options.

    Raises:
        ValueError: If the source is not registered for the generic
            staging runner.
    """
    if source not in STAGING_SOURCES:
        registered_sources = ", ".join(sorted(STAGING_SOURCES))
        raise ValueError(
            f"Unknown staging source '{source}'. Registered sources: {registered_sources}"
        )
    return deepcopy(STAGING_SOURCES[source])


def raw_dir_for(source: str) -> str:
    """Build the raw layer base directory for a registered source."""
    get_source_config(source)
    return layer_dir("raw", source)


def staging_dir_for(source: str) -> str:
    """Build the staging layer base directory for a registered source."""
    get_source_config(source)
    return layer_dir("staging", source)


def table_config_path(source: str) -> str:
    """Build the staging table config path for a registered source."""
    get_source_config(source)
    return os.path.join(PIPELINES_DIR, source, f"staging_{source}.json")


def _read_source(spark: SparkSession, source: str) -> DataFrame:
    """Read the registered source input without changing values."""
    source_config = get_source_config(source)
    source_format = source_config["format"]
    source_path = source_config["path"]
    reader = spark.read
    for option, value in source_config.get("read_options", {}).items():
        reader = reader.option(option, value)

    if source_format == "csv":
        return reader.csv(source_path)
    if source_format == "json":
        return reader.json(source_path)
    if source_format == "parquet":
        return reader.parquet(source_path)

    raise ValueError(f"Unsupported source format '{source_format}' for source '{source}'")


def extract_to_raw(spark: SparkSession, source: str, ingest_date: str) -> dict:
    """Read a registered input source and write it to raw unchanged.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        source: Source name registered in ``STAGING_SOURCES``.
        ingest_date: Ingestion date in ``YYYY-MM-DD`` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination table,
        ingestion date and record count.
    """
    source_config = get_source_config(source)
    source_path = source_config["path"]
    log_label = source_config["log_label"]

    log.info("Starting extraction for %s - reading %s from %s", source, log_label, source_path)
    df = _read_source(spark, source)
    df = df.withColumn("source_file", F.lit(source_path)).withColumn(
        "ingested_at",
        F.current_timestamp(),
    )

    total = df.count()
    log.info("%s read for %s: %d records, %d columns", log_label, source, total, len(df.columns))
    destination = raw_dir_for(source)
    log.info("Writing raw layer for %s to %s (Delta)", source, destination)
    write_delta_partition(df, destination, ingest_date)
    mark_delta_partition_processed(destination, ingest_date)
    log.info("Extraction finished for %s - %d records written to raw", source, total)

    return {
        "layer": "raw",
        "source": source,
        "path": destination,
        "ingest_date": ingest_date,
        "records": total,
    }


def clean_and_validate(raw: DataFrame, config: dict) -> DataFrame:
    """Standardize, validate and derive columns as the config declares.

    Args:
        raw: Raw source DataFrame, as read from the raw layer.
        config: Parsed table config (the staging contract).

    Returns:
        The cleaned DataFrame with quarantine flags and a
        ``processed_at`` timestamp column.
    """
    df = apply_table_treatments(raw, config)
    df = apply_table_validations(df, config)
    df = apply_derived_columns(df, config)
    return df.withColumn("processed_at", F.current_timestamp())


def transform_to_staging(spark: SparkSession, source: str, ingest_date: str) -> dict:
    """Read raw, apply the source table config and write staging.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        source: Source name registered in ``STAGING_SOURCES``.
        ingest_date: Ingestion date in ``YYYY-MM-DD`` format, used
            to locate raw and staging partitions.

    Returns:
        Metrics about the write: layer name, destination table,
        ingestion date, input/output record counts and how many were
        flagged for quality.
    """
    raw_source = raw_dir_for(source)
    log.info(
        "Starting transform for %s - reading raw layer from %s (ingest_date=%s)",
        source,
        raw_source,
        ingest_date,
    )
    raw = read_delta_partition(spark, raw_source, ingest_date)
    total_in = raw.count()
    log.info("Raw layer read for %s: %d records", source, total_in)

    config = load_table_config(table_config_path(source))
    df = clean_and_validate(raw, config)
    df = enforce_table_config(df, config)
    log.info(
        "Treatments applied and schema validated against table config '%s'",
        config["table_name"],
    )

    destination = staging_dir_for(source)
    log.info("Writing staging layer for %s to %s (Delta)", source, destination)
    write_delta_partition(df, destination, ingest_date)
    mark_delta_partition_processed(destination, ingest_date)

    total_out = df.count()
    flagged = df.filter(~F.col("quality_ok")).count()

    dropped = total_in - total_out
    if dropped:
        log.info("%d record(s) dropped for %s (missing key or duplicate)", dropped, source)

    if flagged:
        reasons = (
            df.filter(~F.col("quality_ok"))
            .select(F.explode(F.split("dq_observations", ";")).alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
        for row in sorted(reasons, key=lambda item: item["reason"]):
            log.info("  quality - %s: %d", row["reason"], row["count"])

    log.info(
        "Transform finished for %s - %d in staging, %d flagged for quality",
        source,
        total_out,
        flagged,
    )
    return {
        "layer": "staging",
        "source": source,
        "path": destination,
        "ingest_date": ingest_date,
        "records_in": total_in,
        "records_out": total_out,
        "records_flagged": flagged,
    }
