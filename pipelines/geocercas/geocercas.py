"""geocercas pipeline: the geofence registry."""
from __future__ import annotations

import logging
import os

from data_quality.validation import apply_table_validations
from general.delta_io import (
    mark_delta_partition_processed,
    write_delta_partition,
)
from general.utils import (
    DATA_DIR,
    layer_dir,
)
from parser.treatment import apply_derived_columns, apply_table_treatments
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from staging_pipeline import run_staging_transform

log = logging.getLogger(__name__)

SOURCE = "geocercas"

GEOCERCAS_GEOJSON = os.path.join(DATA_DIR, SOURCE, f"{SOURCE}.geojson")
RAW_DIR = layer_dir("raw", SOURCE)
STAGING_DIR = layer_dir("staging", SOURCE)

# Table config (contract) of the staging table written by this pipeline.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"staging_{SOURCE}.json")

# properties.* fields promoted to flat staging columns.
PROPERTY_FIELDS = ["geocerca_id", "nome", "tipo", "uf", "raio_km", "ativo"]


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read geocercas.geojson and write it to the raw layer, one row per feature.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination table,
        ingestion date and record count.
    """
    log.info("Starting extraction - reading GeoJSON from %s", GEOCERCAS_GEOJSON)
    collection = (
        spark.read.option("multiLine", True)
        .option("primitivesAsString", True)
        .json(GEOCERCAS_GEOJSON)
    )
    df = collection.select(F.explode("features").alias("feature")).select("feature.*")
    df = df.withColumn("source_file", F.lit(GEOCERCAS_GEOJSON)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("GeoJSON read: %d features, %d columns", total, len(df.columns))
    log.info("Writing raw layer to %s (Delta)", RAW_DIR)
    write_delta_partition(df, RAW_DIR, ingest_date)
    mark_delta_partition_processed(RAW_DIR, ingest_date)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": RAW_DIR, "ingest_date": ingest_date, "records": total}


def _struct_fields(df: DataFrame, column: str) -> list[str]:
    """Names of the nested fields of a struct column (empty when absent).

    Args:
        df: DataFrame to inspect.
        column: Name of the (possibly missing) struct column.

    Returns:
        The nested field names, or an empty list when the column is
        missing or not a struct.
    """
    if column not in df.columns:
        return []
    data_type = df.schema[column].dataType
    if not isinstance(data_type, StructType):
        return []
    return [field.name for field in data_type.fields]


def flatten_features(raw: DataFrame) -> DataFrame:
    """Flatten the GeoJSON feature shape into staging columns.

    Args:
        raw: Raw geofence DataFrame, as read from the raw layer
            (one row per feature).

    Returns:
        The flat DataFrame with one column per property, the
        serialized geometry and the raw metadata columns.
    """
    property_fields = _struct_fields(raw, "properties")
    columns: list[Column] = []
    for field in PROPERTY_FIELDS:
        if field in property_fields:
            columns.append(F.col(f"properties.{field}").alias(field))
        else:
            columns.append(F.lit(None).cast("string").alias(field))

    if {"type", "coordinates"} <= set(_struct_fields(raw, "geometry")):
        canonical = F.to_json(
            F.struct(
                F.col("geometry.type").alias("type"),
                F.col("geometry.coordinates")
                .cast("array<array<array<double>>>")
                .alias("coordinates"),
            )
        )
        geometry = F.when(F.col("geometry").isNotNull(), canonical)
    else:
        geometry = F.lit(None).cast("string")
    columns.append(geometry.alias("geometry"))

    return raw.select(*columns, "source_file", "ingested_at")


def clean_and_validate(raw: DataFrame, config: dict) -> DataFrame:
    """Standardize, validate and derive columns as the config declares.

    Args:
        raw: Raw geofence DataFrame, as read from the raw layer.
        config: Parsed table config (the staging contract).

    Returns:
        The cleaned DataFrame with quarantine flags and a
        `processed_at` timestamp column.
    """
    df = flatten_features(raw)
    df = apply_table_treatments(df, config)
    df = apply_table_validations(df, config)
    df = apply_derived_columns(df, config)
    return df.withColumn("processed_at", F.current_timestamp())


def transform_to_staging(spark: SparkSession, ingest_date: str) -> dict:
    """Read the raw layer, clean/validate it and write staging.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to locate the raw partition and to partition the
            staging layer.

    Returns:
        Metrics about the write: layer name, destination table,
        ingestion date, input/output record counts and how many were
        rejected (with percentage and count per reason).
    """
    return run_staging_transform(
        spark,
        SOURCE,
        ingest_date,
        raw_dir=RAW_DIR,
        staging_dir=STAGING_DIR,
        config_path=TABLE_CONFIG,
        clean=clean_and_validate,
    )
