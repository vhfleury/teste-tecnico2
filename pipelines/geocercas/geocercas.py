"""geocercas pipeline: the geofence registry.

Two stages, one task each:

* ``extract_to_raw``      — reads the source GeoJSON (unchanged values) and
  writes it to the raw layer, one row per feature.
* ``transform_to_staging`` — reads the raw layer, flattens the GeoJSON
  feature shape (`flatten_features`, exclusive treatment of this source),
  cleans/validates it (`clean_and_validate`), applies the table-config
  treatments and writes the result to the staging layer. Only rows with
  ``quality_ok`` True reach staging; rejected rows go to the quarantine
  layer.
* ``report_data_quality`` — reads the partition's staging/quarantine
  tables and logs the data-quality alert (the ``data_quality`` DAG task).
"""
from __future__ import annotations

import logging
import os

from data_quality.quality_report import report_quality_partition
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
QUARANTINE_DIR = layer_dir("quarantine", SOURCE)

# Table config (contract) of the staging table written by this pipeline.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"staging_{SOURCE}.json")

# properties.* fields promoted to flat staging columns.
PROPERTY_FIELDS = ["geocerca_id", "nome", "tipo", "uf", "raio_km", "ativo"]


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read geocercas.geojson and write it to the raw layer.

    The source is a GeoJSON ``FeatureCollection`` — a single JSON
    object wrapping a ``features`` array — so the file is read with
    ``multiLine`` enabled and the array is exploded to one row per
    feature (the raw grain is one geofence). Each feature keeps its
    original nested shape (``type``, ``properties``, ``geometry``)
    and every primitive is read as a string (``primitivesAsString``),
    faithful to the source, so no dirty value (e.g. a zeroed
    coordinate or an invalid radius) is masked by an early cast.
    Typing and flattening happen in the staging stage.

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

    Exclusive treatment of this source: promotes each ``properties.*``
    field to a flat column and serializes ``geometry`` to a canonical
    GeoJSON string with numeric coordinates (the raw layer reads every
    primitive as a string). A field absent from the whole partition
    becomes a null column, so the config validations flag it instead
    of the job crashing on a missing path.

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

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be unit-tested with synthetic data. The GeoJSON
    flattening is the only source-exclusive step; every other rule
    lives in the table config: treatments/cast/dedup first, then the
    quarantine validations (invalid values are nulled but the row
    keeps its primary key), and finally the derived columns.

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

    Delegates to the shared staging transform
    (`staging_pipeline.run_staging_transform`) with this source's
    paths and its exclusive `clean_and_validate` chain (GeoJSON
    flattening before the generic engine).

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to locate the raw partition and to partition the
            staging layer.

    Returns:
        Metrics about the write: layer name, destination table,
        ingestion date, input/output record counts and how many were
        quarantined.
    """
    return run_staging_transform(
        spark,
        SOURCE,
        ingest_date,
        raw_dir=RAW_DIR,
        staging_dir=STAGING_DIR,
        quarantine_dir=QUARANTINE_DIR,
        config_path=TABLE_CONFIG,
        clean=clean_and_validate,
    )


def report_data_quality(spark: SparkSession, ingest_date: str) -> dict:
    """Log the data-quality alert for the quarantined partition.

    Backs the ``data_quality`` DAG task, which runs after
    ``transform_to_staging``: reads the partition's staging and
    quarantine tables and logs how many records were rejected, the
    reason counts and the percentage over the total.

    Args:
        spark: Active SparkSession (must be created with
            ``enable_delta=True``).
        ingest_date: Ingestion date in ``YYYY-MM-DD`` format.

    Returns:
        Metrics about the partition: total/rejected record counts,
        rejected percentage and count per reason.
    """
    return report_quality_partition(spark, SOURCE, STAGING_DIR, QUARANTINE_DIR, ingest_date)
