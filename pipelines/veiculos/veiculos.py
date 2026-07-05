"""Compatibility wrapper for the generic vehicle staging pipeline."""
from __future__ import annotations

from pyspark.sql import SparkSession
from staging_pipeline import (
    clean_and_validate,
    extract_to_raw as _extract_to_raw,
    raw_dir_for,
    staging_dir_for,
    table_config_path,
    transform_to_staging as _transform_to_staging,
)

SOURCE = "veiculos"

RAW_DIR = raw_dir_for(SOURCE)
STAGING_DIR = staging_dir_for(SOURCE)
TABLE_CONFIG = table_config_path(SOURCE)


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read the vehicle source and write the raw partition."""
    return _extract_to_raw(spark, SOURCE, ingest_date)


def transform_to_staging(spark: SparkSession, ingest_date: str) -> dict:
    """Read raw vehicles, apply the table config and write staging."""
    return _transform_to_staging(spark, SOURCE, ingest_date)
