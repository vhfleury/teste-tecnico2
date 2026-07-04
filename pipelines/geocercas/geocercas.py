"""geocercas pipeline: the geofence registry.

First stage only (staging comes in a later phase):

* ``extract_to_raw`` — reads the source GeoJSON (unchanged values) and
  writes it to the raw layer, one row per feature.
"""
from __future__ import annotations

import logging
import os

from general.utils import DATA_DIR, raw_dir, raw_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SOURCE = "geocercas"

GEOCERCAS_GEOJSON = os.path.join(DATA_DIR, SOURCE, f"{SOURCE}.geojson")
RAW_DIR = raw_dir(SOURCE)


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
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
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
    destination = raw_path(RAW_DIR, ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}
