"""posicoes pipeline: the vehicle tracking positions.

One stage for now:

* ``extract_to_raw`` — reads the source Parquet (unchanged) and writes it
  to the raw layer.

The staging stage (cleaning/validation driven by the table config) will be
added next.
"""
from __future__ import annotations

import logging
import os

from general.utils import DATA_DIR, raw_dir, raw_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SOURCE = "posicoes"

# The tracking source ships the positions file under data/rastreamento.
POSICOES_PARQUET = os.path.join(DATA_DIR, "rastreamento", f"{SOURCE}.parquet")
RAW_DIR = raw_dir(SOURCE)


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read posicoes.parquet and write it to the raw layer, unchanged.

    Parquet carries its own schema, so reading it applies no inference
    or cast — the raw layer keeps the exact types and values shipped by
    the source. Standardization and validation happen in the staging
    stage.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
    """
    log.info("Starting extraction - reading Parquet from %s", POSICOES_PARQUET)
    df = spark.read.parquet(POSICOES_PARQUET)
    df = df.withColumn("source_file", F.lit(POSICOES_PARQUET)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("Parquet read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(RAW_DIR, ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}
