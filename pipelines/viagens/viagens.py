"""viagens pipeline: the trips fact source.

Single stage for now:

* ``extract_to_raw`` — reads the source CSV (unchanged) and writes it
  to the raw layer.

The staging stage (cleaning/validation driven by the table config)
will be added next.
"""
from __future__ import annotations

import logging
import os

from general.utils import DATA_DIR, raw_dir, raw_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SOURCE = "viagens"

VIAGENS_CSV = os.path.join(DATA_DIR, SOURCE, f"{SOURCE}.csv")
RAW_DIR = raw_dir(SOURCE)


def extract_to_raw(spark: SparkSession, ingest_date: str) -> dict:
    """Read viagens.csv and write it to the raw layer, unchanged.

    Every column is read as a string (no schema inference), faithful
    to the source, so no dirty value (e.g. a missing start date or a
    negative distance) is masked by an early cast. Typing happens in
    the staging stage.

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to partition the raw layer.

    Returns:
        Metrics about the write: layer name, destination path and
        record count.
    """
    log.info("Starting extraction - reading CSV from %s", VIAGENS_CSV)
    df = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .csv(VIAGENS_CSV)
    )
    df = df.withColumn("source_file", F.lit(VIAGENS_CSV)).withColumn(
        "ingested_at", F.current_timestamp()
    )

    total = df.count()
    log.info("CSV read: %d records, %d columns", total, len(df.columns))
    destination = raw_path(RAW_DIR, ingest_date)
    log.info("Writing raw layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)
    log.info("Extraction finished - %d records written to raw", total)

    return {"layer": "raw", "path": destination, "records": total}
