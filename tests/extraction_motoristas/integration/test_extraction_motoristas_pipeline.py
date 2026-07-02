"""Integration tests for the extraction_motoristas pipeline (source JSON -> raw on disk)."""
import json
from pathlib import Path

from pyspark.sql import functions as F

from scripts.extraction_motoristas import statics
from scripts.extraction_motoristas.extraction_motoristas import extract_to_raw
from scripts.extraction_motoristas.extraction_motoristas_parser import (
    partition_processed,
    raw_path,
)

FIXTURE_DIR = Path(__file__).resolve().parent.parent
INPUT_JSON = FIXTURE_DIR / "input_motoristas.json"
EXPECTED_JSON = FIXTURE_DIR / "expected_raw_motoristas.json"

INGEST_DATE = "2024-01-01"


def test_extract_to_raw_writes_raw_layer(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(statics, "MOTORISTAS_JSON", str(INPUT_JSON))
    monkeypatch.setattr(statics, "RAW_DIR", str(tmp_path / "raw" / "motoristas"))

    metrics = extract_to_raw(spark, INGEST_DATE)

    assert metrics["layer"] == "raw"
    assert metrics["records"] == 8
    assert partition_processed(raw_path(INGEST_DATE))

    result = spark.read.parquet(raw_path(INGEST_DATE))

    expected = json.loads(EXPECTED_JSON.read_text(encoding="utf-8"))
    columns = list(expected[0].keys())

    # Raw is faithful to the source: every business column stays a string,
    # dirty values and duplicates included. Typing happens in staging.
    business_types = {name: dtype for name, dtype in result.dtypes if name in columns}
    assert set(business_types) == set(columns)
    assert set(business_types.values()) == {"string"}

    actual = [row.asDict() for row in result.select(*columns).orderBy("motorista_id").collect()]
    assert actual == expected

    audit = result.select("source_file", "ingested_at").distinct().collect()
    assert all(row["source_file"] == str(INPUT_JSON) for row in audit)
    assert result.filter(F.col("ingested_at").isNull()).count() == 0
