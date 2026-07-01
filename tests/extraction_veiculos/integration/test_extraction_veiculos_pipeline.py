"""Integration tests for the extraction_veiculos pipeline (raw -> staging on disk)."""
import json
from pathlib import Path

from scripts.extraction_veiculos import statics
from scripts.extraction_veiculos.extraction_veiculos import extract_to_raw, transform_to_staging

FIXTURE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = FIXTURE_DIR / "input_veiculos.csv"
EXPECTED_JSON = FIXTURE_DIR / "expected_staging_veiculos.json"

INGEST_DATE = "2024-01-01"


def test_pipeline_writes_raw_and_staging_layers(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(statics, "VEICULOS_CSV", str(INPUT_CSV))
    monkeypatch.setattr(statics, "RAW_DIR", str(tmp_path / "raw" / "veiculos"))
    monkeypatch.setattr(statics, "STAGING_DIR", str(tmp_path / "staging" / "veiculos"))

    raw_metrics = extract_to_raw(spark, INGEST_DATE)
    assert raw_metrics["records"] == 8

    staging_metrics = transform_to_staging(spark, INGEST_DATE)
    assert staging_metrics["records_out"] == 6
    assert staging_metrics["records_flagged"] == 5

    expected = json.loads(EXPECTED_JSON.read_text(encoding="utf-8"))
    columns = list(expected[0].keys())
    staging_path = f"{statics.STAGING_DIR}/ingest_date={INGEST_DATE}"
    result = spark.read.parquet(staging_path)
    actual = [row.asDict() for row in result.select(*columns).orderBy("veiculo_id").collect()]

    assert actual == expected
