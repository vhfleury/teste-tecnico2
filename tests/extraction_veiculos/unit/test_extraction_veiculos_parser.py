"""Unit tests for the extraction_veiculos pure transformation function."""
import json
from pathlib import Path

from scripts.extraction_veiculos.extraction_veiculos_parser import clean_and_validate

FIXTURE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = FIXTURE_DIR / "input_veiculos.csv"
EXPECTED_JSON = FIXTURE_DIR / "expected_staging_veiculos.json"


def test_clean_and_validate_flags_quality_issues(spark):
    raw = spark.read.option("header", True).option("encoding", "UTF-8").csv(str(INPUT_CSV))

    result = clean_and_validate(raw)

    expected = json.loads(EXPECTED_JSON.read_text(encoding="utf-8"))
    columns = list(expected[0].keys())
    actual = [row.asDict() for row in result.select(*columns).orderBy("veiculo_id").collect()]

    assert actual == expected
