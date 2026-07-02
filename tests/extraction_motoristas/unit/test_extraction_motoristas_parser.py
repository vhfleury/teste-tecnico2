"""Unit tests for the extraction_motoristas support functions."""
from scripts.extraction_motoristas import statics
from scripts.extraction_motoristas.extraction_motoristas_parser import (
    partition_processed,
    raw_path,
)


def test_raw_path_builds_partition_for_ingest_date(monkeypatch):
    monkeypatch.setattr(statics, "RAW_DIR", "/lakehouse/raw/motoristas")

    assert raw_path("2024-01-01") == "/lakehouse/raw/motoristas/ingest_date=2024-01-01"


def test_partition_processed_requires_success_marker(tmp_path):
    partition = tmp_path / "ingest_date=2024-01-01"
    partition.mkdir()

    assert not partition_processed(str(partition))

    (partition / "_SUCCESS").touch()

    assert partition_processed(str(partition))
