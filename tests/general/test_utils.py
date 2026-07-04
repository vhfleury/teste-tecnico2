"""Unit tests for the shared path and config helpers."""
import json

import pytest

from general import utils
from general.utils import (
    load_table_config,
    partition_processed,
    raw_dir,
    raw_path,
    staging_dir,
    staging_path,
)


def test_raw_dir_builds_layer_directory_for_pipeline(monkeypatch):
    monkeypatch.setattr(utils, "LAKEHOUSE_DIR", "/lakehouse")

    assert raw_dir("motoristas") == "/lakehouse/raw/motoristas"


def test_staging_dir_builds_layer_directory_for_pipeline(monkeypatch):
    monkeypatch.setattr(utils, "LAKEHOUSE_DIR", "/lakehouse")

    assert staging_dir("motoristas") == "/lakehouse/staging/motoristas"


def test_raw_path_builds_partition_for_ingest_date():
    assert (
        raw_path("/lakehouse/raw/motoristas", "2024-01-01")
        == "/lakehouse/raw/motoristas/ingest_date=2024-01-01"
    )


def test_staging_path_builds_partition_for_ingest_date():
    assert (
        staging_path("/lakehouse/staging/motoristas", "2024-01-01")
        == "/lakehouse/staging/motoristas/ingest_date=2024-01-01"
    )


def test_partition_processed_requires_success_marker(tmp_path):
    partition = tmp_path / "ingest_date=2024-01-01"
    partition.mkdir()

    assert not partition_processed(str(partition))

    (partition / "_SUCCESS").touch()

    assert partition_processed(str(partition))


def test_partition_processed_missing_directory(tmp_path):
    assert not partition_processed(str(tmp_path / "does_not_exist"))


def test_load_table_config_parses_json(tmp_path):
    config = {
        "table_name": "staging_example",
        "description": "Example table",
        "partitioned_by": ["ingest_date"],
        "schema": [{"name": "id", "type": "string", "example": "ID-1"}],
    }
    path = tmp_path / "staging_example.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    assert load_table_config(str(path)) == config


def test_load_table_config_fails_on_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_table_config(str(tmp_path / "does_not_exist.json"))
