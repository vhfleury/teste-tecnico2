"""Unit tests for the shared path and config helpers."""
import json
from datetime import datetime
from types import SimpleNamespace

import pytest
from general import utils
from general.utils import (
    layer_dir,
    load_table_config,
    resolve_ingest_date,
)


@pytest.mark.parametrize("layer", ["raw", "staging", "analytics"])
def test_layer_dir_builds_dataset_directory_in_layer(monkeypatch, layer):
    monkeypatch.setattr(utils, "LAKEHOUSE_DIR", "/lakehouse")

    assert layer_dir(layer, "motoristas") == f"/lakehouse/{layer}/motoristas"


def test_resolve_ingest_date_uses_ds_when_present():
    context = {"ds": "2024-01-01"}

    assert resolve_ingest_date(context) == "2024-01-01"


def test_resolve_ingest_date_falls_back_to_run_after():
    context = {
        "ds": None,
        "dag_run": SimpleNamespace(run_after=datetime(2024, 2, 3, 15, 30)),
    }

    assert resolve_ingest_date(context) == "2024-02-03"


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
