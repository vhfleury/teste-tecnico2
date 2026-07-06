"""Path and config helpers shared by every pipeline."""
from __future__ import annotations

import json
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
LAKEHOUSE_DIR = os.environ.get("LAKEHOUSE_DIR", "/opt/airflow/lakehouse")


def layer_dir(layer: str, dataset: str) -> str:
    """Build a dataset's base directory in a lakehouse layer.

    Args:
        layer: Lakehouse layer name (`raw`, `staging` or `analytics`).
        dataset: Pipeline/dataset name (e.g. `motoristas`).

    Returns:
        The dataset's base directory inside that layer.
    """
    return f"{LAKEHOUSE_DIR}/{layer}/{dataset}"


def load_table_config(path: str) -> dict:
    """Load a table config JSON (name, description, schema).

    Args:
        path: Path to the table config JSON file.

    Returns:
        The parsed config: `table_name`, `description`,
        `partitioned_by` and `schema` (list of columns with
        `name`, `type` and `example`).
    """
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def resolve_ingest_date(context: dict) -> str:
    """Resolve the run's ingestion date from the task context.

    Args:
        context: Airflow task context.

    Returns:
        The ingestion date in `YYYY-MM-DD` format.
    """
    ds = context.get("ds")
    if ds:
        return ds
    return context["dag_run"].run_after.strftime("%Y-%m-%d")
