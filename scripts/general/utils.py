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


def partition_path(base_dir: str, ingest_date: str) -> str:
    """Build the partitioned path for a given ingestion date.

    Args:
        base_dir: Base directory of the dataset in its layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The `ingest_date` partition path under `base_dir`.
    """
    return f"{base_dir}/ingest_date={ingest_date}"


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

    Manual runs triggered without a logical date (e.g. ``airflow dags
    trigger`` on the CLI) have ``logical_date=None`` and therefore no
    ``ds`` in the context, so fall back to the run's ``run_after``.

    Args:
        context: Airflow task context.

    Returns:
        The ingestion date in `YYYY-MM-DD` format.
    """
    ds = context.get("ds")
    if ds:
        return ds
    return context["dag_run"].run_after.strftime("%Y-%m-%d")


def partition_processed(path: str) -> bool:
    """Check whether a partition was already written successfully.

    Args:
        path: Path to the partition directory.

    Returns:
        True if a `_SUCCESS` marker exists under `path`.
    """
    return os.path.exists(os.path.join(path, "_SUCCESS"))
