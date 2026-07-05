"""Path and config helpers shared by every pipeline."""
from __future__ import annotations

import json
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
LAKEHOUSE_DIR = os.environ.get("LAKEHOUSE_DIR", "/opt/airflow/lakehouse")


def raw_dir(pipeline: str) -> str:
    """Build the raw layer base directory for a pipeline.

    Args:
        pipeline: Pipeline/dataset name (e.g. `motoristas`).

    Returns:
        The raw layer base directory inside the lakehouse.
    """
    return f"{LAKEHOUSE_DIR}/raw/{pipeline}"


def staging_dir(pipeline: str) -> str:
    """Build the staging layer base directory for a pipeline.

    Args:
        pipeline: Pipeline/dataset name (e.g. `motoristas`).

    Returns:
        The staging layer base directory inside the lakehouse.
    """
    return f"{LAKEHOUSE_DIR}/staging/{pipeline}"


def analytics_dir(dataset: str) -> str:
    """Build the analytics layer base directory for a dataset.

    Args:
        dataset: Analytics dataset name (e.g. `posicoes_geocercas`).

    Returns:
        The analytics layer base directory inside the lakehouse.
    """
    return f"{LAKEHOUSE_DIR}/analytics/{dataset}"


def raw_path(base_dir: str, ingest_date: str) -> str:
    """Build the partitioned raw path for a given ingestion date.

    Args:
        base_dir: Base directory of the dataset's raw layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The raw layer path for that partition.
    """
    return f"{base_dir}/ingest_date={ingest_date}"


def analytics_path(base_dir: str, ingest_date: str) -> str:
    """Build the partitioned analytics path for a given ingestion date.

    Args:
        base_dir: Base directory of the dataset's analytics layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The analytics layer path for that partition.
    """
    return f"{base_dir}/ingest_date={ingest_date}"


def staging_path(base_dir: str, ingest_date: str) -> str:
    """Build the partitioned staging path for a given ingestion date.

    Args:
        base_dir: Base directory of the dataset's staging layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The staging layer path for that partition.
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
