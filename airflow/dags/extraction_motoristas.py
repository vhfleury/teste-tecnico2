"""DAG ``extraction_motoristas`` - ingestion of the driver registry.

Flow across the lakehouse layers, partitioned by ingestion date::

    data/motoristas/motoristas.json
      |-(extract_to_raw)->  lakehouse/raw/motoristas/ingest_date=YYYY-MM-DD

Each run writes to its own ``ingest_date`` partition (the run's
``logical_date``). Different dates coexist; reprocessing the same date only
overwrites that partition.

**Idempotency via pre-check:** before doing any work, the task checks
whether its target partition was already written successfully (a
``_SUCCESS`` marker). If it exists, the task is skipped
(``AirflowSkipException``) instead of reprocessing.

The staging stage (``transform_to_staging``) will be added next, following
the same pattern as ``extraction_veiculos``; the ``staging_motoristas``
Asset will be published by that task.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from connections.spark_session import run_spark
from scripts.extraction_motoristas.extraction_motoristas import extract_to_raw
from scripts.extraction_motoristas.extraction_motoristas_parser import (
    partition_processed,
    raw_path,
)

log = logging.getLogger(__name__)


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


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="extraction_motoristas",
    description="Driver registry ingestion (source JSON -> raw)",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "motoristas", "extraction"],
    doc_md=__doc__,
)
def extraction_motoristas():
    @task(task_id="extract_to_raw")
    def extract_to_raw_task(**context) -> dict:
        """Read the source JSON and write the raw partition.

        Args:
            **context: Airflow task context, used to resolve the
                run's ingestion date (`YYYY-MM-DD`).

        Returns:
            Metrics returned by `extract_to_raw`.

        Raises:
            AirflowSkipException: If the raw partition for
                `ingest_date` was already processed.
        """
        ingest_date = resolve_ingest_date(context)  # YYYY-MM-DD
        destination = raw_path(ingest_date)

        if partition_processed(destination):
            log.info(
                "Raw for %s already processed (%s) - skipping extraction",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"raw ingest_date={ingest_date} already exists")

        metrics = run_spark("extraction_motoristas_extract", extract_to_raw, ingest_date)
        log.info("Raw layer written: %s", metrics)
        return metrics

    extract_to_raw_task()


extraction_motoristas()
