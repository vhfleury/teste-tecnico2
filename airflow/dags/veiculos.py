"""DAG ``veiculos`` - ingestion of the vehicle registry.

Flow across the lakehouse layers, partitioned by ingestion date::

    data/veiculos/veiculos.csv
      |-(extract_to_raw)->  lakehouse/raw/veiculos/ingest_date=YYYY-MM-DD

The DAG is thin on purpose: all PySpark logic lives in
``pipelines/veiculos/veiculos.py``. The staging stage (config-driven
clean/validate, like motoristas) is the next refactoring step.

Each run writes to its own ``ingest_date`` partition (the run's
``logical_date``). Different dates coexist; reprocessing the same date only
overwrites that partition.

**Idempotency via pre-check:** before doing any work, the task checks
whether its target partition was already written successfully (a
``_SUCCESS`` marker). If it exists, the task is skipped
(``AirflowSkipException``) instead of reprocessing.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from connections.spark_session import run_spark
from pipelines.veiculos.veiculos import RAW_DIR, extract_to_raw
from scripts.general.utils import partition_processed, raw_path, resolve_ingest_date

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="veiculos",
    description="Vehicle registry ingestion (CSV -> raw)",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "veiculos", "raw"],
    doc_md=__doc__,
)
def veiculos():
    @task(task_id="extract_to_raw")
    def extract_to_raw_task(**context) -> dict:
        """Read the source CSV and write the raw partition.

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
        destination = raw_path(RAW_DIR, ingest_date)

        if partition_processed(destination):
            log.info(
                "Raw for %s already processed (%s) - skipping extraction",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"raw ingest_date={ingest_date} already exists")

        metrics = run_spark("veiculos_extract", extract_to_raw, ingest_date)
        log.info("Raw layer written: %s", metrics)
        return metrics

    extract_to_raw_task()


veiculos()
