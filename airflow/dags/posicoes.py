"""DAG ``posicoes`` - ingestion and quality of the vehicle tracking positions.

Flow across the lakehouse layers, partitioned by ingestion date::

    data/rastreamento/posicoes.parquet
      |-(extract_to_raw)->  lakehouse/raw/posicoes/ingest_date=YYYY-MM-DD
          |-(transform_to_staging)->  lakehouse/staging/posicoes/ingest_date=YYYY-MM-DD

The DAG is thin on purpose: all PySpark logic lives in
``pipelines/posicoes/posicoes.py`` and the staging contract
(columns, treatments, validations, key) in ``staging_posicoes.json``.

Each run writes to its own ``ingest_date`` partition (the run's
``logical_date``). Different dates coexist; reprocessing the same date only
overwrites that partition.

**Idempotency via pre-check:** before doing any work, each task checks
whether its target partition was already written successfully (a
``_SUCCESS`` marker). If it exists, the task is skipped
(``AirflowSkipException``) instead of reprocessing. The transform task uses
``trigger_rule="none_failed"`` so it can still run its own check even when
the extraction task was skipped because raw already existed.

Tasks communicate through the persisted layer (the raw Parquet), not XCom:
each one reads/writes the lakehouse and can be re-run independently.

The final task publishes the ``staging_posicoes`` Asset, the data-aware
trigger for the future gold DAG (tracking aggregated to the trip grain).
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from connections.spark_session import run_spark
from pipelines.posicoes.posicoes import (
    RAW_DIR,
    STAGING_DIR,
    extract_to_raw,
    transform_to_staging,
)
from scripts.general.utils import (
    partition_processed,
    raw_path,
    resolve_ingest_date,
    staging_path,
)

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

# Output of this source at the staging layer (the future gold DAG schedules on this asset).
STAGING_POSICOES = Asset("staging_posicoes")


@dag(
    dag_id="posicoes",
    description="Vehicle tracking positions ingestion and standardization (Parquet -> raw -> staging)",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "posicoes", "staging"],
    doc_md=__doc__,
)
def posicoes():
    @task(task_id="extract_to_raw")
    def extract_to_raw_task(**context) -> dict:
        """Read the source Parquet and write the raw partition.

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

        metrics = run_spark("posicoes_extract", extract_to_raw, ingest_date)
        log.info("Raw layer written: %s", metrics)
        return metrics

    @task(
        task_id="transform_to_staging",
        outlets=[STAGING_POSICOES],
        trigger_rule="none_failed",
    )
    def transform_to_staging_task(**context) -> dict:
        """Read the raw partition, clean/validate it and write staging.

        Args:
            **context: Airflow task context, used to resolve the
                run's ingestion date (`YYYY-MM-DD`).

        Returns:
            Metrics returned by `transform_to_staging`.

        Raises:
            AirflowSkipException: If the staging partition for
                `ingest_date` was already processed.
        """
        ingest_date = resolve_ingest_date(context)  # YYYY-MM-DD
        destination = staging_path(STAGING_DIR, ingest_date)

        if partition_processed(destination):
            log.info(
                "Staging for %s already processed (%s) - skipping transform",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"staging ingest_date={ingest_date} already exists")

        metrics = run_spark("posicoes_transform", transform_to_staging, ingest_date)
        log.info(
            "Staging layer written: %s records (%s flagged for quality)",
            metrics["records_out"],
            metrics["records_flagged"],
        )
        return metrics

    extract_to_raw_task() >> transform_to_staging_task()


posicoes()
