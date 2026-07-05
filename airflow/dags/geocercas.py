"""DAG ``geocercas`` - ingestion and quality of the geofence registry.

Flow across the lakehouse layers, partitioned by ingestion date::

    data/geocercas/geocercas.geojson
      |-(extract_to_raw)->  lakehouse/raw/geocercas/ingest_date=YYYY-MM-DD
          |-(transform_to_staging)->  lakehouse/staging/geocercas/ingest_date=YYYY-MM-DD

The DAG is thin on purpose: all PySpark logic lives in
``pipelines/geocercas/geocercas.py`` and the staging contract
(columns, treatments, validations, key) in ``staging_geocercas.json``.

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

The final task publishes the ``staging_geocercas`` Asset, the data-aware
trigger for the future gold DAG (geospatial enrichment + trip fact table).
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from connections.spark_session import run_spark
from pipelines.geocercas.geocercas import (
    RAW_DIR,
    STAGING_DIR,
    extract_to_raw,
    transform_to_staging,
)
from scripts.general.utils import (
    partition_path,
    partition_processed,
    resolve_ingest_date,
)

log = logging.getLogger(__name__)

TASK_RETRIES = 2
RETRY_DELAY = pendulum.duration(minutes=1)
# Cadence at which the geofence source is polled for new data.
STAGING_SCHEDULE = "*/10 * * * *"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": TASK_RETRIES,
    "retry_delay": RETRY_DELAY,
}

# Output of this source at the staging layer (the future gold DAG schedules on this asset).
STAGING_GEOCERCAS = Asset("staging_geocercas")


@dag(
    dag_id="geocercas",
    description="Geofence registry ingestion and standardization (GeoJSON -> raw -> staging)",
    schedule=STAGING_SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "geocercas", "staging"],
    doc_md=__doc__,
)
def geocercas():
    @task(task_id="extract_to_raw")
    def extract_to_raw_task(**context) -> dict:
        """Read the source GeoJSON and write the raw partition.

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
        destination = partition_path(RAW_DIR, ingest_date)

        if partition_processed(destination):
            log.info(
                "Raw for %s already processed (%s) - skipping extraction",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"raw ingest_date={ingest_date} already exists")

        metrics = run_spark("geocercas_extract", extract_to_raw, ingest_date)
        log.info("Raw layer written: %s", metrics)
        return metrics

    @task(
        task_id="transform_to_staging",
        outlets=[STAGING_GEOCERCAS],
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
        destination = partition_path(STAGING_DIR, ingest_date)

        if partition_processed(destination):
            log.info(
                "Staging for %s already processed (%s) - skipping transform",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"staging ingest_date={ingest_date} already exists")

        metrics = run_spark("geocercas_transform", transform_to_staging, ingest_date)
        log.info(
            "Staging layer written: %s records (%s flagged for quality)",
            metrics["records_out"],
            metrics["records_flagged"],
        )
        return metrics

    extract_to_raw_task() >> transform_to_staging_task()


geocercas()
