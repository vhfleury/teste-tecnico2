"""DAG ``geocercas`` - ingests the geofence registry from GeoJSON to raw to staging, with a data-quality alert."""
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
from scripts.data_quality.quality_report import log_rejection_alert
from scripts.general.delta_io import delta_partition_processed
from scripts.general.utils import resolve_ingest_date

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
        log.info("Extract task started for geocercas - ingest_date=%s", ingest_date)

        log.info("Checking raw partition marker at %s", RAW_DIR)
        if delta_partition_processed(RAW_DIR, ingest_date):
            log.info(
                "Raw for %s already processed (%s) - skipping extraction",
                ingest_date,
                RAW_DIR,
            )
            raise AirflowSkipException(f"raw ingest_date={ingest_date} already exists")
        log.info("Raw partition ingest_date=%s not processed yet - extracting", ingest_date)

        metrics = run_spark("geocercas_extract", extract_to_raw, ingest_date, enable_delta=True)
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
        log.info("Transform task started for geocercas - ingest_date=%s", ingest_date)

        log.info("Checking staging partition marker at %s", STAGING_DIR)
        if delta_partition_processed(STAGING_DIR, ingest_date):
            log.info(
                "Staging for %s already processed (%s) - skipping transform",
                ingest_date,
                STAGING_DIR,
            )
            raise AirflowSkipException(f"staging ingest_date={ingest_date} already exists")
        log.info("Staging partition ingest_date=%s not processed yet - transforming", ingest_date)

        metrics = run_spark(
            "geocercas_transform", transform_to_staging, ingest_date, enable_delta=True
        )
        log.info(
            "Staging layer written: %s records (%s rejected and discarded)",
            metrics["records_out"],
            metrics["records_rejected"],
        )
        return metrics

    @task(task_id="data_quality")
    def data_quality_task(metrics: dict) -> None:
        """Log the data-quality alert for the partition's rejected rows.

        Args:
            metrics: Transform metrics received via XCom, with the
                rejected count, percentage and count per reason.
        """
        log.info(
            "Data quality task started for geocercas - ingest_date=%s",
            metrics["ingest_date"],
        )
        log_rejection_alert(metrics)

    staging_metrics = transform_to_staging_task()
    extract_to_raw_task() >> staging_metrics
    data_quality_task(staging_metrics)


geocercas()
