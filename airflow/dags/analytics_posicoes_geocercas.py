"""DAG ``analytics_posicoes_geocercas`` - enriches tracking positions with geofence events."""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from analytics.posicoes_geocercas.posicoes_geocercas import (
    ANALYTICS_DIR,
    transform_to_analytics,
)
from connections.spark_session import run_spark
from scripts.general.delta_io import delta_partition_processed
from scripts.general.utils import resolve_ingest_date

log = logging.getLogger(__name__)

TASK_RETRIES = 2
RETRY_DELAY = pendulum.duration(minutes=1)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": TASK_RETRIES,
    "retry_delay": RETRY_DELAY,
}

# Staging inputs this DAG schedules on (published by the staging DAGs).
STAGING_POSICOES = Asset("staging_posicoes")
STAGING_GEOCERCAS = Asset("staging_geocercas")
# Output of this DAG at the analytics layer.
ANALYTICS_POSICOES_GEOCERCAS = Asset("analytics_posicoes_geocercas")


@dag(
    dag_id="analytics_posicoes_geocercas",
    description="Geospatial enrichment of tracking positions with geofence events",
    schedule=[STAGING_POSICOES, STAGING_GEOCERCAS],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "analytics", "geocercas", "posicoes"],
    doc_md=__doc__,
)
def analytics_posicoes_geocercas():
    @task(task_id="enrich_to_analytics", outlets=[ANALYTICS_POSICOES_GEOCERCAS])
    def enrich_to_analytics_task(**context) -> dict:
        """Build the geospatial analytics partition for the run date.

        Args:
            **context: Airflow task context, used to resolve the
                run's ingestion date (`YYYY-MM-DD`).

        Returns:
            Metrics returned by `transform_to_analytics`.

        Raises:
            AirflowSkipException: If the analytics partition for
                `ingest_date` was already processed.
        """
        ingest_date = resolve_ingest_date(context)  # YYYY-MM-DD
        log.info("Enrichment task started - ingest_date=%s", ingest_date)

        log.info("Checking analytics partition marker at %s", ANALYTICS_DIR)
        if delta_partition_processed(ANALYTICS_DIR, ingest_date):
            log.info(
                "Analytics for %s already processed (%s) - skipping enrichment",
                ingest_date,
                ANALYTICS_DIR,
            )
            raise AirflowSkipException(f"analytics ingest_date={ingest_date} already exists")
        log.info("Analytics partition ingest_date=%s not processed yet - enriching", ingest_date)

        metrics = run_spark(
            "analytics_posicoes_geocercas_transform",
            transform_to_analytics,
            ingest_date,
            enable_delta=True,
        )
        log.info(
            "Analytics layer written: %s records, %s entry events, %s exit events",
            metrics["records"],
            metrics["entry_events"],
            metrics["exit_events"],
        )
        return metrics

    enrich_to_analytics_task()


analytics_posicoes_geocercas()
