"""DAG ``analytics_posicoes_geocercas`` - geospatial position enrichment.

Flow across the lakehouse layers, partitioned by ingestion date::

    lakehouse/staging/posicoes/ingest_date=YYYY-MM-DD
    lakehouse/staging/geocercas/ingest_date=YYYY-MM-DD
      |-(enrich_to_analytics)-> lakehouse/analytics/posicoes_geocercas/ingest_date=YYYY-MM-DD

The DAG is thin on purpose: all PySpark enrichment logic lives in
``analytics/posicoes_geocercas.py`` and the analytics table contract in
``analytics_posicoes_geocercas.json``.

Data-aware scheduling: instead of a cron, the DAG runs when the
``staging_posicoes`` and ``staging_geocercas`` Assets are published by
the staging DAGs, so analytics never reads a partition that has not
been written yet.

Each run writes to its own ``ingest_date`` partition (the run's
``logical_date``). Different dates coexist; reprocessing the same date only
overwrites that partition.

**Idempotency via pre-check:** before doing any work, the task checks
whether its target partition was already written successfully (a
``_SUCCESS`` marker). If it exists, the task is skipped
(``AirflowSkipException``) instead of reprocessing.

The task publishes the ``analytics_posicoes_geocercas`` Asset, the
data-aware trigger for downstream consumers of the enriched positions.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from analytics.posicoes_geocercas import ANALYTICS_DIR, transform_to_analytics
from connections.spark_session import run_spark
from scripts.general.utils import partition_path, partition_processed, resolve_ingest_date

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
        destination = partition_path(ANALYTICS_DIR, ingest_date)

        if partition_processed(destination):
            log.info(
                "Analytics for %s already processed (%s) - skipping enrichment",
                ingest_date,
                destination,
            )
            raise AirflowSkipException(f"analytics ingest_date={ingest_date} already exists")

        metrics = run_spark(
            "analytics_posicoes_geocercas_transform",
            transform_to_analytics,
            ingest_date,
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
