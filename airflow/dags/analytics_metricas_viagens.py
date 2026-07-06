"""DAG ``analytics_metricas_viagens`` - builds the six aggregated trip-metric Delta tables, scheduled on the enriched-trips and positions-geofences Assets."""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from analytics.metricas_viagens.metricas_viagens import (
    METRIC_TABLES,
    transform_to_analytics,
)
from connections.spark_session import run_spark
from scripts.general.delta_io import delta_partition_processed
from scripts.general.utils import layer_dir, resolve_ingest_date

log = logging.getLogger(__name__)

TASK_RETRIES = 2
RETRY_DELAY = pendulum.duration(minutes=1)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": TASK_RETRIES,
    "retry_delay": RETRY_DELAY,
}

# Gold inputs this DAG schedules on (published by the analytics DAGs).
ANALYTICS_VIAGENS_ENRIQUECIDAS = Asset("analytics_viagens_enriquecidas")
ANALYTICS_POSICOES_GEOCERCAS = Asset("analytics_posicoes_geocercas")
# Output of this DAG at the analytics layer.
ANALYTICS_METRICAS_VIAGENS = Asset("analytics_metricas_viagens")


@dag(
    dag_id="analytics_metricas_viagens",
    description="Aggregated trip metrics: monthly, route, driver, fleet and geofence dwell",
    schedule=[ANALYTICS_VIAGENS_ENRIQUECIDAS, ANALYTICS_POSICOES_GEOCERCAS],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "analytics", "viagens", "metricas"],
    doc_md=__doc__,
)
def analytics_metricas_viagens():
    @task(task_id="aggregate_to_analytics", outlets=[ANALYTICS_METRICAS_VIAGENS])
    def aggregate_to_analytics_task(**context) -> dict:
        """Build every metric table partition for the run date.

        Args:
            **context: Airflow task context, used to resolve the
                run's ingestion date (`YYYY-MM-DD`).

        Returns:
            Metrics returned by `transform_to_analytics`.

        Raises:
            AirflowSkipException: If every metric table partition for
                `ingest_date` was already processed.
        """
        ingest_date = resolve_ingest_date(context)  # YYYY-MM-DD
        log.info("Aggregation task started - ingest_date=%s", ingest_date)

        log.info("Checking processed markers of %d metric tables", len(METRIC_TABLES))
        pending = [
            name
            for name in METRIC_TABLES
            if not delta_partition_processed(layer_dir("analytics", name), ingest_date)
        ]
        if not pending:
            log.info(
                "All %d metric tables for %s already processed - skipping aggregation",
                len(METRIC_TABLES),
                ingest_date,
            )
            raise AirflowSkipException(f"metrics ingest_date={ingest_date} already exist")

        log.info("Metric tables pending for %s: %s", ingest_date, pending)
        metrics = run_spark(
            "analytics_metricas_viagens_transform",
            transform_to_analytics,
            ingest_date,
            enable_delta=True,
        )
        log.info("Metric tables written: %s", metrics["records"])
        return metrics

    aggregate_to_analytics_task()


analytics_metricas_viagens()
