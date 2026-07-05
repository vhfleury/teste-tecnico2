"""DAG ``analytics_metricas_viagens`` - aggregated trip metric tables.

Flow across the lakehouse layers, partitioned by ingestion date::

    lakehouse/analytics/viagens_enriquecidas (Delta)
    lakehouse/analytics/posicoes_geocercas/ingest_date=YYYY-MM-DD
    lakehouse/staging/veiculos/ingest_date=YYYY-MM-DD
      |-(aggregate_to_analytics)-> lakehouse/analytics/<metric table> (Delta, x6)

The DAG is thin on purpose: all PySpark aggregation logic lives in
``analytics/metricas_viagens.py`` — one Delta table per metric,
registered in ``METRIC_TABLES``, each with its own config contract.

Data-aware scheduling: the DAG runs when the
``analytics_viagens_enriquecidas`` and ``analytics_posicoes_geocercas``
Assets are published. The staging vehicles input is a transitive
dependency: the enriched trips Asset only publishes after
``staging_veiculos`` wrote the same partition, so it needs no explicit
edge here.

Each metric table is Delta: every run atomically replaces only its own
``ingest_date`` partition (``replaceWhere``), so reprocessing a date
can never duplicate or corrupt other partitions.

**Idempotency via pre-check:** Delta writes no ``_SUCCESS`` marker, so
before doing any work the task checks the gold partition marker of
every metric table (``_markers/<ingest_date>``). Only when all six are
present is the run skipped (``AirflowSkipException``); a partially
written date is rebuilt whole, which is safe by the atomic
partition replacement.

The task publishes the ``analytics_metricas_viagens`` Asset, the
data-aware trigger for downstream consumers of the metric tables.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from analytics.metricas_viagens import METRIC_TABLES, transform_to_analytics
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
