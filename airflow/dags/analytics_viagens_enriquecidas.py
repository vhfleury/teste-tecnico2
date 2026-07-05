"""DAG ``analytics_viagens_enriquecidas`` - consolidated trips enrichment.

Flow across the lakehouse layers, partitioned by ingestion date::

    lakehouse/staging/viagens/ingest_date=YYYY-MM-DD
    lakehouse/staging/veiculos/ingest_date=YYYY-MM-DD
    lakehouse/staging/motoristas/ingest_date=YYYY-MM-DD
    lakehouse/staging/geocercas/ingest_date=YYYY-MM-DD
    lakehouse/staging/posicoes/ingest_date=YYYY-MM-DD
      |-(enrich_to_analytics)-> lakehouse/analytics/viagens_enriquecidas (Delta)

The DAG is thin on purpose: all PySpark enrichment logic lives in
``analytics/viagens_enriquecidas.py`` and the analytics table contract
in ``analytics_viagens_enriquecidas.json``.

Data-aware scheduling: instead of a cron, the DAG runs when the five
staging Assets are published by the staging DAGs, so analytics never
reads a partition that has not been written yet.

The gold table is Delta: each run atomically replaces only its own
``ingest_date`` partition (``replaceWhere``), so reprocessing a date
can never duplicate or corrupt other partitions.

**Idempotency via pre-check:** Delta writes no ``_SUCCESS`` marker, so
before doing any work the task checks the gold partition marker
written after the Delta commit (``_markers/<ingest_date>``). If it
exists, the task is skipped (``AirflowSkipException``) instead of
reprocessing.

The task publishes the ``analytics_viagens_enriquecidas`` Asset, the
data-aware trigger for downstream consumers of the enriched trips
(e.g. the aggregated metrics DAG).
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task

from analytics.viagens_enriquecidas import ANALYTICS_DIR, transform_to_analytics
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
STAGING_VIAGENS = Asset("staging_viagens")
STAGING_VEICULOS = Asset("staging_veiculos")
STAGING_MOTORISTAS = Asset("staging_motoristas")
STAGING_GEOCERCAS = Asset("staging_geocercas")
STAGING_POSICOES = Asset("staging_posicoes")
# Output of this DAG at the analytics layer.
ANALYTICS_VIAGENS_ENRIQUECIDAS = Asset("analytics_viagens_enriquecidas")


@dag(
    dag_id="analytics_viagens_enriquecidas",
    description="Consolidated trips enriched with vehicle, driver, geofences and trip metrics",
    schedule=[
        STAGING_VIAGENS,
        STAGING_VEICULOS,
        STAGING_MOTORISTAS,
        STAGING_GEOCERCAS,
        STAGING_POSICOES,
    ],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigcore", "analytics", "viagens"],
    doc_md=__doc__,
)
def analytics_viagens_enriquecidas():
    @task(task_id="enrich_to_analytics", outlets=[ANALYTICS_VIAGENS_ENRIQUECIDAS])
    def enrich_to_analytics_task(**context) -> dict:
        """Build the enriched trips partition for the run date.

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

        if delta_partition_processed(ANALYTICS_DIR, ingest_date):
            log.info(
                "Analytics for %s already processed (%s) - skipping enrichment",
                ingest_date,
                ANALYTICS_DIR,
            )
            raise AirflowSkipException(f"analytics ingest_date={ingest_date} already exists")

        metrics = run_spark(
            "analytics_viagens_enriquecidas_transform",
            transform_to_analytics,
            ingest_date,
            enable_delta=True,
        )
        log.info(
            "Analytics layer written: %s records, %s flagged, %s delayed",
            metrics["records"],
            metrics["records_flagged"],
            metrics["records_delayed"],
        )
        return metrics

    enrich_to_analytics_task()


analytics_viagens_enriquecidas()
