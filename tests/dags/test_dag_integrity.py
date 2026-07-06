"""Smoke test: the DAG files parse with no import errors and the expected DAGs register.

The pure-transform suites never import the DAG modules (``requirements.txt`` has
no Airflow), so a broken import or a bug in the dynamic staging-DAG loop in
``airflow/dags/staging_sources.py`` would reach CI green. This loads
``airflow/dags/`` through a ``DagBag`` exactly as the scheduler would and asserts
the eight expected DAGs are registered with zero import errors.

Importing the modules is enough to exercise the whole chain (staging pipeline,
Spark session factory, Sedona wrappers) because those imports resolve at module
load time; no JVM starts, so the test needs no Java. It is skipped where Airflow
is absent (the pyspark test job) and runs in the dedicated ``dag-parse`` CI job
and inside the container.
"""
from __future__ import annotations

import os

import pytest

pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402  (guarded by importorskip above)
from staging_pipeline import active_sources  # noqa: E402  (sys.path wired in conftest)

DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "airflow",
    "dags",
)

# The three analytics DAGs plus the dedicated geocercas DAG. The staging DAGs are
# generated one per active source, so their ids come from the same registry the
# generator reads (active_sources()) rather than being repeated here.
STATIC_DAG_IDS = {
    "geocercas",
    "analytics_posicoes_geocercas",
    "analytics_viagens_enriquecidas",
    "analytics_metricas_viagens",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dags_have_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"DAG import errors:\n{dagbag.import_errors}"


def test_expected_dags_are_registered(dagbag: DagBag) -> None:
    # Exact equality also guards the count (8): a missing, renamed or extra DAG
    # fails here, the same way the golden coverage guard fails an unregistered
    # source.
    expected = set(active_sources()) | STATIC_DAG_IDS
    assert set(dagbag.dag_ids) == expected
