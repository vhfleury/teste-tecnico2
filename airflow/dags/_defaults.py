"""Shared task defaults and staging schedule for the project's DAGs.

Kept here in the dags folder (not in ``scripts/general``) on purpose: it
depends on ``pendulum``, an Airflow-only dependency, and this way that
dependency never reaches the pure-transform code that ``pipelines/``,
``analytics/`` and the CI test job import.
"""
from __future__ import annotations

import pendulum

TASK_RETRIES = 2
RETRY_DELAY = pendulum.duration(minutes=1)

# Applied to every task in every DAG.
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": TASK_RETRIES,
    "retry_delay": RETRY_DELAY,
}

# Cadence at which every staging source is polled for new data. Analytics DAGs
# schedule by assets, so they do not use this.
STAGING_SCHEDULE = "*/10 * * * *"
