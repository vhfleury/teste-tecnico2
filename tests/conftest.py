import os
import sys

import pytest

# Replicate the container import layout
# (PYTHONPATH=/opt/airflow/scripts:/opt/airflow/pipelines:/opt/airflow):
# scripts/ and pipelines/ must come before the repo root.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for path in (ROOT, os.path.join(ROOT, "pipelines"), os.path.join(ROOT, "scripts")):
    if path not in sys.path:
        sys.path.insert(0, path)

from connections.spark_session import get_spark  # noqa: E402  (needs the sys.path setup above)


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests", master="local[1]")
    yield session
    session.stop()
