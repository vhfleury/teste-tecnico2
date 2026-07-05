import os
import sys

import pytest

# Replicate the container import layout
# (PYTHONPATH=/opt/airflow/scripts:/opt/airflow/pipelines:/opt/airflow):
# scripts/ and pipelines/ must come before the repo root.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_IMPORT_PATHS = (
    os.path.join(ROOT, "scripts"),
    os.path.join(ROOT, "pipelines"),
    ROOT,
)

pythonpath_parts = list(PROJECT_IMPORT_PATHS)
existing_pythonpath = os.environ.get("PYTHONPATH")
if existing_pythonpath:
    pythonpath_parts.extend(
        path
        for path in existing_pythonpath.split(os.pathsep)
        if path and path not in pythonpath_parts
    )
os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

for path in reversed(PROJECT_IMPORT_PATHS):
    if path not in sys.path:
        sys.path.insert(0, path)

from connections.spark_session import get_spark  # noqa: E402  (needs the sys.path setup above)


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests", master="local[1]")
    yield session
    session.stop()
