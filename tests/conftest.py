import pytest

from connections.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()
