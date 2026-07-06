"""Generic golden test: every pipeline with fixtures gets tested."""
import pytest

from tests.pipelines.diff import assert_matches_expected
from tests.pipelines.fixtures import discover_pipelines, load_fixture, produce_staging


@pytest.mark.parametrize("name", discover_pipelines())
def test_staging_matches_golden(spark, name):
    result, key_columns = produce_staging(spark, name)

    expected = load_fixture(name, f"output_staging_{name}.json")[f"staging_{name}"]

    assert_matches_expected(result, expected, key_columns)
