"""Generic golden test: every pipeline with fixtures gets tested.

For each directory under pipelines/ holding the fixture trio
(input_<name>.json, staging_<name>.json, output_staging_<name>.json),
loads the curated raw input, runs the pipeline's treatment chain and
compares the result with the frozen expected output, keyed by the
primary key declared in the table config. Pipelines with exclusive
treatment ship their own ``clean_and_validate`` in
``pipelines/<name>/<name>.py``; declarative sources have no module
and run the generic chain from ``staging_pipeline``. A new pipeline
earns this test just by shipping its three files - no new test code.

The fixture infrastructure (discovery, loading, raw-schema inference,
treatment-chain execution) lives in ``tests/pipelines/fixtures.py``.

After an INTENTIONAL contract change, refresh the expected file with:
    python -m tests.pipelines.regenerate_expected <pipeline>
"""
import pytest

from tests.pipelines.diff import assert_matches_expected
from tests.pipelines.fixtures import discover_pipelines, load_fixture, produce_staging


@pytest.mark.parametrize("name", discover_pipelines())
def test_staging_matches_golden(spark, name):
    result, key_columns = produce_staging(spark, name)

    expected = load_fixture(name, f"output_staging_{name}.json")[f"staging_{name}"]

    assert_matches_expected(result, expected, key_columns)
