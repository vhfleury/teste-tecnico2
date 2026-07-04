"""Generic golden test: every pipeline with fixtures gets tested.

For each directory under pipelines/ holding the fixture trio
(input_<name>.json, staging_<name>.json, output_staging_<name>.json),
loads the curated raw input, runs the pipeline's own
``clean_and_validate`` (imported by convention from
``pipelines/<name>/<name>.py``) and compares the result with the
frozen expected output, keyed by the primary key declared in the
table config. A new pipeline earns this test just by shipping its
three files - no new test code.

After an INTENTIONAL contract change, refresh the expected file with:
    python -m tests.pipelines.regenerate_expected <pipeline>
"""
import importlib
import json
import os

import pytest
from data_quality.validation import enforce_table_config
from general.utils import load_table_config
from pyspark.sql import functions as F

from tests.pipelines.diff import assert_matches_expected, dataframe_to_rows

PIPELINES_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "pipelines",
)

# Runtime metadata: the column must exist in the output (config is the
# contract), but its value is generated at execution time, so it is
# masked to null on both sides of the comparison.
RUNTIME_COLUMNS = ("processed_at",)


def discover_pipelines(require_expected: bool = True) -> list[str]:
    """List the pipelines under pipelines/ that ship golden fixtures.

    Args:
        require_expected: When False, the expected output file is not
            required - used by the regeneration script to create the
            very first golden of a new pipeline.

    Returns:
        Sorted pipeline names.
    """
    names = []
    for name in sorted(os.listdir(PIPELINES_ROOT)):
        fixtures = [f"input_{name}.json", f"staging_{name}.json"]
        if require_expected:
            fixtures.append(f"output_staging_{name}.json")
        if all(os.path.exists(os.path.join(PIPELINES_ROOT, name, fixture)) for fixture in fixtures):
            names.append(name)
    return names


def load_fixture(name: str, file_name: str) -> dict:
    with open(os.path.join(PIPELINES_ROOT, name, file_name), encoding="utf-8") as file:
        return json.load(file)


def produce_staging(spark, name: str) -> tuple[list[dict], list[str]]:
    """Run a pipeline's real treatment chain over its input fixture.

    Single source of truth for how a pipeline runs on fixtures: used
    by the golden test below and by the expected-file regeneration
    script, so both always execute the same chain.

    Args:
        spark: Active SparkSession.
        name: Pipeline name (directory under pipelines/).

    Returns:
        The staging rows as JSON-friendly dicts (runtime metadata
        excluded) and the primary-key columns declared in the config.
    """
    module = importlib.import_module(f"{name}.{name}")
    rows = load_fixture(name, f"input_{name}.json")[f"raw_{name}"]
    config = load_table_config(os.path.join(PIPELINES_ROOT, name, f"staging_{name}.json"))

    # Raw layer shape: every source field read as string (primitivesAsString);
    # ingested_at is the typed metadata column added by extract_to_raw.
    columns = list(rows[0].keys())
    schema = ", ".join(f"{column} string" for column in columns)
    raw = spark.createDataFrame(
        [tuple(row.get(column) for column in columns) for row in rows], schema
    )
    if "ingested_at" in columns:
        raw = raw.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    df = enforce_table_config(module.clean_and_validate(raw, config), config)

    key_columns = [entry["name"] for entry in config["schema"] if entry.get("key")]
    return dataframe_to_rows(df, mask=RUNTIME_COLUMNS), key_columns


@pytest.mark.parametrize("name", discover_pipelines())
def test_staging_matches_golden(spark, name):
    result, key_columns = produce_staging(spark, name)

    expected = load_fixture(name, f"output_staging_{name}.json")[f"staging_{name}"]

    assert_matches_expected(result, expected, key_columns)
