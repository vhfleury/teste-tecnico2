"""Fixture infrastructure of the pipeline golden tests."""
import importlib
import json
import os

import staging_pipeline
from data_quality.validation import enforce_table_config
from general.utils import load_table_config
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, NullType, StringType, StructField, StructType

from tests.pipelines.diff import dataframe_to_rows

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
        require_expected: When False, the expected output file is not required.

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


def _merge_types(current: DataType, new: DataType) -> DataType:
    """Merge two inferred fixture types; NullType yields to the other side."""
    if isinstance(current, NullType):
        return new
    if isinstance(new, NullType):
        return current
    if isinstance(current, StructType) and isinstance(new, StructType):
        merged = {field.name: field.dataType for field in current.fields}
        order = [field.name for field in current.fields]
        for field in new.fields:
            if field.name in merged:
                merged[field.name] = _merge_types(merged[field.name], field.dataType)
            else:
                merged[field.name] = field.dataType
                order.append(field.name)
        return StructType([StructField(name, merged[name]) for name in order])
    if isinstance(current, ArrayType) and isinstance(new, ArrayType):
        return ArrayType(_merge_types(current.elementType, new.elementType))
    return current


def _infer_type(value) -> DataType:
    """Infer the Spark type of one fixture value: strings at the leaves."""
    if value is None:
        return NullType()
    if isinstance(value, dict):
        return StructType(
            [StructField(key, _infer_type(item)) for key, item in value.items()]
        )
    if isinstance(value, list):
        element: DataType = NullType()
        for item in value:
            element = _merge_types(element, _infer_type(item))
        return ArrayType(element)
    return StringType()


def _resolve_unknown(data_type: DataType) -> DataType:
    """Replace NullType placeholders (fields null in every row) with string."""
    if isinstance(data_type, NullType):
        return StringType()
    if isinstance(data_type, StructType):
        return StructType(
            [
                StructField(field.name, _resolve_unknown(field.dataType))
                for field in data_type.fields
            ]
        )
    if isinstance(data_type, ArrayType):
        return ArrayType(_resolve_unknown(data_type.elementType))
    return data_type


def infer_raw_schema(rows: list[dict]) -> StructType:
    """Build the raw-layer schema of an input fixture, merged across rows.

    Args:
        rows: Raw rows loaded from the input fixture.

    Returns:
        The merged schema covering every field seen in any row.
    """
    merged: DataType = NullType()
    for row in rows:
        merged = _merge_types(merged, _infer_type(row))
    return _resolve_unknown(merged)


def resolve_clean_and_validate(name: str):
    """Locate the treatment chain a pipeline runs on fixtures."""
    try:
        module = importlib.import_module(f"{name}.{name}")
    except ModuleNotFoundError as error:
        if error.name != f"{name}.{name}":
            raise
        return staging_pipeline.clean_and_validate
    return module.clean_and_validate


def produce_staging(spark, name: str) -> tuple[list[dict], list[str]]:
    """Run a pipeline's real treatment chain over its input fixture.

    Args:
        spark: Active SparkSession.
        name: Pipeline name (directory under pipelines/).

    Returns:
        The staging rows as JSON-friendly dicts (runtime metadata
        excluded) and the primary-key columns declared in the config.
    """
    clean_and_validate = resolve_clean_and_validate(name)
    rows = load_fixture(name, f"input_{name}.json")[f"raw_{name}"]
    config = load_table_config(os.path.join(PIPELINES_ROOT, name, f"staging_{name}.json"))

    # Raw layer shape: every leaf read as string (primitivesAsString),
    # nested objects as structs/arrays; ingested_at is the typed
    # metadata column added by extract_to_raw.
    schema = infer_raw_schema(rows)
    raw = spark.createDataFrame(rows, schema)
    if "ingested_at" in schema.names:
        raw = raw.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    df = enforce_table_config(clean_and_validate(raw, config), config)

    key_columns = [entry["name"] for entry in config["schema"] if entry.get("key")]
    return dataframe_to_rows(df, mask=RUNTIME_COLUMNS), key_columns
