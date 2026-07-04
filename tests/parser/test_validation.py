"""Unit tests for the shared validation helpers."""
import pytest

from data_quality.validation import enforce_table_config

TABLE_CONFIG = {
    "table_name": "staging_example",
    "partitioned_by": ["ingest_date"],
    "schema": [
        {"name": "id", "type": "string", "example": "ID-1"},
        {"name": "amount", "type": "bigint", "example": 10},
        {"name": "ingest_date", "type": "date", "example": "2024-01-01"},
    ],
}


def test_enforce_table_config_orders_and_drops_undeclared_columns(spark):
    df = spark.createDataFrame([(10, "ID-1", "extra")], ["amount", "id", "undeclared"])

    result = enforce_table_config(df, TABLE_CONFIG)

    # Partition column is not required; declared columns come in config order.
    assert result.columns == ["id", "amount"]


def test_enforce_table_config_fails_on_missing_column(spark):
    df = spark.createDataFrame([("ID-1",)], ["id"])

    with pytest.raises(ValueError, match="missing columns: amount"):
        enforce_table_config(df, TABLE_CONFIG)


def test_enforce_table_config_fails_on_type_mismatch(spark):
    df = spark.createDataFrame([("ID-1", "10")], ["id", "amount"])

    with pytest.raises(ValueError, match="amount \\(expected bigint, got string\\)"):
        enforce_table_config(df, TABLE_CONFIG)


def test_enforce_table_config_expects_renamed_column(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "cpf", "type": "string"},
            {
                "name": "cpf",
                "type": "string",
                "treatment": "normalize_cpf",
                "new_name": "cpf_normalize",
            },
        ],
    }
    df = spark.createDataFrame([("111.111.111-11", "11111111111")], ["cpf", "cpf_normalize"])

    result = enforce_table_config(df, config)

    assert result.columns == ["cpf", "cpf_normalize"]
