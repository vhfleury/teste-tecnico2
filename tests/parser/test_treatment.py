"""Unit tests for the shared treatment helpers."""
import pytest

from parser.treatment import apply_table_treatments

TABLE_CONFIG = {
    "table_name": "staging_example",
    "partitioned_by": ["ingest_date"],
    "schema": [
        {"name": "id", "type": "string", "example": "ID-1"},
        {"name": "amount", "type": "bigint", "example": 10},
        {"name": "ingest_date", "type": "date", "example": "2024-01-01"},
    ],
}


def test_apply_table_treatments_normalize_uppercases_text(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "nome", "type": "string", "treatment": "normalize"}],
    }
    df = spark.createDataFrame([("Ana Souza",), (None,)], ["nome"])

    result = apply_table_treatments(df, config).collect()

    assert [row["nome"] for row in result] == ["ANA SOUZA", None]


def test_apply_table_treatments_new_name_creates_derived_column(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "cpf", "type": "string", "treatment": "cpf_is_valid"},
            {
                "name": "cpf",
                "type": "string",
                "treatment": "normalize_cpf",
                "new_name": "cpf_normalize",
            },
        ],
    }
    df = spark.createDataFrame(
        [("529.982.247-25",), ("111.111.111-11",)],  # valid / invalid check digits
        ["cpf"],
    )

    result = apply_table_treatments(df, config).collect()

    # cpf_is_valid keeps only valid CPFs; the derived column holds the digits.
    assert [(row["cpf"], row["cpf_normalize"]) for row in result] == [
        ("529.982.247-25", "52998224725"),
        (None, None),
    ]


def test_apply_table_treatments_without_treatment_keeps_column(spark):
    df = spark.createDataFrame([("ID-1", 10)], ["id", "amount"])

    result = apply_table_treatments(df, TABLE_CONFIG).collect()

    assert [(row["id"], row["amount"]) for row in result] == [("ID-1", 10)]


def test_apply_table_treatments_fails_on_unknown_treatment(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "nome", "type": "string", "treatment": "does_not_exist"}],
    }
    df = spark.createDataFrame([("Ana Souza",)], ["nome"])

    with pytest.raises(ValueError, match="Unknown treatment 'does_not_exist'"):
        apply_table_treatments(df, config)
