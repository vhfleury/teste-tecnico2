"""Unit tests for the shared treatment helpers."""
import pytest
from pyspark.errors import AnalysisException

from parser.treatment import apply_table_treatments, deduplicate_by_key, trim_columns

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


def test_apply_table_treatments_normalize_telefone_via_config(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "telefone", "type": "string", "treatment": "normalize_telefone"}],
    }
    df = spark.createDataFrame([("+55 (071) 2827-1996",)], ["telefone"])

    result = apply_table_treatments(df, config).collect()

    assert [row["telefone"] for row in result] == ["7128271996"]


def test_apply_table_treatments_fails_on_missing_column(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "missing", "type": "string", "treatment": "normalize"}],
    }
    df = spark.createDataFrame([("Ana Souza",)], ["nome"])

    with pytest.raises(AnalysisException):
        apply_table_treatments(df, config)


def test_trim_columns_strips_only_the_given_columns(spark):
    df = spark.createDataFrame(
        [("  ID-1  ", "  Ana Souza ", "  untouched  "), (None, "no trim needed", None)],
        ["id", "nome", "extra"],
    )

    result = trim_columns(df, ["id", "nome"]).collect()

    assert [(row["id"], row["nome"], row["extra"]) for row in result] == [
        ("ID-1", "Ana Souza", "  untouched  "),
        (None, "no trim needed", None),
    ]


def test_deduplicate_by_key_drops_null_empty_and_duplicate_keys(spark):
    df = spark.createDataFrame(
        [
            ("ID-1", "kept"),
            ("ID-1", "duplicate of ID-1"),
            (None, "null key"),
            ("", "empty key"),
            ("ID-2", "kept"),
        ],
        ["id", "label"],
    )

    result = deduplicate_by_key(df, ["id"]).collect()

    assert sorted(row["id"] for row in result) == ["ID-1", "ID-2"]


def test_deduplicate_by_key_requires_every_column_of_a_composite_key(spark):
    df = spark.createDataFrame(
        [
            ("ID-1", "2024-01-01", "kept"),
            ("ID-1", "2024-01-02", "kept, other second key"),
            ("ID-1", "2024-01-01", "duplicate composite key"),
            ("ID-1", None, "null second key"),
            ("ID-1", "", "empty second key"),
        ],
        ["id", "event_date", "label"],
    )

    result = deduplicate_by_key(df, ["id", "event_date"]).collect()

    assert sorted((row["id"], row["event_date"]) for row in result) == [
        ("ID-1", "2024-01-01"),
        ("ID-1", "2024-01-02"),
    ]
