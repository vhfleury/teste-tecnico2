"""Unit tests for the shared treatment helpers."""
import datetime

import pytest
from parser.treatment import (
    apply_derived_columns,
    apply_table_treatments,
    deduplicate_by_key,
    trim_columns,
)

TABLE_CONFIG = {
    "table_name": "staging_example",
    "partitioned_by": ["ingest_date"],
    "schema": [
        {"name": "id", "type": "string", "example": "ID-1"},
        {"name": "amount", "type": "bigint", "example": 10},
        {"name": "ingest_date", "type": "date", "example": "2024-01-01"},
    ],
}


def test_apply_table_treatments_normalize_trims_and_uppercases_text(spark):
    # normalize merges the former trim + normalize pair: a single
    # normalize treatment both trims surrounding whitespace and uppercases.
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "nome", "type": "string", "treatments": ["normalize"]}],
    }
    df = spark.createDataFrame([("  Ana Souza ",), (None,)], ["nome"])

    result = apply_table_treatments(df, config).collect()

    assert [row["nome"] for row in result] == ["ANA SOUZA", None]


def test_apply_table_treatments_casts_to_declared_type(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "id", "type": "string"},
            {"name": "expiry_date", "type": "date"},
        ],
    }
    df = spark.createDataFrame(
        [("ID-1", "2024-01-31"), ("ID-2", "not a date")], ["id", "expiry_date"]
    )

    result = apply_table_treatments(df, config)

    assert dict(result.dtypes)["expiry_date"] == "date"
    assert [row["expiry_date"] for row in result.collect()] == [
        datetime.date(2024, 1, 31),
        None,  # unparseable date becomes null, flagged later by validations
    ]


def test_apply_table_treatments_deduplicates_by_declared_key(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "id", "type": "string", "key": True}],
    }
    df = spark.createDataFrame(
        [("ID-1",), ("ID-1",), (None,), ("",), ("ID-2",)],
        ["id"],
    )

    result = apply_table_treatments(df, config).collect()

    assert sorted(row["id"] for row in result) == ["ID-1", "ID-2"]


def test_apply_table_treatments_skips_absent_columns(spark):
    # Partition/metadata columns declared in the config are added later in
    # the flow; enforce_table_config catches a genuinely missing column.
    df = spark.createDataFrame([("ID-1", 10)], ["id", "amount"])

    result = apply_table_treatments(df, TABLE_CONFIG).collect()

    assert [(row["id"], row["amount"]) for row in result] == [("ID-1", 10)]


def test_apply_table_treatments_fails_on_unknown_treatment(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "nome", "type": "string", "treatments": ["does_not_exist"]}],
    }
    df = spark.createDataFrame([("Ana Souza",)], ["nome"])

    with pytest.raises(ValueError, match="Unknown treatment 'does_not_exist'"):
        apply_table_treatments(df, config)


def test_apply_table_treatments_normalize_telefone_via_config(spark):
    config = {
        "table_name": "staging_example",
        "schema": [{"name": "telefone", "type": "string", "treatments": ["normalize_telefone"]}],
    }
    df = spark.createDataFrame([("+55 (071) 2827-1996",)], ["telefone"])

    result = apply_table_treatments(df, config).collect()

    assert [row["telefone"] for row in result] == ["7128271996"]


def test_apply_table_treatments_casts_string_to_boolean(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "geocerca_id", "type": "string"},
            {"name": "ativo", "type": "boolean"},
        ],
    }
    df = spark.createDataFrame(
        [("GEO-1", "true"), ("GEO-2", "false"), ("GEO-3", "sim"), ("GEO-4", None)],
        ["geocerca_id", "ativo"],
    )

    result = apply_table_treatments(df, config).collect()

    # A non-boolean token ("sim") casts to null and is flagged later by the
    # validations, instead of aborting the job (Spark 3.5, ANSI off).
    assert [(row["geocerca_id"], row["ativo"]) for row in result] == [
        ("GEO-1", True),
        ("GEO-2", False),
        ("GEO-3", None),
        ("GEO-4", None),
    ]


def test_apply_derived_columns_creates_new_column_from_source(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "cpf", "type": "string"},
            {
                "name": "cpf",
                "type": "string",
                "treatments": ["normalize_cpf"],
                "new_name": "cpf_normalize",
            },
        ],
    }
    df = spark.createDataFrame([("529.982.247-25",), (None,)], ["cpf"])

    result = apply_derived_columns(df, config).collect()

    # The source column is untouched; a null source derives null.
    assert [(row["cpf"], row["cpf_normalize"]) for row in result] == [
        ("529.982.247-25", "52998224725"),
        (None, None),
    ]


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
