"""Unit tests for the shared phone validation helpers."""
from pyspark.sql import functions as F

from parser.parser_telefone import (
    ddd_from_telefone,
    ddd_to_uf,
    normalize_telefone,
    telefone_is_valid,
)


def test_normalize_telefone_strips_country_code_and_trunk_zero(spark):
    cases = [
        ("+55 (071) 2827-1996", "7128271996"),  # country code + trunk zero
        ("(084) 6877-3092", "8468773092"),  # trunk zero only
        ("81 6342 7352", "8163427352"),  # already bare
        ("+55 41 0184-7529", "4101847529"),  # country code, no trunk zero
        ("(62) 99876-5432", "62998765432"),  # 9-digit mobile
        ("55 3222-1111", "5532221111"),  # DDD 55 without country code must keep its 55
        ("+55 55 99876-5432", "55998765432"),  # country code stripped, DDD 55 kept
        ("", ""),  # empty
        (None, None),  # missing stays missing
    ]
    df = spark.createDataFrame([(telefone,) for telefone, _ in cases], ["telefone"])

    result = df.withColumn("normalized", normalize_telefone(F.col("telefone"))).collect()

    assert [row["normalized"] for row in result] == [expected for _, expected in cases]


def test_ddd_from_telefone_extracts_area_code(spark):
    cases = [
        ("+55 (071) 2827-1996", "71"),
        ("(62) 99876-5432", "62"),
        ("81 6342 7352", "81"),
    ]
    df = spark.createDataFrame([(telefone,) for telefone, _ in cases], ["telefone"])

    result = df.withColumn("ddd", ddd_from_telefone(F.col("telefone"))).collect()

    assert [row["ddd"] for row in result] == [expected for _, expected in cases]


def test_ddd_to_uf_resolves_state(spark):
    cases = [
        ("11", "SP"),
        ("62", "GO"),
        ("71", "BA"),
        ("81", "PE"),
        ("51", "RS"),
        ("20", None),  # DDD does not exist
        (None, None),  # missing
    ]
    df = spark.createDataFrame([(ddd,) for ddd, _ in cases], ["ddd"])

    result = df.withColumn("uf", ddd_to_uf(F.col("ddd"))).collect()

    assert [row["uf"] for row in result] == [expected for _, expected in cases]


def test_telefone_is_valid_checks_ddd_and_subscriber(spark):
    cases = [
        ("+55 (071) 2827-1996", True),  # landline with country code + trunk zero
        ("(62) 99876-5432", True),  # 9-digit mobile
        ("81 6342 7352", True),  # bare landline
        ("55 3222-1111", True),  # DDD 55 landline, not mistaken for country code
        ("+55 (021) 0935-2257", False),  # subscriber starts with 0
        ("81 0878 5042", False),  # subscriber starts with 0
        ("81 1878 5042", False),  # subscriber starts with 1
        ("(20) 3333-4444", False),  # DDD does not exist
        ("(62) 89876-5432", False),  # 11 digits but not a mobile (no leading 9)
        ("1234", False),  # too short
        ("abc", False),  # no digits
        ("", False),  # empty
        (None, False),  # missing
    ]
    df = spark.createDataFrame([(telefone,) for telefone, _ in cases], ["telefone"])

    result = df.withColumn("valid", telefone_is_valid(F.col("telefone"))).collect()

    assert [bool(row["valid"]) for row in result] == [expected for _, expected in cases]
