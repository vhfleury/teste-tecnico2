"""Unit tests for the shared CPF validation helpers."""
from parser.parser_cpf import cpf_is_valid, normalize_cpf
from pyspark.sql import functions as F


def test_normalize_cpf_keeps_only_digits(spark):
    cases = [
        ("529.982.247-25", "52998224725"),  # formatted
        ("52998224725", "52998224725"),  # already digits only
        ("529 982 247 25", "52998224725"),  # stray separators
        ("", ""),  # empty
        (None, None),  # missing stays missing
    ]
    df = spark.createDataFrame([(cpf,) for cpf, _ in cases], ["cpf"])

    result = df.withColumn("normalized", normalize_cpf(F.col("cpf"))).collect()

    assert [row["normalized"] for row in result] == [expected for _, expected in cases]


def test_cpf_is_valid_applies_format_and_check_digits(spark):
    cases = [
        ("529.982.247-25", True),  # valid check digits (remainders 2 and 5)
        ("123.456.789-09", True),  # valid check digits
        ("987.654.321-00", True),  # valid check digits
        ("529.982.247-35", False),  # first check digit wrong
        ("529.982.247-24", False),  # second check digit wrong
        ("111.222.333-44", False),  # check digits do not match
        ("100.000.001-08", True),  # remainder-10 rule: remainder 10 becomes check digit 0
        ("100.000.001-18", False),  # remainder 10 must map to 0, not 1
        ("111.111.111-11", False),  # all-same digits
        ("999999", False),  # malformed, too short
        ("12345678909", True),  # digits-only form is accepted after normalization
        ("11111111111", False),  # all-same digits, digits-only form
        ("", False),  # empty
        (None, False),  # missing
    ]
    df = spark.createDataFrame([(cpf,) for cpf, _ in cases], ["cpf"])

    result = df.withColumn("valid", cpf_is_valid(F.col("cpf"))).collect()

    assert [bool(row["valid"]) for row in result] == [expected for _, expected in cases]
