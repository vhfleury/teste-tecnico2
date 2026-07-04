"""Unit tests for the shared CNH validation helpers."""
from pyspark.sql import functions as F

from parser.parser_cnh import cnh_category_is_valid, cnh_is_valid


def test_cnh_is_valid_requires_eleven_digits(spark):
    cases = [
        ("12345678900", True),  # 11 digits
        ("1234567890", False),  # too short
        ("123456789001", False),  # too long
        ("1234567890a", False),  # non-digit character
        ("", False),  # empty
        (None, False),  # missing
    ]
    df = spark.createDataFrame([(cnh,) for cnh, _ in cases], ["cnh"])

    result = df.withColumn("valid", cnh_is_valid(F.col("cnh"))).collect()

    assert [bool(row["valid"]) for row in result] == [expected for _, expected in cases]


def test_cnh_category_is_valid_accepts_truck_driver_categories(spark):
    cases = [
        ("C", True),
        ("D", True),
        ("E", True),
        ("A", False),  # existing category, but not a truck-driver one
        ("X", False),  # not a CNH category
        (None, False),  # missing
    ]
    df = spark.createDataFrame([(category,) for category, _ in cases], ["categoria_cnh"])

    result = df.withColumn("valid", cnh_category_is_valid(F.col("categoria_cnh"))).collect()

    assert [bool(row["valid"]) for row in result] == [expected for _, expected in cases]
