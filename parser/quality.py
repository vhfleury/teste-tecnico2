"""Generic data-quality helpers shared by every pipeline.

These functions only assume a DataFrame and a set of column names — pipeline
specific business rules (valid domains, regexes, etc.) stay in each
pipeline's own ``*_parser.py`` module.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def trim_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim leading/trailing whitespace on the given columns.

    Args:
        df: DataFrame to transform.
        columns: Names of the string columns to trim.

    Returns:
        A new DataFrame with the given columns trimmed.
    """
    for column in columns:
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def deduplicate_by_key(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Drop rows with a null/empty key and deduplicate by key.

    Args:
        df: DataFrame to transform.
        key_columns: Columns that make up the row's primary key.

    Returns:
        A DataFrame without null/empty keys or duplicate keys.
    """
    has_key = None
    for column in key_columns:
        column_has_value = F.col(column).isNotNull() & (F.col(column) != "")
        has_key = column_has_value if has_key is None else has_key & column_has_value
    return df.filter(has_key).dropDuplicates(key_columns)


def apply_quarantine(df: DataFrame, checks: dict[str, Column]) -> DataFrame:
    """Flag rows that fail data-quality checks without dropping them.

    Failing rows keep their primary key so joins with other tables
    still work; the reason is recorded in `dq_observations` and the
    overall row status in `quality_ok`.

    Args:
        df: DataFrame to validate.
        checks: Maps a human-readable reason to a boolean column
            that is True when the value is valid.

    Returns:
        The DataFrame with `dq_observations` and `quality_ok`
        columns added.
    """
    df = df.withColumn(
        "dq_observations",
        F.concat_ws(
            ";",
            *[F.when(~condition, F.lit(reason)) for reason, condition in checks.items()],
        ),
    )
    return df.withColumn("quality_ok", F.length("dq_observations") == 0)
