"""Generic data treatments shared by every pipeline.

This module imports every treatment a table config can declare (see
``TREATMENTS``) and applies them (`apply_table_treatments`). Validations
that flag or abort instead of changing values live in ``validation.py``.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from parser.parser_cpf import cpf_is_valid, normalize_cpf
from parser.parser_telefone import normalize_telefone


def normalize(column: Column) -> Column:
    """Normalize a text column: all characters in uppercase.

    Args:
        column: String column to normalize.

    Returns:
        The column with its text in uppercase.
    """
    return F.upper(column)


# Treatments a table config can declare on a column. `cpf_is_valid` keeps the
# value only when the CPF is valid (invalid ones become null).
TREATMENTS = {
    "normalize": normalize,
    "normalize_cpf": normalize_cpf,
    "normalize_telefone": normalize_telefone,
    "cpf_is_valid": lambda column: F.when(cpf_is_valid(column), column),
}


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


def apply_table_treatments(df: DataFrame, config: dict) -> DataFrame:
    """Apply the treatments declared in the table config.

    Each ``schema`` entry may declare an optional ``treatment`` (a key
    of ``TREATMENTS``), applied to the column in ``name``. When the
    entry also declares ``new_name``, the result is written to a new
    column with that name (the source column is untouched); otherwise
    the treatment replaces the column itself.

    Args:
        df: DataFrame produced by the pipeline's cleaning stage.
        config: Parsed table config with `schema` (list of columns
            with `name` and optional `treatment` / `new_name`).

    Returns:
        The DataFrame with every declared treatment applied.

    Raises:
        ValueError: If a declared treatment is not in `TREATMENTS` -
            the config demands exactly that treatment, so an unknown
            one must abort instead of being skipped.
    """
    for entry in config["schema"]:
        treatment = entry.get("treatment")
        if treatment is None:
            continue
        if treatment not in TREATMENTS:
            table = config.get("table_name", "<unknown>")
            raise ValueError(
                f"Unknown treatment '{treatment}' for column '{entry['name']}' "
                f"in table config '{table}'"
            )
        target = entry.get("new_name", entry["name"])
        df = df.withColumn(target, TREATMENTS[treatment](F.col(entry["name"])))
    return df


