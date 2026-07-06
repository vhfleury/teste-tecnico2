"""Generic data treatments shared by every pipeline, driven by the table config."""
from __future__ import annotations

from parser.parser_cpf import normalize_cpf
from parser.parser_telefone import normalize_telefone
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def normalize(column: Column) -> Column:
    """Normalize a text column: trim surrounding whitespace and uppercase.

    Args:
        column: String column to normalize.

    Returns:
        The column trimmed and with its text in uppercase.
    """
    return F.upper(F.trim(column))


def parse_date(column: Column) -> Column:
    """Parse a date/datetime string into a timestamp (malformed -> null, not an abort).

    Args:
        column: String column holding the date or datetime.

    Returns:
        Timestamp column, null when the value cannot be parsed.
    """
    return F.to_timestamp(F.trim(column))


# Treatments a table config can declare on a column (`treatments`),
# applied in the declared order. Treatments change values; anything
# that flags rows instead belongs to data_quality.
TREATMENTS = {
    "trim": F.trim,
    "normalize": normalize,
    "parse_date": parse_date,
    "normalize_cpf": normalize_cpf,
    "normalize_telefone": normalize_telefone,
}


def _apply_entry_treatments(column: Column, entry: dict, table: str) -> Column:
    """Chain the treatments declared by a schema entry onto a column.

    Args:
        column: Column expression to transform.
        entry: Schema entry with `name` and optional `treatments`.
        table: Table name, used in the error message.

    Returns:
        The column with every declared treatment applied, in order.

    Raises:
        ValueError: If a declared treatment is not in `TREATMENTS`.
    """
    for treatment in entry.get("treatments", []):
        if treatment not in TREATMENTS:
            raise ValueError(
                f"Unknown treatment '{treatment}' for column '{entry['name']}' "
                f"in table config '{table}'"
            )
        column = TREATMENTS[treatment](column)
    return column


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


def drop_null_keys(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Drop rows whose primary key is null or empty (duplicate keys are flagged by the ``unique`` validation, not dropped here).

    Args:
        df: DataFrame to transform.
        key_columns: Columns that make up the row's primary key.

    Returns:
        A DataFrame without null/empty keys.
    """
    has_key = None
    for column in key_columns:
        column_has_value = F.col(column).isNotNull() & (F.col(column) != "")
        has_key = column_has_value if has_key is None else has_key & column_has_value
    return df.filter(has_key)


def apply_table_treatments(df: DataFrame, config: dict) -> DataFrame:
    """Apply each entry's treatments, cast to the declared type and drop keyless rows.

    Args:
        df: DataFrame read from the raw layer.
        config: Parsed table config with `schema` (list of columns
            with `name`, `type` and optional `treatments` / `key`).

    Returns:
        The standardized DataFrame with keyless rows dropped.

    Raises:
        ValueError: If a declared treatment is not in `TREATMENTS`.
    """
    table = config.get("table_name", "<unknown>")
    for entry in config["schema"]:
        name = entry["name"]
        if entry.get("new_name") or name not in df.columns:
            continue
        column = _apply_entry_treatments(F.col(name), entry, table)
        # Spark 3.5 (pinned, ANSI off): a malformed value casts to null and
        # is then flagged by the validations, instead of aborting the job.
        df = df.withColumn(name, column.cast(entry["type"]))

    key_columns = [
        entry["name"]
        for entry in config["schema"]
        if entry.get("key") and not entry.get("new_name")
    ]
    if key_columns:
        df = drop_null_keys(df, key_columns)
    return df


def apply_derived_columns(df: DataFrame, config: dict) -> DataFrame:
    """Create the ``new_name`` derived columns; runs after the validations so a nulled-out source derives null.

    Args:
        df: DataFrame already treated and validated.
        config: Parsed table config with `schema` entries that may
            declare `new_name` and `treatments`.

    Returns:
        The DataFrame with every declared derived column added.

    Raises:
        ValueError: If a declared treatment is not in `TREATMENTS`.
    """
    table = config.get("table_name", "<unknown>")
    for entry in config["schema"]:
        new_name = entry.get("new_name")
        if not new_name or entry["name"] not in df.columns:
            continue
        column = _apply_entry_treatments(F.col(entry["name"]), entry, table)
        df = df.withColumn(new_name, column.cast(entry["type"]))
    return df
