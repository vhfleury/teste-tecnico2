"""Generic data treatments shared by every pipeline.

This module executes what a table config declares for each column:
the ``treatments`` chain (keys of ``TREATMENTS``, applied in order),
the cast to the declared ``type`` and the deduplication by the
declared ``key`` columns (`apply_table_treatments`), plus the
``new_name`` derived columns (`apply_derived_columns`). Validations
that flag or abort instead of changing values live in
``data_quality/validation.py``.
"""
from __future__ import annotations

from parser.parser_cpf import normalize_cpf
from parser.parser_telefone import normalize_telefone
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def normalize(column: Column) -> Column:
    """Normalize a text column: trim surrounding whitespace and uppercase.

    Merges the former ``trim`` and ``normalize`` treatments into one, so
    a column only needs to declare ``normalize`` to be trimmed and
    uppercased.

    Args:
        column: String column to normalize.

    Returns:
        The column trimmed and with its text in uppercase.
    """
    return F.upper(F.trim(column))


# Treatments a table config can declare on a column (`treatments`),
# applied in the declared order. Treatments change values; anything
# that flags rows instead belongs to data_quality.
TREATMENTS = {
    "trim": F.trim,
    "normalize": normalize,
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
        ValueError: If a declared treatment is not in `TREATMENTS` -
            the config demands exactly that treatment, so an unknown
            one must abort instead of being skipped.
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
    """Standardize the DataFrame as declared in the table config.

    For every schema entry whose column exists in the DataFrame:
    apply the declared ``treatments`` in order, then cast to the
    declared ``type`` (the raw layer reads every primitive as a
    string, so typing happens here). Entries with ``new_name`` are
    derived columns, handled later by `apply_derived_columns`;
    entries absent from the DataFrame (partition and metadata columns
    added downstream) are skipped - `enforce_table_config` catches a
    genuinely missing column before the write. Finally, rows are
    deduplicated by the entries marked ``key``.

    Args:
        df: DataFrame read from the raw layer.
        config: Parsed table config with `schema` (list of columns
            with `name`, `type` and optional `treatments` / `key`).

    Returns:
        The standardized, deduplicated DataFrame.

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
        df = deduplicate_by_key(df, key_columns)
    return df


def apply_derived_columns(df: DataFrame, config: dict) -> DataFrame:
    """Create the ``new_name`` derived columns declared in the config.

    Runs after the validations so a derived column (e.g. the
    digits-only CPF) is computed from the final, quarantined value of
    its source column - an invalid source that was nulled out derives
    null, not a normalized copy of a bad value.

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
