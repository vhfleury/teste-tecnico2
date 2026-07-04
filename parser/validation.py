"""Generic validation helpers shared by every pipeline.

Validations never mutate values: they flag rows (`apply_quarantine`) or
abort the pipeline when the data drifts from its table config
(`enforce_table_config`). Value-changing logic lives in ``treatment.py``.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Registry status/type domains (docs/dados.md).
VALID_DRIVER_STATUS = ["ativo", "ferias", "afastado", "desligado"]
VALID_VEHICLE_STATUS = ["ativo", "em_manutencao", "inativo"]
VALID_VEHICLE_TYPES = [
    "VUC",
    "Caminhão Toco",
    "Caminhão Truck",
    "Carreta Simples",
    "Carreta LS",
    "Bitrem",
]


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


def enforce_table_config(df: DataFrame, config: dict) -> DataFrame:
    """Validate a DataFrame against a table config and order its columns.

    The config is the table's contract: every non-partition column
    declared in ``schema`` must be present with the declared type,
    otherwise the pipeline fails instead of writing a table that
    drifted from its config. Partition columns (``partitioned_by``)
    are not required in the DataFrame - they only materialize in the
    path when the partition is written. Columns not declared in the
    config are dropped.

    Args:
        df: DataFrame about to be written to the table.
        config: Parsed table config with `schema` (list of columns
            with `name` and `type`) and optional `partitioned_by`.

    Returns:
        The DataFrame with exactly the declared columns, in the
        config order.

    Raises:
        ValueError: If a declared column is missing or has a type
            different from the config.
    """
    partition_columns = set(config.get("partitioned_by", []))
    expected = {
        column.get("new_name", column["name"]): column["type"]
        for column in config["schema"]
        if column.get("new_name", column["name"]) not in partition_columns
    }
    actual = dict(df.dtypes)

    missing = [name for name in expected if name not in actual]
    mismatched = [
        f"{name} (expected {expected[name]}, got {actual[name]})"
        for name in expected
        if name in actual and actual[name] != expected[name]
    ]
    if missing or mismatched:
        table = config.get("table_name", "<unknown>")
        problems = []
        if missing:
            problems.append(f"missing columns: {', '.join(missing)}")
        if mismatched:
            problems.append(f"type mismatches: {', '.join(mismatched)}")
        raise ValueError(f"DataFrame does not match table config '{table}': {'; '.join(problems)}")

    return df.select(*expected)
