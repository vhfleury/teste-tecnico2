"""Generic validation helpers shared by every pipeline.

Validations never mutate valid values: they flag rows
(`apply_table_validations` / `apply_quarantine`) or abort the pipeline
when the data drifts from its table config (`enforce_table_config`).
Value-changing logic lives in ``parser/treatment.py``.
"""
from __future__ import annotations

from data_quality.statics import (
    VALID_DRIVER_STATUS,
    VALID_GEOFENCE_TYPES,
    VALID_VEHICLE_STATUS,
    VALID_VEHICLE_TYPES,
)
from parser.parser_cnh import cnh_category_is_valid, cnh_is_valid
from parser.parser_cpf import cpf_is_valid
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def required(column: Column) -> Column:
    """Validate that a value is present: not null and not empty.

    Args:
        column: Column of any type to check.

    Returns:
        Boolean column, False when the value is null or an empty
        string.
    """
    return column.isNotNull() & (column.cast("string") != "")


def plate_is_valid(column: Column) -> Column:
    """Validate a Brazilian license plate.

    Accepts the old format (`ABC1234`) and the Mercosul format
    (`ABC1D23`). The value is expected to be already trimmed and
    uppercased by the treatments.

    Args:
        column: String column with the plate to check.

    Returns:
        Boolean column, True when the plate matches either format.
    """
    return column.rlike("^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$")


# Spark SQL schema of a GeoJSON Polygon geometry, as serialized by the
# staging canonicalization (numeric coordinates).
GEOJSON_POLYGON_SCHEMA = "type string, coordinates array<array<array<double>>>"


def geojson_polygon_is_valid(column: Column) -> Column:
    """Validate a GeoJSON Polygon serialized as a JSON string.

    Structural check on the outer ring: the value must parse as a
    ``Polygon`` whose ring has at least 4 points and contains no
    zeroed ``(0, 0)`` coordinate (the "null island" defect). A null
    geometry passes, so presence can be flagged separately by
    ``required`` under its own reason.

    Args:
        column: String column holding the GeoJSON geometry.

    Returns:
        Boolean column, True when the geometry is null or
        structurally valid.
    """
    geometry = F.from_json(column, GEOJSON_POLYGON_SCHEMA)
    ring = geometry["coordinates"].getItem(0)
    has_zeroed_point = F.exists(
        ring, lambda point: (point.getItem(0) == 0.0) & (point.getItem(1) == 0.0)
    )
    valid = (
        (geometry["type"] == "Polygon")
        & (F.size(geometry["coordinates"]) > 0)
        & (F.size(ring) >= 4)
        & ~has_zeroed_point
    )
    return column.isNull() | valid


# Checks a table config can declare on a column (`validations`). Each
# check maps to a boolean column that is True when the value is valid;
# `apply_table_validations` wraps it null-safely (null => invalid).
VALIDATIONS = {
    "required": required,
    "non_negative": lambda column: column >= 0,
    "positive": lambda column: column > 0,
    "cpf_is_valid": cpf_is_valid,
    "cnh_is_valid": cnh_is_valid,
    "cnh_category_is_valid": cnh_category_is_valid,
    "plate_is_valid": plate_is_valid,
    "driver_status_is_valid": lambda column: column.isin(VALID_DRIVER_STATUS),
    "vehicle_status_is_valid": lambda column: column.isin(VALID_VEHICLE_STATUS),
    "vehicle_type_is_valid": lambda column: column.isin(VALID_VEHICLE_TYPES),
    "geofence_type_is_valid": lambda column: column.isin(VALID_GEOFENCE_TYPES),
    "geojson_polygon_is_valid": geojson_polygon_is_valid,
}


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


def apply_table_validations(df: DataFrame, config: dict) -> DataFrame:
    """Run the validations declared in the table config (quarantine).

    Each ``schema`` entry may declare ``validations``: a list of
    ``{"check", "reason"}`` pairs, where ``check`` is a key of
    ``VALIDATIONS`` and ``reason`` is the label recorded in
    `dq_observations`. Every check is wrapped null-safely (a null
    boolean counts as invalid). Failing rows are flagged, never
    dropped, and the failing value is nulled out - the row keeps its
    primary key so joins still work.

    Args:
        df: DataFrame already standardized by the treatments.
        config: Parsed table config with `schema` entries that may
            declare `validations`.

    Returns:
        The DataFrame with `dq_observations`/`quality_ok` added and
        every failing value nulled out.

    Raises:
        ValueError: If a declared check is not in `VALIDATIONS` -
            the config demands exactly that check, so an unknown one
            must abort instead of being skipped.
    """
    table = config.get("table_name", "<unknown>")
    checks: dict[str, Column] = {}
    column_conditions: dict[str, list[Column]] = {}
    for entry in config["schema"]:
        name = entry["name"]
        if entry.get("new_name") or name not in df.columns:
            continue
        for validation in entry.get("validations", []):
            check, reason = validation["check"], validation["reason"]
            if check not in VALIDATIONS:
                raise ValueError(
                    f"Unknown validation '{check}' for column '{name}' "
                    f"in table config '{table}'"
                )
            condition = F.coalesce(VALIDATIONS[check](F.col(name)), F.lit(False))
            checks[reason] = condition
            column_conditions.setdefault(name, []).append(condition)

    df = apply_quarantine(df, checks)

    # Quarantine: null out the failing value but keep the row.
    for name, conditions in column_conditions.items():
        passed = conditions[0]
        for condition in conditions[1:]:
            passed = passed & condition
        df = df.withColumn(name, F.when(passed, F.col(name)))
    return df


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
