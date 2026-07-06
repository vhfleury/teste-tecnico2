"""Generic validation helpers shared by every pipeline.

Validations never mutate valid values: they flag rows
(`apply_table_validations` / `apply_quarantine`) or abort the pipeline
when the data drifts from its table config (`enforce_table_config`).
Value-changing logic lives in ``parser/treatment.py``.
"""
from __future__ import annotations

from data_quality.statics import (
    BRAZIL_LATITUDE_MAX,
    BRAZIL_LATITUDE_MIN,
    BRAZIL_LONGITUDE_MAX,
    BRAZIL_LONGITUDE_MIN,
    MAX_SPEED_KMH,
    VALID_DRIVER_STATUS,
    VALID_GEOFENCE_TYPES,
    VALID_TRIP_STATUS,
    VALID_VEHICLE_STATUS,
    VALID_VEHICLE_TYPES,
)
from parser.parser_cnh import cnh_category_is_valid, cnh_is_valid
from parser.parser_cpf import cpf_is_valid
from pyspark.sql import Column, DataFrame, Window
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


def coordinates_are_in_brazil(latitude: Column, longitude: Column) -> Column:
    """Validate a GPS coordinate pair against broad Brazil bounds.

    The zeroed ``(0, 0)`` sentinel is intentionally left to the
    dedicated ``non_zero`` checks so the reason remains precise.

    Args:
        latitude: Latitude column.
        longitude: Longitude column.

    Returns:
        Boolean column, True when the coordinate pair falls inside
        Brazil's broad bounding box, or when it is the zeroed sentinel
        handled by the zero checks.
    """
    zeroed_coordinate = (latitude == 0.0) & (longitude == 0.0)
    inside_brazil_bounds = (
        latitude.between(BRAZIL_LATITUDE_MIN, BRAZIL_LATITUDE_MAX)
        & longitude.between(BRAZIL_LONGITUDE_MIN, BRAZIL_LONGITUDE_MAX)
    )
    return zeroed_coordinate | inside_brazil_bounds


# Spark SQL schema of a GeoJSON Polygon geometry, as serialized by the
# staging canonicalization (numeric coordinates).
GEOJSON_POLYGON_SCHEMA = "type string, coordinates array<array<array<double>>>"


def _polygon_checks(column: Column) -> tuple[Column, Column, Column]:
    """Build the expressions shared by the GeoJSON Polygon checks.

    Single definition of what a structurally valid polygon is, shared
    by `geojson_polygon_is_valid` and `geojson_polygon_is_in_brazil`.
    The expressions are not null-safe: an unparseable geometry yields
    null, and each caller decides how nulls count for its concern.

    Args:
        column: String column holding the GeoJSON geometry.

    Returns:
        A tuple with the outer ring, the structural-validity flag
        (``Polygon`` type, non-empty coordinates, ring with at least
        4 points) and the zeroed-point flag (the "null island"
        defect).
    """
    geometry = F.from_json(column, GEOJSON_POLYGON_SCHEMA)
    ring = geometry["coordinates"].getItem(0)
    structurally_valid = (
        (geometry["type"] == "Polygon")
        & (F.size(geometry["coordinates"]) > 0)
        & (F.size(ring) >= 4)
    )
    has_zeroed_point = F.exists(
        ring, lambda point: (point.getItem(0) == 0.0) & (point.getItem(1) == 0.0)
    )
    return ring, structurally_valid, has_zeroed_point


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
    _, structurally_valid, has_zeroed_point = _polygon_checks(column)
    return column.isNull() | (structurally_valid & ~has_zeroed_point)


def geojson_polygon_is_in_brazil(column: Column) -> Column:
    """Validate that a GeoJSON Polygon's outer ring is inside Brazil bounds.

    This check only owns the geographic-bounds concern. Null,
    unparseable, non-Polygon, degenerate or zeroed geometries pass here
    so `required` and `geojson_polygon_is_valid` can record the precise
    structural reason.

    Args:
        column: String column holding the GeoJSON geometry.

    Returns:
        Boolean column, False when a structurally valid polygon has at
        least one outer-ring point outside Brazil's broad bounding box.
    """
    ring, structurally_valid, has_zeroed_point = _polygon_checks(column)
    valid_geometry = (
        F.coalesce(structurally_valid, F.lit(False))
        & ~F.coalesce(has_zeroed_point, F.lit(False))
    )
    has_outside_point = F.coalesce(
        F.exists(
            ring,
            lambda point: (
                (point.getItem(1) < BRAZIL_LATITUDE_MIN)
                | (point.getItem(1) > BRAZIL_LATITUDE_MAX)
                | (point.getItem(0) < BRAZIL_LONGITUDE_MIN)
                | (point.getItem(0) > BRAZIL_LONGITUDE_MAX)
            ),
        ),
        F.lit(False),
    )
    return column.isNull() | ~valid_geometry | ~has_outside_point


# Config check name for the windowed uniqueness quarantine. It is not
# a per-value predicate like the entries in ``VALIDATIONS``: it depends
# on the other rows, so it is handled by `apply_unique_quarantine`
# (single, stable window pass) instead of `_resolve_checks`.
UNIQUE_CHECK = "unique"


# Checks a table config can declare on a column (`validations`). Each
# check maps to a boolean column that is True when the value is valid;
# `apply_table_validations` wraps it null-safely (null => invalid).
VALIDATIONS = {
    "required": required,
    "non_negative": lambda column: column >= 0,
    "positive": lambda column: column > 0,
    "non_zero": lambda column: column != 0,
    "speed_within_limit": lambda column: column <= MAX_SPEED_KMH,
    "coordinates_in_brazil": coordinates_are_in_brazil,
    "cpf_is_valid": cpf_is_valid,
    "cnh_is_valid": cnh_is_valid,
    "cnh_category_is_valid": cnh_category_is_valid,
    "plate_is_valid": plate_is_valid,
    "driver_status_is_valid": lambda column: column.isin(VALID_DRIVER_STATUS),
    "vehicle_status_is_valid": lambda column: column.isin(VALID_VEHICLE_STATUS),
    "vehicle_type_is_valid": lambda column: column.isin(VALID_VEHICLE_TYPES),
    "geofence_type_is_valid": lambda column: column.isin(VALID_GEOFENCE_TYPES),
    "trip_status_is_valid": lambda column: column.isin(VALID_TRIP_STATUS),
    "geojson_polygon_is_valid": geojson_polygon_is_valid,
    "geojson_polygon_is_in_brazil": geojson_polygon_is_in_brazil,
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


def _resolve_checks(
    df: DataFrame, config: dict
) -> tuple[dict[str, Column], dict[str, list[Column]]]:
    """Build the check expressions the table config declares.

    Args:
        df: DataFrame to validate.
        config: Parsed table config with `schema` entries that may
            declare `validations`.

    Returns:
        Two maps: reason -> null-safe boolean check (True when the
        value is valid), and column name -> conditions that must all
        pass for the column's value to be kept.

    Raises:
        ValueError: If a declared check is not in `VALIDATIONS` or
            references a column missing from the DataFrame - the
            config demands exactly that check, so it must abort
            instead of being skipped.
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
            if check == UNIQUE_CHECK:
                # Windowed, multi-row check: handled by apply_unique_quarantine.
                continue
            if check not in VALIDATIONS:
                raise ValueError(
                    f"Unknown validation '{check}' for column '{name}' "
                    f"in table config '{table}'"
                )
            validation_columns = validation.get("columns", [name])
            missing_columns = [column for column in validation_columns if column not in df.columns]
            if missing_columns:
                raise ValueError(
                    f"Validation '{check}' for column '{name}' references missing "
                    f"column(s) in table config '{table}': {', '.join(missing_columns)}"
                )
            condition = F.coalesce(
                VALIDATIONS[check](*[F.col(column) for column in validation_columns]),
                F.lit(False),
            )
            checks[reason] = condition
            for column_name in validation.get("null_columns", validation_columns):
                if column_name in df.columns:
                    column_conditions.setdefault(column_name, []).append(condition)
    return checks, column_conditions


def _null_failing_values(df: DataFrame, column_conditions: dict[str, list[Column]]) -> DataFrame:
    """Null out values that failed a check, keeping the row.

    Args:
        df: DataFrame already flagged by `apply_quarantine`.
        column_conditions: Maps a column to the conditions that must
            all pass for its value to be kept.

    Returns:
        The DataFrame with every failing value nulled out.
    """
    for name, conditions in column_conditions.items():
        passed = conditions[0]
        for condition in conditions[1:]:
            passed = passed & condition
        df = df.withColumn(name, F.when(passed, F.col(name)))
    return df


def apply_unique_quarantine(df: DataFrame, config: dict) -> DataFrame:
    """Flag duplicate rows on a ``unique`` column, keeping the first one.

    Uniqueness is a windowed, multi-row check, so unlike the per-value
    validations it is computed once into a helper column and reused: the
    reason is appended to ``dq_observations``, ``quality_ok`` recomputed
    and the value nulled, all from that single column so the flag and the
    null always land on the same physical row (two separate window passes
    could disagree on which row is the first). Duplicates are then
    discarded like any rejected row, which counts them in the
    data-quality alert instead of dropping them silently.

    Two details make it safe on a non-key column (e.g. a duplicate
    invoice number, not just the primary key):

    * Null is not a duplicate of null (SQL ``UNIQUE`` semantics), so a
      missing value never flags — it is already caught by ``required``.
    * The kept row is the one first by primary key, a deterministic
      tie-break, so which duplicate is flagged never varies between runs.

    Args:
        df: DataFrame already flagged by `apply_quarantine`
            (`dq_observations`/`quality_ok` present).
        config: Parsed table config; a ``unique`` validation names the
            columns that must be distinct.

    Returns:
        The DataFrame with duplicate rows flagged and their value nulled.
    """
    primary_key = [
        entry["name"]
        for entry in config["schema"]
        if entry.get("key") and not entry.get("new_name")
    ]
    for entry in config["schema"]:
        if entry.get("new_name"):
            continue
        for validation in entry.get("validations", []):
            if validation["check"] != UNIQUE_CHECK:
                continue
            unique_columns = validation.get("columns", [entry["name"]])
            if any(column not in df.columns for column in unique_columns):
                continue
            ordering = Window.partitionBy(*unique_columns).orderBy(
                *(primary_key or unique_columns)
            )
            has_value = None
            for column in unique_columns:
                present = F.col(column).isNotNull()
                has_value = present if has_value is None else has_value & present
            is_duplicate = (F.row_number().over(ordering) > 1) & has_value
            df = df.withColumn("is_duplicate", is_duplicate)
            df = df.withColumn(
                "dq_observations",
                F.concat_ws(
                    ";",
                    F.when(F.length("dq_observations") > 0, F.col("dq_observations")),
                    F.when(F.col("is_duplicate"), F.lit(validation["reason"])),
                ),
            )
            df = df.withColumn("quality_ok", F.length("dq_observations") == 0)
            for column in unique_columns:
                df = df.withColumn(column, F.when(~F.col("is_duplicate"), F.col(column)))
            df = df.drop("is_duplicate")
    return df


def apply_table_validations(df: DataFrame, config: dict) -> DataFrame:
    """Run the validations declared in the table config (quarantine).

    Each ``schema`` entry may declare ``validations``: a list of
    ``{"check", "reason"}`` pairs, where ``check`` is a key of
    ``VALIDATIONS`` and ``reason`` is the label recorded in
    `dq_observations`. Every check is wrapped null-safely (a null
    boolean counts as invalid). Failing rows are flagged, never
    dropped, and the failing value is nulled out - the row keeps its
    primary key so joins still work. The ``unique`` check is special
    (windowed): `apply_unique_quarantine` flags duplicate keys after
    the per-value checks so they, too, reach the data-quality alert.

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
    checks, column_conditions = _resolve_checks(df, config)
    df = apply_quarantine(df, checks)
    df = _null_failing_values(df, column_conditions)
    return apply_unique_quarantine(df, config)


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
