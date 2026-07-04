"""Unit tests for the shared validation helpers."""
import pytest
from data_quality.validation import (
    apply_quarantine,
    apply_table_validations,
    enforce_table_config,
)
from pyspark.sql import functions as F

TABLE_CONFIG = {
    "table_name": "staging_example",
    "partitioned_by": ["ingest_date"],
    "schema": [
        {"name": "id", "type": "string", "example": "ID-1"},
        {"name": "amount", "type": "bigint", "example": 10},
        {"name": "ingest_date", "type": "date", "example": "2024-01-01"},
    ],
}


def test_enforce_table_config_orders_and_drops_undeclared_columns(spark):
    df = spark.createDataFrame([(10, "ID-1", "extra")], ["amount", "id", "undeclared"])

    result = enforce_table_config(df, TABLE_CONFIG)

    # Partition column is not required; declared columns come in config order.
    assert result.columns == ["id", "amount"]


def test_enforce_table_config_fails_on_missing_column(spark):
    df = spark.createDataFrame([("ID-1",)], ["id"])

    with pytest.raises(ValueError, match="missing columns: amount"):
        enforce_table_config(df, TABLE_CONFIG)


def test_enforce_table_config_fails_on_type_mismatch(spark):
    df = spark.createDataFrame([("ID-1", "10")], ["id", "amount"])

    with pytest.raises(ValueError, match="amount \\(expected bigint, got string\\)"):
        enforce_table_config(df, TABLE_CONFIG)


def test_enforce_table_config_expects_renamed_column(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "cpf", "type": "string"},
            {
                "name": "cpf",
                "type": "string",
                "treatment": "normalize_cpf",
                "new_name": "cpf_normalize",
            },
        ],
    }
    df = spark.createDataFrame([("111.111.111-11", "11111111111")], ["cpf", "cpf_normalize"])

    result = enforce_table_config(df, config)

    assert result.columns == ["cpf", "cpf_normalize"]


def test_apply_table_validations_flags_and_nulls_declared_reasons(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {"name": "motorista_id", "type": "string"},
            {
                "name": "nome",
                "type": "string",
                "validations": [{"check": "required", "reason": "missing_name"}],
            },
            {
                "name": "cpf",
                "type": "string",
                "validations": [{"check": "cpf_is_valid", "reason": "invalid_cpf"}],
            },
        ],
    }
    df = spark.createDataFrame(
        [
            ("MOT-0001", "ANA SOUZA", "529.982.247-25"),
            ("MOT-0002", "", "111.111.111-11"),
        ],
        ["motorista_id", "nome", "cpf"],
    )

    result = apply_table_validations(df, config).collect()

    # Row 2 keeps its key, both failing values are nulled, reasons recorded.
    assert [
        (row["motorista_id"], row["nome"], row["cpf"], row["dq_observations"], row["quality_ok"])
        for row in result
    ] == [
        ("MOT-0001", "ANA SOUZA", "529.982.247-25", "", True),
        ("MOT-0002", None, None, "missing_name;invalid_cpf", False),
    ]


def test_apply_table_validations_treats_null_check_result_as_invalid(spark):
    # A null boolean (e.g. isin over a null value) counts as invalid here,
    # unlike raw apply_quarantine - the engine closes that loophole.
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "status",
                "type": "string",
                "validations": [{"check": "driver_status_is_valid", "reason": "invalid_status"}],
            },
        ],
    }
    df = spark.createDataFrame([("MOT-0001", None)], "motorista_id string, status string")

    result = apply_table_validations(df, config).collect()

    assert [(row["dq_observations"], row["quality_ok"]) for row in result] == [
        ("invalid_status", False)
    ]


def test_apply_table_validations_plate_check_accepts_both_formats(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "placa",
                "type": "string",
                "validations": [{"check": "plate_is_valid", "reason": "invalid_plate"}],
            },
        ],
    }
    df = spark.createDataFrame(
        [
            ("VEI-0001", "ABC1234"),
            ("VEI-0002", "ABC1D23"),
            ("VEI-0003", "INVALIDA"),
            ("VEI-0004", None),
        ],
        ["veiculo_id", "placa"],
    )

    result = apply_table_validations(df, config).collect()

    # Old and Mercosul formats pass; a malformed or null plate is flagged.
    assert [
        (row["veiculo_id"], row["placa"], row["dq_observations"], row["quality_ok"])
        for row in result
    ] == [
        ("VEI-0001", "ABC1234", "", True),
        ("VEI-0002", "ABC1D23", "", True),
        ("VEI-0003", None, "invalid_plate", False),
        ("VEI-0004", None, "invalid_plate", False),
    ]


def test_apply_table_validations_non_negative_flags_negative_values(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "km_atual",
                "type": "int",
                "validations": [{"check": "non_negative", "reason": "negative_mileage"}],
            },
        ],
    }
    df = spark.createDataFrame(
        [("VEI-0001", 768660), ("VEI-0002", 0), ("VEI-0003", -201014)],
        "veiculo_id string, km_atual int",
    )

    result = apply_table_validations(df, config).collect()

    assert [
        (row["veiculo_id"], row["km_atual"], row["dq_observations"], row["quality_ok"])
        for row in result
    ] == [
        ("VEI-0001", 768660, "", True),
        ("VEI-0002", 0, "", True),
        ("VEI-0003", None, "negative_mileage", False),
    ]


def test_apply_table_validations_positive_flags_zero_negative_and_null(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "raio_km",
                "type": "double",
                "validations": [{"check": "positive", "reason": "invalid_radius"}],
            },
        ],
    }
    df = spark.createDataFrame(
        [("GEO-0001", 1.0), ("GEO-0002", 0.0), ("GEO-0003", -0.5), ("GEO-0004", None)],
        "geocerca_id string, raio_km double",
    )

    result = apply_table_validations(df, config).collect()

    # Unlike non_negative, zero is not a meaningful radius and is flagged.
    assert [
        (row["geocerca_id"], row["raio_km"], row["dq_observations"], row["quality_ok"])
        for row in result
    ] == [
        ("GEO-0001", 1.0, "", True),
        ("GEO-0002", None, "invalid_radius", False),
        ("GEO-0003", None, "invalid_radius", False),
        ("GEO-0004", None, "invalid_radius", False),
    ]


def test_apply_table_validations_coordinates_in_brazil_check_nulls_pair(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "latitude",
                "type": "double",
                "validations": [
                    {"check": "non_zero", "reason": "zeroed_latitude"},
                    {
                        "check": "coordinates_in_brazil",
                        "reason": "coordinates_outside_brazil",
                        "columns": ["latitude", "longitude"],
                    },
                ],
            },
            {
                "name": "longitude",
                "type": "double",
                "validations": [{"check": "non_zero", "reason": "zeroed_longitude"}],
            },
        ],
    }
    df = spark.createDataFrame(
        [
            ("POS-1", -23.55, -46.63),
            ("POS-2", 40.7128, -74.0060),
            ("POS-3", 0.0, 0.0),
        ],
        "posicao_id string, latitude double, longitude double",
    )

    result = apply_table_validations(df, config).collect()

    assert [
        (
            row["posicao_id"],
            row["latitude"],
            row["longitude"],
            row["dq_observations"],
            row["quality_ok"],
        )
        for row in result
    ] == [
        ("POS-1", -23.55, -46.63, "", True),
        ("POS-2", None, None, "coordinates_outside_brazil", False),
        ("POS-3", None, None, "zeroed_latitude;zeroed_longitude", False),
    ]


def test_apply_table_validations_geofence_type_check(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "tipo",
                "type": "string",
                "validations": [
                    {"check": "geofence_type_is_valid", "reason": "invalid_geofence_type"}
                ],
            },
        ],
    }
    df = spark.createDataFrame(
        [
            ("GEO-0001", "centro_distribuicao"),
            ("GEO-0002", "pedagio"),
            ("GEO-0003", "posto_combustivel"),
            ("GEO-0004", "cliente"),
            ("GEO-0005", "garagem"),
        ],
        ["geocerca_id", "tipo"],
    )

    result = apply_table_validations(df, config).collect()

    assert [
        (row["geocerca_id"], row["tipo"], row["quality_ok"]) for row in result
    ] == [
        ("GEO-0001", "centro_distribuicao", True),
        ("GEO-0002", "pedagio", True),
        ("GEO-0003", "posto_combustivel", True),
        ("GEO-0004", "cliente", True),
        ("GEO-0005", None, False),
    ]


def test_apply_table_validations_trip_status_check(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "status",
                "type": "string",
                "validations": [
                    {"check": "trip_status_is_valid", "reason": "invalid_status"}
                ],
            },
        ],
    }
    df = spark.createDataFrame(
        [
            ("VIA-000001", "em_transito"),
            ("VIA-000002", "concluida"),
            ("VIA-000003", "cancelada"),
            ("VIA-000004", "atrasada"),
            ("VIA-000005", "planejada"),
            ("VIA-000006", None),
        ],
        "viagem_id string, status string",
    )

    result = apply_table_validations(df, config).collect()

    # The four known statuses pass; unknown or null statuses are flagged.
    assert [
        (row["viagem_id"], row["status"], row["quality_ok"]) for row in result
    ] == [
        ("VIA-000001", "em_transito", True),
        ("VIA-000002", "concluida", True),
        ("VIA-000003", "cancelada", True),
        ("VIA-000004", "atrasada", True),
        ("VIA-000005", None, False),
        ("VIA-000006", None, False),
    ]


def test_apply_table_validations_geojson_polygon_check(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "geometry",
                "type": "string",
                "validations": [
                    {"check": "geojson_polygon_is_valid", "reason": "invalid_geometry"}
                ],
            },
        ],
    }
    valid = '{"type":"Polygon","coordinates":[[[-1.0,-2.0],[1.0,-2.0],[1.0,2.0],[-1.0,-2.0]]]}'
    zeroed = '{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,0.0],[0.0,0.0],[0.0,0.0]]]}'
    short_ring = '{"type":"Polygon","coordinates":[[[-1.0,-2.0],[1.0,-2.0],[-1.0,-2.0]]]}'
    not_polygon = '{"type":"Point","coordinates":[[[-1.0,-2.0]]]}'
    df = spark.createDataFrame(
        [
            ("GEO-0001", valid),
            ("GEO-0002", zeroed),
            ("GEO-0003", short_ring),
            ("GEO-0004", not_polygon),
            ("GEO-0005", "not a geojson"),
            ("GEO-0006", None),
        ],
        "geocerca_id string, geometry string",
    )

    result = apply_table_validations(df, config).collect()

    # Null passes (presence is `required`'s job under its own reason);
    # zeroed, degenerate, non-Polygon and unparseable geometries fail.
    assert [
        (row["geocerca_id"], row["geometry"], row["quality_ok"]) for row in result
    ] == [
        ("GEO-0001", valid, True),
        ("GEO-0002", None, False),
        ("GEO-0003", None, False),
        ("GEO-0004", None, False),
        ("GEO-0005", None, False),
        ("GEO-0006", None, True),
    ]


def test_apply_table_validations_geojson_polygon_brazil_bounds_check(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "geometry",
                "type": "string",
                "validations": [
                    {"check": "geojson_polygon_is_valid", "reason": "invalid_geometry"},
                    {
                        "check": "geojson_polygon_is_in_brazil",
                        "reason": "geometry_outside_brazil",
                    },
                ],
            },
        ],
    }
    inside_brazil = (
        '{"type":"Polygon","coordinates":[[[-46.65,-23.56],[-46.63,-23.56],'
        '[-46.63,-23.54],[-46.65,-23.56]]]}'
    )
    outside_brazil = (
        '{"type":"Polygon","coordinates":[[[-74.1,40.7],[-74.0,40.7],'
        '[-74.0,40.8],[-74.1,40.7]]]}'
    )
    zeroed = '{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,0.0],[0.0,0.0],[0.0,0.0]]]}'
    df = spark.createDataFrame(
        [
            ("GEO-0001", inside_brazil),
            ("GEO-0002", outside_brazil),
            ("GEO-0003", zeroed),
            ("GEO-0004", "not a geojson"),
        ],
        "geocerca_id string, geometry string",
    )

    result = apply_table_validations(df, config).collect()

    assert [
        (row["geocerca_id"], row["geometry"], row["dq_observations"], row["quality_ok"])
        for row in result
    ] == [
        ("GEO-0001", inside_brazil, "", True),
        ("GEO-0002", None, "geometry_outside_brazil", False),
        ("GEO-0003", None, "invalid_geometry", False),
        ("GEO-0004", None, "invalid_geometry", False),
    ]


def test_apply_table_validations_fails_on_unknown_check(spark):
    config = {
        "table_name": "staging_example",
        "schema": [
            {
                "name": "nome",
                "type": "string",
                "validations": [{"check": "does_not_exist", "reason": "missing_name"}],
            },
        ],
    }
    df = spark.createDataFrame([("Ana Souza",)], ["nome"])

    with pytest.raises(ValueError, match="Unknown validation 'does_not_exist'"):
        apply_table_validations(df, config)


def test_apply_quarantine_flags_failing_rows_without_dropping_them(spark):
    df = spark.createDataFrame(
        [
            ("MOT-0001", "ANA SOUZA", "ativo"),
            ("MOT-0002", None, "ativo"),
            ("MOT-0003", None, "desconhecido"),
        ],
        ["motorista_id", "nome", "status"],
    )
    checks = {
        "missing_name": F.col("nome").isNotNull(),
        "invalid_status": F.col("status").isin(["ativo", "ferias"]),
    }

    result = apply_quarantine(df, checks).collect()

    # Every row survives; reasons accumulate in declaration order, ";"-separated.
    assert [
        (row["motorista_id"], row["dq_observations"], row["quality_ok"]) for row in result
    ] == [
        ("MOT-0001", "", True),
        ("MOT-0002", "missing_name", False),
        ("MOT-0003", "missing_name;invalid_status", False),
    ]


def test_apply_quarantine_null_check_result_passes_silently(spark):
    # Contract: checks must be null-safe (e.g. include isNotNull), because a
    # null boolean is neither True nor False and records no observation.
    df = spark.createDataFrame([("MOT-0001", None)], "motorista_id string, status string")
    checks = {"invalid_status": F.col("status").isin(["ativo"])}

    result = apply_quarantine(df, checks).collect()

    assert [(row["dq_observations"], row["quality_ok"]) for row in result] == [("", True)]


def test_apply_quarantine_keeps_existing_columns_intact(spark):
    df = spark.createDataFrame([("MOT-0001", "529.982.247-25")], ["motorista_id", "cpf"])
    checks = {"invalid_cpf": F.lit(False)}

    result = apply_quarantine(df, checks).collect()

    # Flagging never mutates the offending value - that decision belongs
    # to the pipeline (quarantine nulls it out explicitly when needed).
    assert [(row["motorista_id"], row["cpf"], row["quality_ok"]) for row in result] == [
        ("MOT-0001", "529.982.247-25", False)
    ]
