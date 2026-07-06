"""Dedicated tests for the posicoes_geocercas analytics table."""
import json
import os

from general.utils import load_table_config
from pyspark.sql import functions as F

from analytics.posicoes_geocercas.posicoes_geocercas import (
    build_analytics,
    match_positions_to_geofences,
)
from tests.pipelines.diff import assert_matches_expected, dataframe_to_rows

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ANALYTICS_ROOT = os.path.join(ROOT, "analytics", "posicoes_geocercas")

# Runtime metadata: the column must exist in the output (config is the
# contract), but its value is generated at execution time, so it is
# masked to null on both sides of the comparison.
RUNTIME_COLUMNS = ("processed_at",)

# Shape of the staging inputs as this analytics consumes them: the
# typed staging output columns (timestamps read as string from the
# JSON fixture and cast below), not raw all-string leaves.
POSITIONS_SCHEMA = (
    "posicao_id string, viagem_id string, veiculo_id string, timestamp string, "
    "latitude double, longitude double, velocidade_kmh int, source_file string, "
    "ingested_at string, dq_observations string, quality_ok boolean"
)
GEOFENCES_SCHEMA = (
    "geocerca_id string, nome string, tipo string, uf string, raio_km double, "
    "ativo boolean, geometry string, source_file string, ingested_at string, "
    "dq_observations string, quality_ok boolean"
)


def load_fixture(file_name: str) -> dict:
    with open(os.path.join(ANALYTICS_ROOT, file_name), encoding="utf-8") as file:
        return json.load(file)


def _position_row(posicao_id: str, latitude: float, longitude: float) -> dict:
    return {
        "posicao_id": posicao_id,
        "viagem_id": "VIA-001",
        "veiculo_id": "VEI-001",
        "timestamp": "2026-04-28 10:00:00",
        "latitude": latitude,
        "longitude": longitude,
        "velocidade_kmh": 10,
        "source_file": "test",
        "ingested_at": "2026-07-04 12:00:00",
        "dq_observations": "",
        "quality_ok": True,
    }


def test_spatial_match_handles_inside_outside_and_boundary_points(spark):
    geometry = (
        '{"type":"Polygon","coordinates":[[[-46.65,-23.56],[-46.63,-23.56],'
        '[-46.63,-23.54],[-46.65,-23.54],[-46.65,-23.56]]]}'
    )
    geofences = spark.createDataFrame(
        [
            {
                "geocerca_id": "GEO-A",
                "nome": "Terminal A",
                "tipo": "CENTRO_DISTRIBUICAO",
                "uf": "SP",
                "raio_km": 1.0,
                "ativo": True,
                "geometry": geometry,
                "source_file": "test",
                "ingested_at": "2026-07-04 12:00:00",
                "dq_observations": "",
                "quality_ok": True,
            }
        ],
        GEOFENCES_SCHEMA,
    )
    positions = spark.createDataFrame(
        [
            _position_row("POS-INSIDE", -23.55, -46.64),
            _position_row("POS-OUTSIDE", -23.57, -46.62),
            # On the southern edge: boundary points count as inside.
            _position_row("POS-BOUNDARY", -23.56, -46.64),
        ],
        POSITIONS_SCHEMA,
    )

    matched = {
        row["posicao_id"]: row["geocerca_id"]
        for row in match_positions_to_geofences(positions, geofences).collect()
    }

    assert matched == {
        "POS-INSIDE": "GEO-A",
        "POS-OUTSIDE": None,
        "POS-BOUNDARY": "GEO-A",
    }


def test_build_analytics_matches_golden(spark):
    fixture = load_fixture("input_posicoes_geocercas.json")
    positions = spark.createDataFrame(fixture["staging_posicoes"], POSITIONS_SCHEMA)
    geofences = spark.createDataFrame(fixture["staging_geocercas"], GEOFENCES_SCHEMA)
    positions = positions.withColumn("timestamp", F.to_timestamp("timestamp")).withColumn(
        "ingested_at",
        F.to_timestamp("ingested_at"),
    )
    geofences = geofences.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    config = load_table_config(os.path.join(ANALYTICS_ROOT, "analytics_posicoes_geocercas.json"))
    result = dataframe_to_rows(build_analytics(positions, geofences, config), mask=RUNTIME_COLUMNS)

    expected = load_fixture("output_analytics_posicoes_geocercas.json")[
        "analytics_posicoes_geocercas"
    ]
    assert_matches_expected(result, expected, ["posicao_id"])
