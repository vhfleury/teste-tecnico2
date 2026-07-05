"""Dedicated tests for the posicoes_geocercas analytics table.

Analytics has exclusive treatment (joins and business rules live in
code, not in the config), so it is not covered by the generic staging
golden test. Two tests: a unit check of the point-in-polygon
primitive (inside, outside and boundary points) and a golden test
that runs the full `build_analytics` chain over the input fixture —
one key per staging table consumed — and compares the result with the
frozen expected output, keyed by `posicao_id`.
"""
import json
import os

from analytics.posicoes_geocercas import (
    _point_is_inside_geojson_polygon,
    build_analytics,
)
from general.utils import load_table_config
from pyspark.sql import functions as F

from tests.pipelines.diff import assert_matches_expected, dataframe_to_rows

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ANALYTICS_ROOT = os.path.join(ROOT, "analytics")

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


def test_point_in_polygon_handles_inside_outside_and_boundary_points():
    geometry = (
        '{"type":"Polygon","coordinates":[[[-46.65,-23.56],[-46.63,-23.56],'
        '[-46.63,-23.54],[-46.65,-23.54],[-46.65,-23.56]]]}'
    )

    assert _point_is_inside_geojson_polygon(-23.55, -46.64, geometry)
    assert not _point_is_inside_geojson_polygon(-23.57, -46.62, geometry)
    # On the southern edge: boundary points count as inside.
    assert _point_is_inside_geojson_polygon(-23.56, -46.64, geometry)


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
