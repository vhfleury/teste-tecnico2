"""Dedicated tests for the aggregated trip metric tables."""
import json
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.metricas_viagens.metricas_viagens import METRIC_TABLES, build_metric
from tests.pipelines.diff import assert_matches_expected, dataframe_to_rows

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ANALYTICS_ROOT = os.path.join(ROOT, "analytics", "metricas_viagens")

# Runtime metadata: the column must exist in the output (config is the
# contract), but its value is generated at execution time, so it is
# masked to null on both sides of the comparison.
RUNTIME_COLUMNS = ("processed_at",)

# Shape of the inputs as the metric builders consume them: the typed
# upstream columns (timestamps read as string from the JSON fixture
# and cast below), not the full upstream contracts.
TRIPS_SCHEMA = (
    "viagem_id string, veiculo_id string, motorista_id string, motorista_nome string, "
    "geocerca_origem_id string, geocerca_destino_id string, origem_nome string, "
    "destino_nome string, status string, duracao_horas double, atrasada_flag boolean, "
    "mes string, dq_observations string"
)
GEOFENCE_POSITIONS_SCHEMA = (
    "posicao_id string, viagem_id string, veiculo_id string, timestamp string, "
    "classificacao_localizacao string, evento_entrada_geocerca boolean, geocerca_tipo string"
)
VEHICLES_SCHEMA = "veiculo_id string, status string"


def load_fixture(file_name: str) -> dict:
    with open(os.path.join(ANALYTICS_ROOT, file_name), encoding="utf-8") as file:
        return json.load(file)


def build_frames(spark: SparkSession) -> dict:
    """Load the input fixture as the frames the builders consume.

    Args:
        spark: Active SparkSession.

    Returns:
        Input DataFrames by name (`trips`, `vehicles`, `geofence_positions`).
    """
    fixture = load_fixture("input_metricas_viagens.json")
    trips = spark.createDataFrame(fixture["analytics_viagens_enriquecidas"], TRIPS_SCHEMA)
    geofence_positions = spark.createDataFrame(
        fixture["analytics_posicoes_geocercas"], GEOFENCE_POSITIONS_SCHEMA
    ).withColumn("timestamp", F.to_timestamp("timestamp"))
    vehicles = spark.createDataFrame(fixture["staging_veiculos"], VEHICLES_SCHEMA)
    return {
        "trips": trips,
        "vehicles": vehicles,
        "geofence_positions": geofence_positions,
    }


def build_result_rows(spark: SparkSession, table_name: str) -> list[dict]:
    """Run one metric build over the input fixture.

    Args:
        spark: Active SparkSession.
        table_name: Metric dataset name (a `METRIC_TABLES` key).

    Returns:
        The metric rows as JSON-friendly dicts with the runtime columns masked.
    """
    return dataframe_to_rows(
        build_metric(table_name, build_frames(spark)), mask=RUNTIME_COLUMNS
    )


@pytest.mark.parametrize("table_name", sorted(METRIC_TABLES))
def test_metric_matches_golden(spark, table_name):
    result = build_result_rows(spark, table_name)

    expected = load_fixture("output_metricas_viagens.json")[f"analytics_{table_name}"]
    assert_matches_expected(result, expected, METRIC_TABLES[table_name]["key_columns"])
