"""Dedicated tests for the viagens_enriquecidas analytics table.

Analytics has exclusive treatment (joins and business rules live in
code, not in the config), so it is not covered by the generic staging
golden test. The golden test runs the full `build_analytics` chain
over the input fixture — one key per staging table consumed — and
compares the result with the frozen expected output, keyed by
`viagem_id`. The fixture covers every trip status, an implicit delay
(actual end after the planned end on a `concluida` trip), one orphan
per dimension, a trip without positions, staging-inherited defects and
trips spread across three months.
"""
import json
import os

from general.utils import load_table_config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.viagens_enriquecidas import build_analytics
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
TRIPS_SCHEMA = (
    "viagem_id string, veiculo_id string, motorista_id string, geocerca_origem_id string, "
    "geocerca_destino_id string, data_inicio string, data_fim_prevista string, "
    "data_fim_real string, status string, distancia_km int, peso_carga_kg int, "
    "nota_fiscal string, source_file string, ingested_at string, dq_observations string, "
    "quality_ok boolean"
)
VEHICLES_SCHEMA = (
    "veiculo_id string, placa string, marca string, modelo string, tipo string, "
    "capacidade_kg int, status string"
)
DRIVERS_SCHEMA = (
    "motorista_id string, nome string, categoria_cnh string, base_operacional string, "
    "status string"
)
GEOFENCES_SCHEMA = "geocerca_id string, nome string, tipo string, uf string"
POSITIONS_SCHEMA = "posicao_id string, viagem_id string, velocidade_kmh int, timestamp string"

TRIPS_TIMESTAMP_COLUMNS = ("data_inicio", "data_fim_prevista", "data_fim_real", "ingested_at")


def load_fixture(file_name: str) -> dict:
    with open(os.path.join(ANALYTICS_ROOT, file_name), encoding="utf-8") as file:
        return json.load(file)


def build_result_rows(spark: SparkSession) -> list[dict]:
    """Run the full analytics chain over the input fixture.

    Shared by the golden test and by the manual golden regeneration,
    so the frozen output is always produced by the exact code path
    the test exercises.

    Args:
        spark: Active SparkSession.

    Returns:
        The analytics rows as JSON-friendly dicts with the runtime
        columns masked.
    """
    fixture = load_fixture("input_viagens_enriquecidas.json")
    trips = spark.createDataFrame(fixture["staging_viagens"], TRIPS_SCHEMA)
    for column in TRIPS_TIMESTAMP_COLUMNS:
        trips = trips.withColumn(column, F.to_timestamp(column))
    vehicles = spark.createDataFrame(fixture["staging_veiculos"], VEHICLES_SCHEMA)
    drivers = spark.createDataFrame(fixture["staging_motoristas"], DRIVERS_SCHEMA)
    geofences = spark.createDataFrame(fixture["staging_geocercas"], GEOFENCES_SCHEMA)
    positions = spark.createDataFrame(
        fixture["staging_posicoes"], POSITIONS_SCHEMA
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    config = load_table_config(
        os.path.join(ANALYTICS_ROOT, "analytics_viagens_enriquecidas.json")
    )
    df = build_analytics(trips, vehicles, drivers, geofences, positions, config)
    return dataframe_to_rows(df, mask=RUNTIME_COLUMNS)


def test_build_analytics_matches_golden(spark):
    result = build_result_rows(spark)

    expected = load_fixture("output_analytics_viagens_enriquecidas.json")[
        "analytics_viagens_enriquecidas"
    ]
    assert_matches_expected(result, expected, ["viagem_id"])
