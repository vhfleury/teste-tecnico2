"""metricas_viagens analytics: aggregated trip metric tables."""
from __future__ import annotations

import logging
import os

from data_quality.validation import enforce_table_config
from general.delta_io import (
    mark_delta_partition_processed,
    read_delta_partition,
    write_delta_partition,
)
from general.utils import (
    layer_dir,
    load_table_config,
)
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

# Gold and staging inputs consumed by the metric tables.
ANALYTICS_VIAGENS_ENRIQUECIDAS_DIR = layer_dir("analytics", "viagens_enriquecidas")
ANALYTICS_POSICOES_GEOCERCAS_DIR = layer_dir("analytics", "posicoes_geocercas")
STAGING_VEICULOS_DIR = layer_dir("staging", "veiculos")

CONFIG_ROOT = os.path.dirname(__file__)

TOP_DRIVERS_LIMIT = 10
SECONDS_PER_MINUTE = 60
RATE_PRECISION = 4
DURATION_PRECISION = 2


def build_trips_by_month_status(trips: DataFrame) -> DataFrame:
    """Count trips per month and per status.

    Args:
        trips: Enriched trips DataFrame.

    Returns:
        One row per (mes, status) with the trip count.
    """
    return (
        trips.filter(F.col("mes").isNotNull())
        .groupBy("mes", "status")
        .agg(F.count(F.lit(1)).alias("total_viagens"))
    )


def build_avg_route_duration(trips: DataFrame) -> DataFrame:
    """Average the actual trip duration per origin -> destination route.

    Args:
        trips: Enriched trips DataFrame.

    Returns:
        One row per route with the geofence names, average duration
        and trip count.
    """
    return (
        trips.filter(
            F.col("geocerca_origem_id").isNotNull() & F.col("geocerca_destino_id").isNotNull()
        )
        .groupBy("geocerca_origem_id", "geocerca_destino_id")
        .agg(
            F.max("origem_nome").alias("origem_nome"),
            F.max("destino_nome").alias("destino_nome"),
            F.round(F.avg("duracao_horas"), DURATION_PRECISION).alias("tempo_medio_horas"),
            F.count(F.lit(1)).alias("total_viagens"),
        )
    )


def build_monthly_delay_rate(trips: DataFrame) -> DataFrame:
    """Compute the monthly delay rate (delayed trips / total trips).

    Args:
        trips: Enriched trips DataFrame.

    Returns:
        One row per month with the delayed count, total count and rate.
    """
    return (
        trips.filter(F.col("mes").isNotNull())
        .groupBy("mes")
        .agg(
            F.count_if(F.col("atrasada_flag")).alias("viagens_atrasadas"),
            F.count(F.lit(1)).alias("viagens_total"),
        )
        .withColumn(
            "taxa_atraso",
            F.round(F.col("viagens_atrasadas") / F.col("viagens_total"), RATE_PRECISION),
        )
    )


def build_top_drivers(trips: DataFrame) -> DataFrame:
    """Rank the top drivers by completed trips.

    Args:
        trips: Enriched trips DataFrame.

    Returns:
        At most ``TOP_DRIVERS_LIMIT`` rows, ranked by completed trips.
    """
    completed = trips.filter(
        (F.col("status") == F.lit("CONCLUIDA")) & F.col("motorista_id").isNotNull()
    )
    ranked = (
        completed.groupBy("motorista_id")
        .agg(
            F.max("motorista_nome").alias("motorista_nome"),
            F.count(F.lit(1)).alias("viagens_concluidas"),
        )
        .withColumn(
            "posicao_rank",
            F.row_number().over(
                Window.orderBy(F.col("viagens_concluidas").desc(), F.col("motorista_id"))
            ),
        )
    )
    return ranked.filter(F.col("posicao_rank") <= TOP_DRIVERS_LIMIT).select(
        "posicao_rank", "motorista_id", "motorista_nome", "viagens_concluidas"
    )


def build_monthly_fleet_utilization(trips: DataFrame, vehicles: DataFrame) -> DataFrame:
    """Compute the monthly active-fleet utilization rate.

    Args:
        trips: Enriched trips DataFrame.
        vehicles: Staging vehicles DataFrame.

    Returns:
        One row per month with the counts and the utilization rate.
    """
    active_vehicles = vehicles.filter(F.col("status") == F.lit("ATIVO")).select("veiculo_id")
    monthly_trips = trips.filter(F.col("mes").isNotNull())

    months = monthly_trips.select("mes").distinct()
    vehicles_per_month = (
        monthly_trips.join(active_vehicles, "veiculo_id", "semi")
        .groupBy("mes")
        .agg(F.count_distinct("veiculo_id").alias("veiculos_com_viagem"))
    )
    fleet_total = active_vehicles.agg(
        F.count(F.lit(1)).alias("veiculos_ativos_total")
    )
    return (
        months.join(vehicles_per_month, "mes", "left")
        .withColumn(
            "veiculos_com_viagem",
            F.coalesce(F.col("veiculos_com_viagem"), F.lit(0).cast("bigint")),
        )
        .crossJoin(fleet_total)
        .withColumn(
            "taxa_utilizacao",
            F.round(
                F.col("veiculos_com_viagem") / F.col("veiculos_ativos_total"), RATE_PRECISION
            ),
        )
    )


def build_geofence_dwell_time(geofence_positions: DataFrame) -> DataFrame:
    """Average the dwell time inside geofences per geofence type.

    Args:
        geofence_positions: analytics_posicoes_geocercas DataFrame.

    Returns:
        One row per geofence type with the visit count and average
        dwell minutes.
    """
    trip_window = (
        Window.partitionBy("viagem_id", "veiculo_id")
        .orderBy(F.col("timestamp").asc_nulls_last(), F.col("posicao_id"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    visits = (
        geofence_positions.filter(F.col("classificacao_localizacao") == F.lit("em_geocerca"))
        .withColumn(
            "visit_id",
            F.sum(F.col("evento_entrada_geocerca").cast("int")).over(trip_window),
        )
        .groupBy("viagem_id", "veiculo_id", "visit_id")
        .agg(
            F.max("geocerca_tipo").alias("geocerca_tipo"),
            (
                (F.max(F.col("timestamp").cast("double"))
                 - F.min(F.col("timestamp").cast("double")))
                / SECONDS_PER_MINUTE
            ).alias("dwell_minutes"),
        )
    )
    return visits.groupBy("geocerca_tipo").agg(
        F.count(F.lit(1)).alias("total_visitas"),
        F.round(F.avg("dwell_minutes"), DURATION_PRECISION).alias(
            "tempo_medio_parado_minutos"
        ),
    )


# Registry driving the transform, the golden tests and the coverage
# guard. Keys are the analytics dataset names; `inputs` names the
# frames each builder consumes; `key_columns` is the table's grain,
# used by the golden diff.
METRIC_TABLES = {
    "viagens_por_mes_status": {
        "builder": build_trips_by_month_status,
        "inputs": ("trips",),
        "key_columns": ["mes", "status"],
    },
    "tempo_medio_por_rota": {
        "builder": build_avg_route_duration,
        "inputs": ("trips",),
        "key_columns": ["geocerca_origem_id", "geocerca_destino_id"],
    },
    "taxa_atraso_mensal": {
        "builder": build_monthly_delay_rate,
        "inputs": ("trips",),
        "key_columns": ["mes"],
    },
    "top_motoristas": {
        "builder": build_top_drivers,
        "inputs": ("trips",),
        "key_columns": ["motorista_id"],
    },
    "utilizacao_frota_mensal": {
        "builder": build_monthly_fleet_utilization,
        "inputs": ("trips", "vehicles"),
        "key_columns": ["mes"],
    },
    "tempo_parado_geocercas": {
        "builder": build_geofence_dwell_time,
        "inputs": ("geofence_positions",),
        "key_columns": ["geocerca_tipo"],
    },
}


def table_config_path(name: str) -> str:
    """Build the config path of a metric table.

    Args:
        name: Metric dataset name (a `METRIC_TABLES` key).

    Returns:
        Path to the table's config JSON.
    """
    return os.path.join(CONFIG_ROOT, f"analytics_{name}.json")


# A trip counts for the metrics only when its vehicle and driver both resolved
# in the join. Geofence orphans do not make a trip invalid.
TRIP_INVALIDATING_REASONS = ("vehicle_not_found", "driver_not_found")


def valid_trips(trips: DataFrame) -> DataFrame:
    """Keep only trips whose vehicle and driver resolved (no orphan on either).

    Args:
        trips: Enriched trips DataFrame carrying ``dq_observations``.

    Returns:
        The trips without a vehicle/driver referential orphan. The enriched
        fact table keeps every trip; only the metrics narrow to the valid ones.
    """
    invalid = F.lit(False)
    for reason in TRIP_INVALIDATING_REASONS:
        invalid = invalid | F.col("dq_observations").contains(reason)
    return trips.filter(~F.coalesce(invalid, F.lit(False)))


def build_metric(name: str, frames: dict[str, DataFrame]) -> DataFrame:
    """Build one metric table and enforce its declared contract.

    Args:
        name: Metric dataset name (a `METRIC_TABLES` key).
        frames: Input DataFrames by name (`trips`, `vehicles`,
            `geofence_positions`).

    Returns:
        The final metric DataFrame, matching the declared schema.
    """
    spec = METRIC_TABLES[name]
    if "trips" in spec["inputs"]:
        # Metrics aggregate over valid trips only; orphan trips stay in
        # viagens_enriquecidas but must not skew the aggregates.
        frames = {**frames, "trips": valid_trips(frames["trips"])}
    inputs = [frames[input_name] for input_name in spec["inputs"]]
    df = spec["builder"](*inputs).withColumn("processed_at", F.current_timestamp())
    return enforce_table_config(df, load_table_config(table_config_path(name)))


def build_all_metrics(frames: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build every registered metric table.

    Args:
        frames: Input DataFrames by name (`trips`, `vehicles`,
            `geofence_positions`).

    Returns:
        The metric DataFrames by dataset name.
    """
    return {name: build_metric(name, frames) for name in METRIC_TABLES}


def transform_to_analytics(spark: SparkSession, ingest_date: str) -> dict:
    """Read the gold/staging inputs, build and write every metric table.

    Args:
        spark: Active SparkSession (must be created with
            `enable_delta=True`).
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used to
            locate the input partitions and to partition the metric
            tables.

    Returns:
        Metrics about the write: layer name, ingestion date and the
        record count per metric table.
    """
    log.info(
        "Reading enriched trips from %s (ingest_date=%s)",
        ANALYTICS_VIAGENS_ENRIQUECIDAS_DIR,
        ingest_date,
    )
    trips = read_delta_partition(spark, ANALYTICS_VIAGENS_ENRIQUECIDAS_DIR, ingest_date)
    log.info(
        "Reading geofence positions from %s (ingest_date=%s)",
        ANALYTICS_POSICOES_GEOCERCAS_DIR,
        ingest_date,
    )
    geofence_positions = read_delta_partition(
        spark, ANALYTICS_POSICOES_GEOCERCAS_DIR, ingest_date
    )
    log.info("Reading staging vehicles from %s (ingest_date=%s)", STAGING_VEICULOS_DIR, ingest_date)
    vehicles = read_delta_partition(spark, STAGING_VEICULOS_DIR, ingest_date)

    frames = {
        "trips": trips,
        "vehicles": vehicles,
        "geofence_positions": geofence_positions,
    }
    records = {}
    for name, df in build_all_metrics(frames).items():
        destination = layer_dir("analytics", name)
        log.info("Writing metric table %s to %s (Delta)", name, destination)
        write_delta_partition(df, destination, ingest_date)
        mark_delta_partition_processed(destination, ingest_date)
        records[name] = df.count()

    log.info("Metrics transform finished - %s", records)
    return {
        "layer": "analytics",
        "ingest_date": ingest_date,
        "records": records,
    }
