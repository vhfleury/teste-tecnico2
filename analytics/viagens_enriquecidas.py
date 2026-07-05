"""viagens_enriquecidas analytics: consolidated trips fact.

One stage, one task:

* ``transform_to_analytics`` — reads the five staging inputs, enriches
  each trip with its vehicle, driver, origin/destination geofences and
  per-trip tracking metrics (`enrich_trips`) and writes the result to
  the analytics layer as a Delta table.

Staging standardized the FORM of every input; this module applies the
SEMANTICS: dimension lookups, per-trip position aggregates and derived
trip metrics. Joins and business rules live here in code — the table
config declares only names and types. Every join is a LEFT JOIN from
the fact (trips), so no row is ever dropped: referential orphans are
flagged in ``dq_observations`` (merged with the observations inherited
from staging) with ``quality_ok`` set to false.

Position aggregates are computed at the trip grain BEFORE the join, so
the fact never fans out. Staging already guarantees unique, non-null
primary keys on every dimension (`deduplicate_by_key`), so dimension
joins cannot fan out either.
"""
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
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SOURCE = "viagens_enriquecidas"

# Staging inputs consumed by this analytics table.
STAGING_VIAGENS_DIR = layer_dir("staging", "viagens")
STAGING_VEICULOS_DIR = layer_dir("staging", "veiculos")
STAGING_MOTORISTAS_DIR = layer_dir("staging", "motoristas")
STAGING_GEOCERCAS_DIR = layer_dir("staging", "geocercas")
STAGING_POSICOES_DIR = layer_dir("staging", "posicoes")
ANALYTICS_DIR = layer_dir("analytics", SOURCE)

# Table config (contract) of the analytics table written by this module.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"analytics_{SOURCE}.json")

SECONDS_PER_HOUR = 3600


def _hours_between(start_column: str, end_column: str) -> Column:
    """Compute the signed difference between two timestamps, in hours.

    Args:
        start_column: Name of the timestamp column the interval starts at.
        end_column: Name of the timestamp column the interval ends at.

    Returns:
        The difference in hours rounded to 2 decimals (negative when
        the end precedes the start), or null when either side is null.
    """
    seconds = F.col(end_column).cast("double") - F.col(start_column).cast("double")
    return F.round(seconds / SECONDS_PER_HOUR, 2)


def _vehicle_dimension(vehicles: DataFrame) -> DataFrame:
    """Project the vehicle attributes carried by the enriched trip.

    Args:
        vehicles: Staging vehicles DataFrame.

    Returns:
        One row per vehicle with the renamed veiculo_* columns and a
        match marker consumed by the orphan checks.
    """
    return vehicles.select(
        "veiculo_id",
        F.col("placa").alias("veiculo_placa"),
        F.col("marca").alias("veiculo_marca"),
        F.col("modelo").alias("veiculo_modelo"),
        F.col("tipo").alias("veiculo_tipo"),
        F.col("capacidade_kg").alias("veiculo_capacidade_kg"),
        F.col("status").alias("veiculo_status"),
        F.lit(True).alias("veiculo_matched"),
    )


def _driver_dimension(drivers: DataFrame) -> DataFrame:
    """Project the driver attributes carried by the enriched trip.

    Args:
        drivers: Staging drivers DataFrame.

    Returns:
        One row per driver with the renamed motorista_* columns and a
        match marker consumed by the orphan checks.
    """
    return drivers.select(
        "motorista_id",
        F.col("nome").alias("motorista_nome"),
        F.col("categoria_cnh").alias("motorista_categoria_cnh"),
        F.col("base_operacional").alias("motorista_base_operacional"),
        F.col("status").alias("motorista_status"),
        F.lit(True).alias("motorista_matched"),
    )


def _geofence_dimension(geofences: DataFrame, role: str, key_column: str) -> DataFrame:
    """Project geofences as the trip's origin or destination dimension.

    Inactive and quarantined geofences are kept: a trip that references
    them still deserves the descriptive attributes — referential
    integrity only breaks when the id is unknown.

    Args:
        geofences: Staging geofences DataFrame.
        role: Column prefix for the projected attributes (`origem` or
            `destino`).
        key_column: Fact column this dimension joins on
            (`geocerca_origem_id` or `geocerca_destino_id`).

    Returns:
        One row per geofence keyed by `key_column` with the renamed
        `<role>_*` columns and a match marker consumed by the orphan
        checks.
    """
    return geofences.select(
        F.col("geocerca_id").alias(key_column),
        F.col("nome").alias(f"{role}_nome"),
        F.col("tipo").alias(f"{role}_tipo"),
        F.col("uf").alias(f"{role}_uf"),
        F.lit(True).alias(f"{role}_matched"),
    )


def aggregate_positions_by_trip(positions: DataFrame) -> DataFrame:
    """Aggregate tracking positions to the trip grain before the join.

    Invalid readings (absurd speeds, bad coordinates) were already
    nullified by the staging quarantine, so the average naturally
    skips them; positions without a trip id cannot be attributed and
    are left out.

    Args:
        positions: Staging positions DataFrame.

    Returns:
        One row per trip with the average speed, position count and
        first/last position timestamps.
    """
    return (
        positions.filter(F.col("viagem_id").isNotNull())
        .groupBy("viagem_id")
        .agg(
            F.round(F.avg("velocidade_kmh"), 2).alias("velocidade_media_kmh"),
            F.count(F.lit(1)).alias("num_posicoes"),
            F.min("timestamp").alias("primeira_posicao_ts"),
            F.max("timestamp").alias("ultima_posicao_ts"),
        )
    )


def _merge_quarantine(df: DataFrame) -> DataFrame:
    """Flag referential orphans without losing the staging observations.

    The staging quarantine columns were renamed before the joins;
    here the new join-based reasons are appended to them so a trip
    flagged upstream (e.g. missing_start_date) keeps that history.
    `quality_ok` is recomputed from the merged observations: true only
    when staging passed AND every reference resolved.

    Args:
        df: Enriched DataFrame carrying the `staging_dq_observations`
            / `staging_quality_ok` columns and the dimension match
            markers.

    Returns:
        The DataFrame with the merged `dq_observations` /
        `quality_ok` columns and the helper columns dropped.
    """
    checks = {
        "vehicle_not_found": F.col("veiculo_matched").isNotNull(),
        "driver_not_found": F.col("motorista_matched").isNotNull(),
        "origin_geofence_not_found": F.col("origem_matched").isNotNull(),
        "destination_geofence_not_found": F.col("destino_matched").isNotNull(),
        "no_positions_found": F.col("num_posicoes").isNotNull(),
    }
    join_observations = F.concat_ws(
        ";",
        *[F.when(~condition, F.lit(reason)) for reason, condition in checks.items()],
    )
    merged = F.concat_ws(
        ";",
        F.when(F.length("staging_dq_observations") > 0, F.col("staging_dq_observations")),
        F.when(F.length(join_observations) > 0, join_observations),
    )
    return (
        df.withColumn("dq_observations", merged)
        .withColumn("quality_ok", F.length("dq_observations") == 0)
        .drop(
            "staging_dq_observations",
            "staging_quality_ok",
            "veiculo_matched",
            "motorista_matched",
            "origem_matched",
            "destino_matched",
        )
    )


def enrich_trips(
    trips: DataFrame,
    vehicles: DataFrame,
    drivers: DataFrame,
    geofences: DataFrame,
    positions: DataFrame,
) -> DataFrame:
    """Enrich each trip with dimensions, tracking aggregates and metrics.

    Business rules applied on top of the LEFT JOINs from the fact:

    * ``duracao_horas`` / ``duracao_prevista_horas`` — actual/planned
      trip duration from ``data_inicio``.
    * ``atraso_horas`` — signed actual-vs-planned end difference
      (negative means early), null while the trip has no actual end.
    * ``atrasada_flag`` — explicit `ATRASADA` status OR actual end
      after the planned end (a late trip closed as `CONCLUIDA` still
      counts as delayed).
    * ``mes`` — `yyyy-MM` month of ``data_inicio`` (UTC session), the
      bucket used by the monthly metric tables.

    Args:
        trips: Staging trips DataFrame (the fact).
        vehicles: Staging vehicles DataFrame.
        drivers: Staging drivers DataFrame.
        geofences: Staging geofences DataFrame.
        positions: Staging positions DataFrame.

    Returns:
        The enriched DataFrame with merged quarantine columns and a
        `processed_at` timestamp column.
    """
    fact = trips.withColumnRenamed(
        "dq_observations", "staging_dq_observations"
    ).withColumnRenamed("quality_ok", "staging_quality_ok")

    enriched = (
        fact.join(F.broadcast(_vehicle_dimension(vehicles)), "veiculo_id", "left")
        .join(F.broadcast(_driver_dimension(drivers)), "motorista_id", "left")
        .join(
            F.broadcast(_geofence_dimension(geofences, "origem", "geocerca_origem_id")),
            "geocerca_origem_id",
            "left",
        )
        .join(
            F.broadcast(_geofence_dimension(geofences, "destino", "geocerca_destino_id")),
            "geocerca_destino_id",
            "left",
        )
        .join(aggregate_positions_by_trip(positions), "viagem_id", "left")
    )

    is_delayed_status = F.coalesce(F.col("status") == F.lit("ATRASADA"), F.lit(False))
    ended_late = F.coalesce(F.col("data_fim_real") > F.col("data_fim_prevista"), F.lit(False))
    enriched = (
        enriched.withColumn("duracao_horas", _hours_between("data_inicio", "data_fim_real"))
        .withColumn(
            "duracao_prevista_horas", _hours_between("data_inicio", "data_fim_prevista")
        )
        .withColumn("atraso_horas", _hours_between("data_fim_prevista", "data_fim_real"))
        .withColumn("atrasada_flag", is_delayed_status | ended_late)
        .withColumn("mes", F.date_format("data_inicio", "yyyy-MM"))
    )

    return _merge_quarantine(enriched).withColumn("processed_at", F.current_timestamp())


def build_analytics(
    viagens: DataFrame,
    veiculos: DataFrame,
    motoristas: DataFrame,
    geocercas: DataFrame,
    posicoes: DataFrame,
    config: dict,
) -> DataFrame:
    """Build the analytics output and enforce the declared contract.

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be tested with synthetic data. The table config is
    the analytics contract: wrong columns/types abort the write.

    Args:
        viagens: Staging trips DataFrame (the fact).
        veiculos: Staging vehicles DataFrame.
        motoristas: Staging drivers DataFrame.
        geocercas: Staging geofences DataFrame.
        posicoes: Staging positions DataFrame.
        config: Parsed table config (the analytics contract).

    Returns:
        The final analytics DataFrame, matching the declared schema.
    """
    return enforce_table_config(
        enrich_trips(viagens, veiculos, motoristas, geocercas, posicoes), config
    )


def transform_to_analytics(spark: SparkSession, ingest_date: str) -> dict:
    """Read staging inputs, build the enriched trips and write the partition.

    The output is a Delta table: the write atomically replaces only
    this `ingest_date` partition and the marker is set right after
    the commit (Delta writes no `_SUCCESS` file).

    Args:
        spark: Active SparkSession (must be created with
            `enable_delta=True`).
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to locate the staging partitions and to partition the
            analytics layer.

    Returns:
        Metrics about the write: layer name, destination path, record
        count, how many trips were flagged by quarantine and how many
        are delayed.
    """
    staging_inputs = {
        "trips": STAGING_VIAGENS_DIR,
        "vehicles": STAGING_VEICULOS_DIR,
        "drivers": STAGING_MOTORISTAS_DIR,
        "geofences": STAGING_GEOCERCAS_DIR,
        "positions": STAGING_POSICOES_DIR,
    }
    frames = {}
    for name, base_dir in staging_inputs.items():
        log.info("Reading staging %s from %s (ingest_date=%s)", name, base_dir, ingest_date)
        frames[name] = read_delta_partition(spark, base_dir, ingest_date)

    config = load_table_config(TABLE_CONFIG)
    df = build_analytics(
        frames["trips"],
        frames["vehicles"],
        frames["drivers"],
        frames["geofences"],
        frames["positions"],
        config,
    )

    log.info("Writing analytics layer to %s (Delta)", ANALYTICS_DIR)
    write_delta_partition(df, ANALYTICS_DIR, ingest_date)
    mark_delta_partition_processed(ANALYTICS_DIR, ingest_date)

    total = df.count()
    flagged = df.filter(~F.col("quality_ok")).count()
    delayed = df.filter(F.col("atrasada_flag")).count()
    log.info(
        "Analytics transform finished - %d trips, %d flagged, %d delayed",
        total,
        flagged,
        delayed,
    )
    return {
        "layer": "analytics",
        "path": ANALYTICS_DIR,
        "ingest_date": ingest_date,
        "records": total,
        "records_flagged": flagged,
        "records_delayed": delayed,
    }
