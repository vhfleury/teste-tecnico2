"""posicoes_geocercas analytics: geospatial enrichment of tracking positions.

One stage, one task:

* ``transform_to_analytics`` — reads the staging positions and geofences,
  enriches each position with the geofence that contains it
  (`enrich_positions_with_geofences`) and writes the result to the
  analytics layer as a Delta table.

Staging standardized the FORM of both inputs; this module applies the
SEMANTICS: point-in-polygon matching via Apache Sedona, location
classification (`em_geocerca` / `em_rota`) and geofence entry/exit
events along each trip. Joins and business rules live here in code —
the table config declares only names and types. Every join is a LEFT
JOIN from the fact (positions), so no row is ever dropped.
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
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_predicates as stp

log = logging.getLogger(__name__)

SOURCE = "posicoes_geocercas"

# Staging inputs consumed by this analytics table.
STAGING_POSICOES_DIR = layer_dir("staging", "posicoes")
STAGING_GEOCERCAS_DIR = layer_dir("staging", "geocercas")
ANALYTICS_DIR = layer_dir("analytics", SOURCE)

# Table config (contract) of the analytics table written by this module.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"analytics_{SOURCE}.json")

# Structural shape of a serialized GeoJSON Polygon, used by `from_json`
# to pre-validate geometries: Sedona's ST_GeomFromGeoJSON raises on
# malformed input, so only geometries that pass this check reach it —
# one bad geofence never crashes the whole partition.
GEOJSON_POLYGON_SCHEMA = StructType(
    [
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(ArrayType(ArrayType(DoubleType())))),
    ]
)


def _is_valid_geojson_polygon(geometry: Column) -> Column:
    """Build a predicate that structurally validates a GeoJSON Polygon.

    Defensive by design: unparseable JSON, wrong geometry type, missing
    coordinates, non-numeric points, rings with fewer than 4 points or
    unclosed rings are all flagged invalid instead of raising, so the
    matcher can exclude them before Sedona parses the geometry.

    Args:
        geometry: Column with the GeoJSON Polygon serialized as a JSON
            string, as canonicalized by the geocercas staging.

    Returns:
        Boolean Column, true only for structurally valid polygons.
    """
    parsed = F.from_json(geometry, GEOJSON_POLYGON_SCHEMA)
    outer_ring = parsed["coordinates"].getItem(0)
    has_valid_points = ~F.exists(
        outer_ring,
        lambda point: point.isNull() | point.getItem(0).isNull() | point.getItem(1).isNull(),
    )
    is_closed_ring = F.element_at(outer_ring, 1) == F.element_at(outer_ring, -1)
    return (
        parsed.isNotNull()
        & (parsed["type"] == F.lit("Polygon"))
        & outer_ring.isNotNull()
        & (F.size(outer_ring) >= F.lit(4))
        & has_valid_points
        & is_closed_ring
    )


def _active_geofences(geofences: DataFrame) -> DataFrame:
    """Prepare active, quality-approved geofences for spatial matching.

    Only geofences that passed staging quality, are active and carry a
    structurally valid geometry can contain a position; the GeoJSON is
    parsed once here into a Sedona geometry. Geofences with malformed
    geometry are excluded from matching — they cannot contain any
    point.

    Args:
        geofences: Staging geofences DataFrame.

    Returns:
        One row per matchable geofence with the renamed geocerca_*
        columns and the parsed Sedona geometry.
    """
    return (
        geofences.filter(
            (F.col("quality_ok") == F.lit(True))
            & (F.col("ativo") == F.lit(True))
            & F.col("geometry").isNotNull()
        )
        .filter(_is_valid_geojson_polygon(F.col("geometry")))
        .select(
            "geocerca_id",
            F.col("nome").alias("geocerca_nome"),
            F.col("tipo").alias("geocerca_tipo"),
            F.col("raio_km").alias("geocerca_raio_km"),
            stc.ST_GeomFromGeoJSON("geometry").alias("geofence_geometry"),
        )
    )


def _position_base(positions: DataFrame) -> DataFrame:
    """Select the position columns consumed and emitted by analytics.

    Args:
        positions: Staging positions DataFrame.

    Returns:
        The positions projected to the analytics contract columns
        (identity, GPS reading and control columns).
    """
    return positions.select(
        "posicao_id",
        "viagem_id",
        "veiculo_id",
        "timestamp",
        "latitude",
        "longitude",
        "velocidade_kmh",
        "source_file",
        "ingested_at",
        "dq_observations",
        "quality_ok",
    )


def match_positions_to_geofences(positions: DataFrame, geofences: DataFrame) -> DataFrame:
    """Left-enrich each position with the best containing geofence, if any.

    The spatial match is delegated to Apache Sedona:

    1. Each position with coordinates becomes an ``ST_Point`` and each
       matchable geofence a Sedona geometry (``ST_GeomFromGeoJSON``).
    2. ``ST_Intersects`` joins the points against the (small,
       broadcast) geofence table — Sedona plans an indexed spatial
       join, replacing the manual bounding-box prefilter. Boundary
       points intersect their polygon, so they count as inside. Ties
       (overlapping geofences) are broken by the smallest radius, then
       by geofence id, keeping exactly one match per position.

    The join back to the positions is a LEFT JOIN from the fact:
    positions without a containing geofence (including quarantined
    rows with null coordinates) keep null geocerca_* columns and no
    row is ever dropped.

    Args:
        positions: Staging positions DataFrame.
        geofences: Staging geofences DataFrame.

    Returns:
        The position base columns plus `geocerca_id`, `geocerca_nome`
        and `geocerca_tipo` (null when no geofence contains the
        position).
    """
    SedonaContext.create(positions.sparkSession)
    position_base = _position_base(positions)
    geofence_base = _active_geofences(geofences)

    position_points = position_base.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select(
        "posicao_id",
        stc.ST_Point("longitude", "latitude").alias("position_point"),
    )
    matches = position_points.join(
        F.broadcast(geofence_base),
        stp.ST_Intersects(F.col("geofence_geometry"), F.col("position_point")),
        "inner",
    ).select("posicao_id", "geocerca_id", "geocerca_nome", "geocerca_tipo", "geocerca_raio_km")

    match_window = Window.partitionBy("posicao_id").orderBy(
        F.col("geocerca_raio_km").asc_nulls_last(),
        F.col("geocerca_id"),
    )
    best_match = (
        matches.withColumn("match_rank", F.row_number().over(match_window))
        .filter(F.col("match_rank") == 1)
        .select("posicao_id", "geocerca_id", "geocerca_nome", "geocerca_tipo")
    )
    return position_base.join(best_match, "posicao_id", "left")


def enrich_positions_with_geofences(positions: DataFrame, geofences: DataFrame) -> DataFrame:
    """Classify positions and detect geofence entry/exit events.

    Business rules applied on top of the spatial match:

    * ``classificacao_localizacao`` — `em_geocerca` when a geofence
      contains the position, `em_rota` otherwise.
    * ``evento_entrada_geocerca`` / ``evento_saida_geocerca`` — the
      previous position of the same trip (window ordered by timestamp,
      tie-broken by `posicao_id`) decides the transition; moving
      straight between two geofences flags both an exit and an entry
      on the same position.

    Args:
        positions: Staging positions DataFrame.
        geofences: Staging geofences DataFrame.

    Returns:
        The enriched DataFrame with classification, event flags and a
        `processed_at` timestamp column.
    """
    matched = match_positions_to_geofences(positions, geofences).withColumn(
        "classificacao_localizacao",
        F.when(F.col("geocerca_id").isNotNull(), F.lit("em_geocerca")).otherwise(
            F.lit("em_rota")
        ),
    )

    trip_window = Window.partitionBy("viagem_id", "veiculo_id").orderBy(
        F.col("timestamp").asc_nulls_last(),
        F.col("posicao_id"),
    )
    with_previous = matched.withColumn(
        "geocerca_anterior_id",
        F.lag("geocerca_id").over(trip_window),
    )
    current_geofence = F.col("geocerca_id")
    previous_geofence = F.col("geocerca_anterior_id")
    return (
        with_previous.withColumn(
            "evento_entrada_geocerca",
            current_geofence.isNotNull()
            & (previous_geofence.isNull() | (current_geofence != previous_geofence)),
        )
        .withColumn(
            "evento_saida_geocerca",
            previous_geofence.isNotNull()
            & (current_geofence.isNull() | (current_geofence != previous_geofence)),
        )
        .drop("geocerca_anterior_id")
        .withColumn("processed_at", F.current_timestamp())
    )


def build_analytics(posicoes: DataFrame, geocercas: DataFrame, config: dict) -> DataFrame:
    """Build the analytics output and enforce the declared contract.

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be tested with synthetic data. The table config is
    the analytics contract: wrong columns/types abort the write.

    Args:
        posicoes: Staging positions DataFrame.
        geocercas: Staging geofences DataFrame.
        config: Parsed table config (the analytics contract).

    Returns:
        The final analytics DataFrame, matching the declared schema.
    """
    return enforce_table_config(enrich_positions_with_geofences(posicoes, geocercas), config)


def transform_to_analytics(spark: SparkSession, ingest_date: str) -> dict:
    """Read staging inputs, build geospatial analytics and write the partition.

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
        Metrics about the write: layer name, destination table,
        ingestion date, record count, how many positions fell inside
        a geofence and how many entry/exit events were flagged.
    """
    log.info("Reading staging positions from %s (ingest_date=%s)", STAGING_POSICOES_DIR, ingest_date)
    positions = read_delta_partition(spark, STAGING_POSICOES_DIR, ingest_date)
    log.info("Reading staging geofences from %s (ingest_date=%s)", STAGING_GEOCERCAS_DIR, ingest_date)
    geofences = read_delta_partition(spark, STAGING_GEOCERCAS_DIR, ingest_date)

    config = load_table_config(TABLE_CONFIG)
    df = build_analytics(positions, geofences, config)

    log.info("Writing analytics layer to %s (Delta)", ANALYTICS_DIR)
    write_delta_partition(df, ANALYTICS_DIR, ingest_date)
    mark_delta_partition_processed(ANALYTICS_DIR, ingest_date)

    total = df.count()
    in_geofence = df.filter(F.col("classificacao_localizacao") == "em_geocerca").count()
    entry_events = df.filter(F.col("evento_entrada_geocerca")).count()
    exit_events = df.filter(F.col("evento_saida_geocerca")).count()
    log.info(
        "Analytics transform finished - %d records, %d in geofence, "
        "%d entry events, %d exit events",
        total,
        in_geofence,
        entry_events,
        exit_events,
    )
    return {
        "layer": "analytics",
        "path": ANALYTICS_DIR,
        "ingest_date": ingest_date,
        "records": total,
        "records_in_geofence": in_geofence,
        "entry_events": entry_events,
        "exit_events": exit_events,
    }
