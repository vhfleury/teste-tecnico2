"""posicoes_geocercas analytics: geospatial enrichment of tracking positions.

One stage, one task:

* ``transform_to_analytics`` — reads the staging positions and geofences,
  enriches each position with the geofence that contains it
  (`enrich_positions_with_geofences`) and writes the result to the
  analytics layer.

Staging standardized the FORM of both inputs; this module applies the
SEMANTICS: point-in-polygon matching, location classification
(`em_geocerca` / `em_rota`) and geofence entry/exit events along each
trip. Joins and business rules live here in code — the table config
declares only names and types. Every join is a LEFT JOIN from the
fact (positions), so no row is ever dropped.
"""
from __future__ import annotations

import json
import logging
import os

from data_quality.validation import enforce_table_config
from general.utils import (
    layer_dir,
    load_table_config,
    partition_path,
)
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StructField, StructType

log = logging.getLogger(__name__)

SOURCE = "posicoes_geocercas"

# Staging inputs consumed by this analytics table.
STAGING_POSICOES_DIR = layer_dir("staging", "posicoes")
STAGING_GEOCERCAS_DIR = layer_dir("staging", "geocercas")
ANALYTICS_DIR = layer_dir("analytics", SOURCE)

# Table config (contract) of the analytics table written by this module.
TABLE_CONFIG = os.path.join(os.path.dirname(__file__), f"analytics_{SOURCE}.json")

# Return type of the `polygon_bounds` UDF: the bounding box of a geofence
# polygon, used to prefilter join candidates before the exact
# point-in-polygon check.
GEOFENCE_BOUNDS_SCHEMA = StructType(
    [
        StructField("min_longitude", DoubleType()),
        StructField("max_longitude", DoubleType()),
        StructField("min_latitude", DoubleType()),
        StructField("max_latitude", DoubleType()),
    ]
)


def _extract_outer_ring(geometry: str | None) -> list[tuple[float, float]] | None:
    """Parse the outer ring from a serialized GeoJSON Polygon.

    Defensive by design: any malformed geometry (unparseable JSON,
    missing coordinates, non-numeric point, ring with fewer than 4
    points) yields None instead of raising, so one bad geofence never
    crashes the whole partition.

    Args:
        geometry: GeoJSON Polygon serialized as a JSON string, as
            canonicalized by the geocercas staging.

    Returns:
        The outer ring as (longitude, latitude) tuples, or None when
        the geometry is absent or malformed.
    """
    if not geometry:
        return None
    try:
        parsed = json.loads(geometry)
        coordinates = parsed.get("coordinates") or []
        ring = coordinates[0]
    except (TypeError, ValueError, IndexError, KeyError):
        return None

    points = []
    for point in ring:
        if not isinstance(point, list | tuple) or len(point) < 2:
            return None
        try:
            longitude = float(point[0])
            latitude = float(point[1])
        except (TypeError, ValueError):
            return None
        points.append((longitude, latitude))
    if len(points) < 4:
        return None
    return points


def _point_is_on_segment(
    point_longitude: float,
    point_latitude: float,
    start_longitude: float,
    start_latitude: float,
    end_longitude: float,
    end_latitude: float,
) -> bool:
    """Check whether a point lies exactly on a polygon edge.

    Boundary points are checked apart from the ray casting because the
    crossing count is ambiguous on edges; the matcher treats them as
    inside.

    Args:
        point_longitude: Longitude of the GPS point.
        point_latitude: Latitude of the GPS point.
        start_longitude: Longitude of the edge start vertex.
        start_latitude: Latitude of the edge start vertex.
        end_longitude: Longitude of the edge end vertex.
        end_latitude: Latitude of the edge end vertex.

    Returns:
        True when the point is collinear with the edge and inside its
        bounding box.
    """
    cross_product = (
        (point_latitude - start_latitude) * (end_longitude - start_longitude)
        - (point_longitude - start_longitude) * (end_latitude - start_latitude)
    )
    if abs(cross_product) > 1e-9:
        return False
    within_longitude = min(start_longitude, end_longitude) <= point_longitude <= max(
        start_longitude, end_longitude
    )
    within_latitude = min(start_latitude, end_latitude) <= point_latitude <= max(
        start_latitude, end_latitude
    )
    return within_longitude and within_latitude


def _point_is_inside_geojson_polygon(
    latitude: float | None,
    longitude: float | None,
    geometry: str | None,
) -> bool:
    """Check if a GPS point is inside a serialized GeoJSON Polygon.

    Ray casting over the outer ring: a horizontal ray from the point
    crosses the polygon edges an odd number of times when the point is
    inside. Points exactly on an edge count as inside. Null
    coordinates and malformed geometries are outside — quarantined
    positions stay `em_rota` instead of failing the job.

    Args:
        latitude: Latitude of the GPS point.
        longitude: Longitude of the GPS point.
        geometry: GeoJSON Polygon serialized as a JSON string.

    Returns:
        True when the point is inside (or on the boundary of) the
        polygon.
    """
    if latitude is None or longitude is None:
        return False
    ring = _extract_outer_ring(geometry)
    if not ring:
        return False

    inside = False
    point_longitude = float(longitude)
    point_latitude = float(latitude)
    for index, (start_longitude, start_latitude) in enumerate(ring):
        end_longitude, end_latitude = ring[(index + 1) % len(ring)]
        if _point_is_on_segment(
            point_longitude,
            point_latitude,
            start_longitude,
            start_latitude,
            end_longitude,
            end_latitude,
        ):
            return True
        crosses_ray = (start_latitude > point_latitude) != (end_latitude > point_latitude)
        if crosses_ray:
            intersection_longitude = start_longitude + (
                (point_latitude - start_latitude)
                * (end_longitude - start_longitude)
                / (end_latitude - start_latitude)
            )
            if point_longitude < intersection_longitude:
                inside = not inside
    return inside


def _geojson_polygon_bounds(geometry: str | None) -> tuple[float, float, float, float] | None:
    """Return bounding-box coordinates for a serialized GeoJSON Polygon.

    Args:
        geometry: GeoJSON Polygon serialized as a JSON string.

    Returns:
        (min_longitude, max_longitude, min_latitude, max_latitude), or
        None when the geometry is absent or malformed.
    """
    ring = _extract_outer_ring(geometry)
    if not ring:
        return None
    longitudes = [point[0] for point in ring]
    latitudes = [point[1] for point in ring]
    return (min(longitudes), max(longitudes), min(latitudes), max(latitudes))


# The geometry math is plain Python (json + arithmetic), so it runs as
# UDFs: bounds once per geofence, point-in-polygon only on the
# candidates that pass the bounding-box prefilter.
point_in_polygon = F.udf(_point_is_inside_geojson_polygon, BooleanType())
polygon_bounds = F.udf(_geojson_polygon_bounds, GEOFENCE_BOUNDS_SCHEMA)


def _active_geofences(geofences: DataFrame) -> DataFrame:
    """Prepare active, quality-approved geofences for spatial matching.

    Only geofences that passed staging quality, are active and carry a
    geometry can contain a position; their bounding box is precomputed
    here so the join prefilter compares plain columns. Geofences whose
    geometry yields no bounds (malformed) are excluded from matching —
    they cannot contain any point.

    Args:
        geofences: Staging geofences DataFrame.

    Returns:
        One row per matchable geofence with the renamed geocerca_*
        columns, the canonical geometry and its bounding box.
    """
    return (
        geofences.filter(
            (F.col("quality_ok") == F.lit(True))
            & (F.col("ativo") == F.lit(True))
            & F.col("geometry").isNotNull()
        )
        .withColumn("bounds", polygon_bounds("geometry"))
        .filter(F.col("bounds").isNotNull())
        .select(
            "geocerca_id",
            F.col("nome").alias("geocerca_nome"),
            F.col("tipo").alias("geocerca_tipo"),
            F.col("raio_km").alias("geocerca_raio_km"),
            "geometry",
            F.col("bounds.min_longitude").alias("min_longitude"),
            F.col("bounds.max_longitude").alias("max_longitude"),
            F.col("bounds.min_latitude").alias("min_latitude"),
            F.col("bounds.max_latitude").alias("max_latitude"),
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

    Two-step spatial match, cheap filter first:

    1. Bounding-box prefilter — broadcast join of the positions
       against the (small) geofence table on plain column
       comparisons, so the UDF never runs on the full cross product.
    2. Exact check — the point-in-polygon UDF confirms each candidate;
       ties (overlapping geofences) are broken by the smallest radius,
       then by geofence id, keeping exactly one match per position.

    The join back to the positions is a LEFT JOIN from the fact:
    positions without a containing geofence keep null geocerca_*
    columns and no row is ever dropped.

    Args:
        positions: Staging positions DataFrame.
        geofences: Staging geofences DataFrame.

    Returns:
        The position base columns plus `geocerca_id`, `geocerca_nome`
        and `geocerca_tipo` (null when no geofence contains the
        position).
    """
    position_base = _position_base(positions)
    geofence_base = _active_geofences(geofences)

    join_condition = (
        F.col("p.latitude").isNotNull()
        & F.col("p.longitude").isNotNull()
        & (F.col("p.latitude") >= F.col("g.min_latitude"))
        & (F.col("p.latitude") <= F.col("g.max_latitude"))
        & (F.col("p.longitude") >= F.col("g.min_longitude"))
        & (F.col("p.longitude") <= F.col("g.max_longitude"))
    )
    candidates = position_base.alias("p").join(
        F.broadcast(geofence_base).alias("g"),
        join_condition,
        "left",
    )
    matches = (
        candidates.withColumn(
            "is_inside_geofence",
            F.when(
                F.col("g.geocerca_id").isNotNull(),
                point_in_polygon(F.col("p.latitude"), F.col("p.longitude"), F.col("g.geometry")),
            ).otherwise(F.lit(False)),
        )
        .filter(F.col("is_inside_geofence"))
        .select(
            F.col("p.posicao_id").alias("posicao_id"),
            F.col("g.geocerca_id").alias("geocerca_id"),
            F.col("g.geocerca_nome").alias("geocerca_nome"),
            F.col("g.geocerca_tipo").alias("geocerca_tipo"),
            F.col("g.geocerca_raio_km").alias("geocerca_raio_km"),
        )
    )

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

    Args:
        spark: Active SparkSession.
        ingest_date: Ingestion date in `YYYY-MM-DD` format, used
            to locate the staging partitions and to partition the
            analytics layer.

    Returns:
        Metrics about the write: layer name, destination path, record
        count, how many positions fell inside a geofence and how many
        entry/exit events were flagged.
    """
    positions_path = partition_path(STAGING_POSICOES_DIR, ingest_date)
    geofences_path = partition_path(STAGING_GEOCERCAS_DIR, ingest_date)
    log.info("Reading staging positions from %s", positions_path)
    positions = spark.read.parquet(positions_path)
    log.info("Reading staging geofences from %s", geofences_path)
    geofences = spark.read.parquet(geofences_path)

    config = load_table_config(TABLE_CONFIG)
    df = build_analytics(positions, geofences, config)

    destination = partition_path(ANALYTICS_DIR, ingest_date)
    log.info("Writing analytics layer to %s", destination)
    df.coalesce(1).write.mode("overwrite").parquet(destination)

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
        "path": destination,
        "records": total,
        "records_in_geofence": in_geofence,
        "entry_events": entry_events,
        "exit_events": exit_events,
    }
