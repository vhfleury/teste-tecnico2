"""Dedicated tests for the geocercas exclusive treatment."""
from geocercas.geocercas import flatten_features

RAW_SCHEMA = (
    "type string, "
    "properties struct<geocerca_id:string, nome:string, tipo:string, uf:string, "
    "raio_km:string, ativo:string>, "
    "geometry struct<type:string, coordinates:array<array<array<string>>>>, "
    "source_file string, ingested_at string"
)


def test_flatten_features_promotes_properties_and_serializes_geometry(spark):
    row = {
        "type": "Feature",
        "properties": {
            "geocerca_id": "GEO-0001",
            "nome": "CD São Paulo - Guarulhos",
            "tipo": "centro_distribuicao",
            "uf": "SP",
            "raio_km": "1.0",
            "ativo": "true",
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[["-1.0", "-2.0"], ["1.0", "-2.0"], ["1.0", "2.0"], ["-1.0", "-2.0"]]],
        },
        "source_file": "/opt/airflow/data/geocercas/geocercas.geojson",
        "ingested_at": "2026-07-04 12:00:00",
    }

    result = flatten_features(spark.createDataFrame([row], RAW_SCHEMA))

    assert result.columns == [
        "geocerca_id",
        "nome",
        "tipo",
        "uf",
        "raio_km",
        "ativo",
        "geometry",
        "source_file",
        "ingested_at",
    ]
    flat = result.collect()[0]
    assert flat["geocerca_id"] == "GEO-0001"
    assert flat["raio_km"] == "1.0"
    # Canonical GeoJSON string: numeric coordinates, type first.
    assert flat["geometry"] == (
        '{"type":"Polygon","coordinates":[[[-1.0,-2.0],[1.0,-2.0],[1.0,2.0],[-1.0,-2.0]]]}'
    )


def test_flatten_features_null_geometry_stays_null(spark):
    row = {
        "type": "Feature",
        "properties": {
            "geocerca_id": "GEO-0002",
            "nome": "Cliente Sem Geometria",
            "tipo": "cliente",
            "uf": "BA",
            "raio_km": "0.5",
            "ativo": "true",
        },
        "geometry": None,
        "source_file": "/opt/airflow/data/geocercas/geocercas.geojson",
        "ingested_at": "2026-07-04 12:00:00",
    }

    flat = flatten_features(spark.createDataFrame([row], RAW_SCHEMA)).collect()[0]

    # Not "{}": a missing geometry must stay null so `required` flags it.
    assert flat["geometry"] is None


def test_flatten_features_fields_absent_from_partition_become_null_columns(spark):
    # A partition where no feature has uf nor geometry: the raw schema
    # lacks the paths entirely and flatten must not crash on them.
    schema = (
        "type string, "
        "properties struct<geocerca_id:string, nome:string, tipo:string, "
        "raio_km:string, ativo:string>, "
        "source_file string, ingested_at string"
    )
    row = {
        "type": "Feature",
        "properties": {
            "geocerca_id": "GEO-0003",
            "nome": "Pedágio Sem UF",
            "tipo": "pedagio",
            "raio_km": "0.3",
            "ativo": "true",
        },
        "source_file": "/opt/airflow/data/geocercas/geocercas.geojson",
        "ingested_at": "2026-07-04 12:00:00",
    }

    flat = flatten_features(spark.createDataFrame([row], schema)).collect()[0]

    assert flat["geocerca_id"] == "GEO-0003"
    assert flat["uf"] is None
    assert flat["geometry"] is None
