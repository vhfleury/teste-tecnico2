"""Support functions for the extraction_veiculos pipeline."""
from __future__ import annotations

import datetime as dt

from parser.treatment import deduplicate_by_key, trim_columns
from parser.validation import (
    VALID_VEHICLE_STATUS,
    VALID_VEHICLE_TYPES,
    apply_quarantine,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PLATE_MERCOSUL_REGEX = r"^[A-Z]{3}[0-9][A-Z][0-9]{2}$"
MIN_MANUFACTURE_YEAR = 1990


def clean_and_validate(raw: DataFrame) -> DataFrame:
    """Standardize types, deduplicate and flag data-quality issues.

    Pure DataFrame -> DataFrame transformation, kept separate from
    I/O so it can be unit-tested with synthetic data. Uses a
    quarantine strategy: an invalid value (e.g. a malformed plate)
    is not dropped - the primary key is preserved (so joins with
    `viagens` keep working), the bad value is nulled out and the
    reason is recorded in `dq_observations`.

    Args:
        raw: Raw vehicle registry DataFrame, as read from the raw
            layer.

    Returns:
        The cleaned DataFrame with quarantine flags and a
        `processed_at` timestamp column.
    """
    df = trim_columns(raw, ["veiculo_id", "placa", "marca", "modelo", "tipo", "status"])
    df = (
        df.withColumn("veiculo_id", F.upper(F.col("veiculo_id")))
        .withColumn("placa", F.upper(F.col("placa")))
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("ano_fabricacao", F.col("ano_fabricacao").cast("int"))
        .withColumn("capacidade_kg", F.col("capacidade_kg").cast("int"))
        .withColumn("capacidade_paletes", F.col("capacidade_paletes").cast("int"))
        .withColumn("km_atual", F.col("km_atual").cast("int"))
        .withColumn("data_ultima_revisao", F.to_date("data_ultima_revisao", "yyyy-MM-dd"))
    )

    df = deduplicate_by_key(df, ["veiculo_id"])

    max_year = dt.date.today().year + 1
    checks = {
        "invalid_plate": F.col("placa").rlike(PLATE_MERCOSUL_REGEX),
        "invalid_mileage": F.col("km_atual").isNotNull() & (F.col("km_atual") >= 0),
        "invalid_year": F.col("ano_fabricacao").between(MIN_MANUFACTURE_YEAR, max_year),
        "invalid_status": F.col("status").isin(VALID_VEHICLE_STATUS),
        "invalid_type": F.col("tipo").isin(VALID_VEHICLE_TYPES),
        "missing_revision_date": F.col("data_ultima_revisao").isNotNull(),
    }
    df = apply_quarantine(df, checks)

    # Quarantine: null out invalid values but keep the row (preserves the PK for joins).
    df = (
        df.withColumn("placa", F.when(checks["invalid_plate"], F.col("placa")))
        .withColumn("km_atual", F.when(checks["invalid_mileage"], F.col("km_atual")))
        .withColumn("ano_fabricacao", F.when(checks["invalid_year"], F.col("ano_fabricacao")))
    )

    return df.withColumn("processed_at", F.current_timestamp())
