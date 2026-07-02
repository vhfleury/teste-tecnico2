"""CNH validation helpers shared by every pipeline."""
from __future__ import annotations

from pyspark.sql import Column

CNH_DIGITS_REGEX = r"^[0-9]{11}$"


def cnh_is_valid(cnh: Column) -> Column:
    """Validate a CNH registration number: exactly 11 digits.

    Args:
        cnh: Column with the CNH registration number.

    Returns:
        Boolean column, False when the CNH is null or is not an
        11-digit string.
    """
    return cnh.isNotNull() & cnh.rlike(CNH_DIGITS_REGEX)
