"""CNH validation helpers shared by every pipeline."""
from __future__ import annotations

from pyspark.sql import Column

CNH_DIGITS_REGEX = r"^[0-9]{11}$"
# Truck-driver categories (docs/dados.md): the fleet only employs C, D and E drivers.
VALID_CNH_CATEGORIES = ["C", "D", "E"]


def cnh_is_valid(cnh: Column) -> Column:
    """Validate a CNH registration number: exactly 11 digits.

    Args:
        cnh: Column with the CNH registration number.

    Returns:
        Boolean column, False when the CNH is null or is not an
        11-digit string.
    """
    return cnh.isNotNull() & cnh.rlike(CNH_DIGITS_REGEX)


def cnh_category_is_valid(category: Column) -> Column:
    """Validate a CNH category against the accepted set.

    Args:
        category: Column with the CNH category (e.g. `C`).

    Returns:
        Boolean column, False when the category is null or outside
        the accepted set.
    """
    return category.isin(VALID_CNH_CATEGORIES)
