"""Brazilian phone number validation helpers shared by every pipeline."""
from __future__ import annotations

from parser.statics import DDD_UF
from pyspark.sql import Column
from pyspark.sql import functions as F


def normalize_telefone(telefone: Column) -> Column:
    """Reduce a phone number to DDD + subscriber digits (strips punctuation, `+55`, trunk zero).

    Args:
        telefone: Column with the phone number in any common format.

    Returns:
        String column with the normalized digits.
    """
    digits = F.regexp_replace(telefone, r"[^0-9]", "")
    digits = F.when(
        digits.startswith("55") & (F.length(digits) >= 12),
        F.substring(digits, 3, 20),
    ).otherwise(digits)
    return F.regexp_replace(digits, r"^0+", "")


def ddd_from_telefone(telefone: Column) -> Column:
    """Extract the DDD (area code) from a phone number.

    Args:
        telefone: Column with the phone number in any common format.

    Returns:
        String column with the two-digit DDD.
    """
    return F.substring(normalize_telefone(telefone), 1, 2)


def ddd_to_uf(ddd: Column) -> Column:
    """Resolve the UF (state) a DDD belongs to.

    Args:
        ddd: Column with the two-digit DDD (e.g. `81`).

    Returns:
        String column with the UF (e.g. `PE`), null when the DDD
        does not exist.
    """
    mapping = F.create_map(*[F.lit(value) for pair in DDD_UF.items() for value in pair])
    return mapping[ddd]


def telefone_is_valid(telefone: Column) -> Column:
    """Validate a Brazilian phone number: existing DDD + 8/9-digit subscriber.

    Args:
        telefone: Column with the phone number in any common format.

    Returns:
        Boolean column, False when the phone is null, malformed,
        has an unknown DDD or an invalid subscriber number.
    """
    digits = normalize_telefone(telefone)
    first_subscriber_digit = F.substring(digits, 3, 1)
    return (
        telefone.isNotNull()
        & digits.rlike(r"^[0-9]{10,11}$")
        & F.substring(digits, 1, 2).isin(list(DDD_UF))
        & (
            ((F.length(digits) == 10) & first_subscriber_digit.rlike(r"[2-9]"))
            | ((F.length(digits) == 11) & (first_subscriber_digit == "9"))
        )
    )
