"""CPF validation helpers shared by every pipeline."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

CPF_FORMAT_REGEX = r"^[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]{2}$"


def _cpf_check_digit(digits: Column, length: int) -> Column:
    """Compute one CPF check digit from the leading digits.

    Standard CPF rule: multiply the first ``length`` digits by
    decreasing weights (``length + 1`` down to ``2``), then the check
    digit is ``((sum * 10) % 11) % 10``.

    Args:
        digits: Column with the CPF digits only (no punctuation).
        length: How many leading digits feed the check digit
            (9 for the first check digit, 10 for the second).

    Returns:
        Column with the expected check digit.
    """
    weighted = sum(
        F.substring(digits, position + 1, 1).try_cast("int") * (length + 1 - position)
        for position in range(length)
    )
    return ((weighted * 10) % 11) % 10


def cpf_is_valid(cpf: Column) -> Column:
    """Validate a CPF: format, not all-same digits and check digits.

    Args:
        cpf: Column with the CPF in `XXX.XXX.XXX-XX` format.

    Returns:
        Boolean column, False when the CPF is null, malformed or
        fails the check-digit rule.
    """
    digits = F.regexp_replace(cpf, r"[^0-9]", "")
    return (
        cpf.isNotNull()
        & cpf.rlike(CPF_FORMAT_REGEX)
        & (digits != F.repeat(F.substring(digits, 1, 1), 11))
        & (F.substring(digits, 10, 1).try_cast("int") == _cpf_check_digit(digits, 9))
        & (F.substring(digits, 11, 1).try_cast("int") == _cpf_check_digit(digits, 10))
    )
