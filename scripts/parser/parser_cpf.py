"""CPF validation helpers shared by every pipeline."""
from __future__ import annotations

from parser.statics import CPF_DIGITS_REGEX
from pyspark.sql import Column
from pyspark.sql import functions as F


def normalize_cpf(cpf: Column) -> Column:
    """Strip the CPF punctuation, keeping only its digits.

    Args:
        cpf: Column with the CPF, formatted (`XXX.XXX.XXX-XX`) or not.

    Returns:
        String column with only the CPF digits (e.g. `52998224725`).
    """
    return F.regexp_replace(cpf, r"[^0-9]", "")


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
        F.substring(digits, position + 1, 1).cast("int") * (length + 1 - position)
        for position in range(length)
    )
    return ((weighted * 10) % 11) % 10


def cpf_is_valid(cpf: Column) -> Column:
    """Validate a CPF: 11 digits, not all-same digits and check digits.

    The CPF is normalized first, so both the formatted
    (`XXX.XXX.XXX-XX`) and the digits-only forms are accepted.

    Args:
        cpf: Column with the CPF, formatted or digits-only.

    Returns:
        Boolean column, False when the CPF is null, malformed or
        fails the check-digit rule.
    """
    digits = normalize_cpf(cpf)
    return (
        cpf.isNotNull()
        & digits.rlike(CPF_DIGITS_REGEX)
        & (digits != F.repeat(F.substring(digits, 1, 1), 11))
        & (F.substring(digits, 10, 1).cast("int") == _cpf_check_digit(digits, 9))
        & (F.substring(digits, 11, 1).cast("int") == _cpf_check_digit(digits, 10))
    )
