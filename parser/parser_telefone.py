"""Brazilian phone number validation helpers shared by every pipeline."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Brazilian area codes (DDD) and the state (UF) each one belongs to.
DDD_UF = {
    "11": "SP", "12": "SP", "13": "SP", "14": "SP", "15": "SP",
    "16": "SP", "17": "SP", "18": "SP", "19": "SP",
    "21": "RJ", "22": "RJ", "24": "RJ",
    "27": "ES", "28": "ES",
    "31": "MG", "32": "MG", "33": "MG", "34": "MG", "35": "MG",
    "37": "MG", "38": "MG",
    "41": "PR", "42": "PR", "43": "PR", "44": "PR", "45": "PR", "46": "PR",
    "47": "SC", "48": "SC", "49": "SC",
    "51": "RS", "53": "RS", "54": "RS", "55": "RS",
    "61": "DF",
    "62": "GO", "64": "GO",
    "63": "TO",
    "65": "MT", "66": "MT",
    "67": "MS",
    "68": "AC",
    "69": "RO",
    "71": "BA", "73": "BA", "74": "BA", "75": "BA", "77": "BA",
    "79": "SE",
    "81": "PE", "87": "PE",
    "82": "AL",
    "83": "PB",
    "84": "RN",
    "85": "CE", "88": "CE",
    "86": "PI", "89": "PI",
    "91": "PA", "93": "PA", "94": "PA",
    "92": "AM", "97": "AM",
    "95": "RR",
    "96": "AP",
    "98": "MA", "99": "MA",
}


def normalize_telefone(telefone: Column) -> Column:
    """Reduce a phone number to DDD + subscriber digits.

    Strips punctuation, the optional `+55` country code and the
    trunk zero (e.g. `(071)` becomes `71`), so
    `+55 (071) 2827-1996` and `71 2827 1996` normalize to the same
    `7128271996`.

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
    """Validate a Brazilian phone number: DDD + 8/9-digit subscriber.

    After normalization the number must have 10 digits (landline) or
    11 digits (mobile), an existing DDD, and a subscriber number that
    does not start with 0 or 1 (mobiles must start with 9).

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
