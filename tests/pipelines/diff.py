"""Golden-file comparison for pipeline tests.

Compares result rows against the frozen expected output by primary
key (never by position) and reports every divergence in three
categories: rows missing from the result, unexpected extra rows and
cell-level differences (including columns present on one side only).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

MISSING = "<missing>"


def dataframe_to_rows(df: DataFrame, mask: tuple = ()) -> list[dict]:
    """Collect a DataFrame as JSON-friendly dicts.

    Dates and timestamps become strings so rows compare cleanly
    against a JSON fixture. Columns in ``mask`` are kept in place
    (the table must contain them, in order) but their value becomes
    null - they are runtime metadata whose value is non-deterministic
    (e.g. `processed_at`).

    Args:
        df: DataFrame to collect.
        mask: Column names whose value is replaced with null.

    Returns:
        One dict per row, in collection order, columns in DataFrame
        order.
    """
    rows = []
    for row in df.collect():
        data = {}
        for key, value in row.asDict().items():
            if key in mask:
                data[key] = None
            elif value is None or isinstance(value, (bool, int, float, str)):
                data[key] = value
            else:
                data[key] = str(value)
        rows.append(data)
    return rows


def diff_rows(result: list[dict], expected: list[dict], key_columns: list[str]) -> list[str]:
    """Describe every divergence between result and expected rows.

    Args:
        result: Rows produced by the pipeline under test.
        expected: Rows from the golden file.
        key_columns: Primary-key columns used to pair rows.

    Returns:
        Human-readable problems, empty when both sides match.
    """
    def key_of(row):
        return tuple(row.get(column) for column in key_columns)

    result_by_key = {key_of(row): row for row in result}
    expected_by_key = {key_of(row): row for row in expected}

    problems = []
    for key in sorted(expected_by_key.keys() - result_by_key.keys(), key=repr):
        problems.append(f"missing row: key={key}")
    for key in sorted(result_by_key.keys() - expected_by_key.keys(), key=repr):
        problems.append(f"unexpected row: key={key}")

    paired = expected_by_key.keys() & result_by_key.keys()
    if paired:
        sample = min(paired, key=repr)
        expected_order = list(expected_by_key[sample].keys())
        result_order = list(result_by_key[sample].keys())
        if expected_order != result_order and set(expected_order) == set(result_order):
            problems.append(
                f"column order differs: expected {expected_order}, got {result_order}"
            )

    for key in sorted(paired, key=repr):
        expected_row, result_row = expected_by_key[key], result_by_key[key]
        for column in sorted(expected_row.keys() | result_row.keys()):
            expected_value = expected_row.get(column, MISSING)
            result_value = result_row.get(column, MISSING)
            if expected_value != result_value:
                problems.append(
                    f"key={key} column '{column}': "
                    f"expected {expected_value!r}, got {result_value!r}"
                )
    return problems


def assert_matches_expected(
    result: list[dict],
    expected: list[dict],
    key_columns: list[str],
) -> None:
    """Assert result rows equal the golden rows, else fail with the full diff.

    Args:
        result: Rows produced by the pipeline under test.
        expected: Rows from the golden file.
        key_columns: Primary-key columns used to pair rows.

    Raises:
        AssertionError: With one line per divergence.
    """
    problems = diff_rows(result, expected, key_columns)
    assert not problems, "golden file divergence:\n" + "\n".join(problems)
