"""Regenerate a pipeline's expected golden file from its input fixture.

The table config is the source of truth: after an INTENTIONAL change
to the contract (columns, treatments, validations) or to the engine,
run this to make the expected output reflect it - then review the
resulting diff before trusting it. If the golden test fails and the
change was NOT intentional, fix the code instead of regenerating.

Also creates the very first golden of a new pipeline (only the input
and config fixtures are required to exist).

Usage (inside the container):
    python -m tests.pipelines.regenerate_expected <pipeline>
"""
import json
import os
import sys

from connections.spark_session import get_spark
from tests.pipelines.fixtures import PIPELINES_ROOT, discover_pipelines, produce_staging


def main() -> None:
    names = discover_pipelines(require_expected=False)
    if len(sys.argv) != 2 or sys.argv[1] not in names:
        raise SystemExit(
            f"usage: python -m tests.pipelines.regenerate_expected [{'|'.join(names)}]"
        )
    name = sys.argv[1]

    spark = get_spark("regenerate-expected", master="local[1]")
    try:
        rows, key_columns = produce_staging(spark, name)
    finally:
        spark.stop()
    rows.sort(key=lambda row: tuple(repr(row.get(column)) for column in key_columns))

    destination = os.path.join(PIPELINES_ROOT, name, f"output_staging_{name}.json")
    with open(destination, "w", encoding="utf-8") as file:
        json.dump({f"staging_{name}": rows}, file, ensure_ascii=False, indent=4)
        file.write("\n")
    print(f"golden regenerated: {destination} ({len(rows)} rows)")
    print("review the diff before trusting it")


if __name__ == "__main__":
    main()
