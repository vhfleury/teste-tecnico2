"""Regenerate a pipeline's expected golden file from its input fixture.

The table config is the source of truth: after an INTENTIONAL change
to the contract (columns, treatments, validations) or to the engine,
run this to make the expected output reflect it - then review the
resulting diff before trusting it. If the golden test fails and the
change was NOT intentional, fix the code instead of regenerating.

Usage (inside the container):
    python -m tests.pipelines.regenerate_expected motoristas
"""
import json
import os
import sys

from connections.spark_session import get_spark
from tests.pipelines.test_motoristas import PIPELINE_DIR as MOTORISTAS_DIR
from tests.pipelines.test_motoristas import produce_staging as produce_motoristas

# name -> (pipeline dir, expected file, table name, produce function)
PIPELINES = {
    "motoristas": (
        MOTORISTAS_DIR,
        "output_staging_motoristas.json",
        "staging_motoristas",
        produce_motoristas,
    ),
}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in PIPELINES:
        options = "|".join(PIPELINES)
        raise SystemExit(f"usage: python -m tests.pipelines.regenerate_expected [{options}]")
    pipeline_dir, file_name, table_name, produce = PIPELINES[sys.argv[1]]

    spark = get_spark("regenerate-expected", master="local[1]")
    try:
        rows, key_columns = produce(spark)
    finally:
        spark.stop()
    rows.sort(key=lambda row: tuple(repr(row.get(column)) for column in key_columns))

    destination = os.path.join(pipeline_dir, file_name)
    with open(destination, "w", encoding="utf-8") as file:
        json.dump({table_name: rows}, file, ensure_ascii=False, indent=4)
        file.write("\n")
    print(f"golden regenerated: {destination} ({len(rows)} rows)")
    print("review the diff before trusting it")


if __name__ == "__main__":
    main()
