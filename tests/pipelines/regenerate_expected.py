"""Regenerate a pipeline's expected golden file from its input fixture."""
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
