from data_quality.quality_report import split_by_quality, summarize_rejections


def test_split_by_quality_routes_rows_by_flag(spark):
    df = spark.createDataFrame(
        [
            ("v1", True, ""),
            ("v2", False, "invalid_plate"),
            ("v3", True, ""),
        ],
        "id string, quality_ok boolean, dq_observations string",
    )

    approved, rejected = split_by_quality(df)

    assert sorted(row["id"] for row in approved.collect()) == ["v1", "v3"]
    assert [row["id"] for row in rejected.collect()] == ["v2"]


def test_split_by_quality_null_flag_counts_as_rejected(spark):
    df = spark.createDataFrame(
        [("v1", True), ("v2", None)],
        "id string, quality_ok boolean",
    )

    approved, rejected = split_by_quality(df)

    assert [row["id"] for row in approved.collect()] == ["v1"]
    assert [row["id"] for row in rejected.collect()] == ["v2"]


def test_split_by_quality_keeps_existing_columns_intact(spark):
    df = spark.createDataFrame(
        [("v1", "ABC1234", False, "invalid_plate")],
        "id string, plate string, quality_ok boolean, dq_observations string",
    )

    _, rejected = split_by_quality(df)

    assert rejected.columns == df.columns
    assert rejected.collect()[0]["plate"] == "ABC1234"


def test_summarize_rejections_counts_each_reason(spark):
    df = spark.createDataFrame(
        [
            ("v1", "invalid_plate"),
            ("v2", "invalid_plate;missing_driver"),
            ("v3", "missing_driver"),
            ("v4", "invalid_plate"),
        ],
        "id string, dq_observations string",
    )

    assert summarize_rejections(df) == [("invalid_plate", 3), ("missing_driver", 2)]


def test_summarize_rejections_orders_ties_by_reason(spark):
    df = spark.createDataFrame(
        [("v1", "b_reason"), ("v2", "a_reason")],
        "id string, dq_observations string",
    )

    assert summarize_rejections(df) == [("a_reason", 1), ("b_reason", 1)]


def test_summarize_rejections_empty_input_returns_no_reasons(spark):
    df = spark.createDataFrame([], "id string, dq_observations string")

    assert summarize_rejections(df) == []
