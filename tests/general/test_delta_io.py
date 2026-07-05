"""Unit tests for the Delta partition marker helpers."""
import os

from general.delta_io import (
    MARKERS_DIR,
    delta_partition_processed,
    mark_delta_partition_processed,
)


def test_delta_partition_processed_requires_marker(tmp_path):
    base_dir = str(tmp_path / "analytics" / "example")

    assert not delta_partition_processed(base_dir, "2024-01-01")

    mark_delta_partition_processed(base_dir, "2024-01-01")

    assert delta_partition_processed(base_dir, "2024-01-01")


def test_marker_is_scoped_per_ingest_date(tmp_path):
    base_dir = str(tmp_path / "analytics" / "example")

    mark_delta_partition_processed(base_dir, "2024-01-01")

    assert delta_partition_processed(base_dir, "2024-01-01")
    assert not delta_partition_processed(base_dir, "2024-01-02")


def test_mark_delta_partition_processed_is_idempotent(tmp_path):
    base_dir = str(tmp_path / "analytics" / "example")

    mark_delta_partition_processed(base_dir, "2024-01-01")
    mark_delta_partition_processed(base_dir, "2024-01-01")

    assert delta_partition_processed(base_dir, "2024-01-01")


def test_marker_lives_in_underscore_directory_ignored_by_readers(tmp_path):
    base_dir = str(tmp_path / "analytics" / "example")

    mark_delta_partition_processed(base_dir, "2024-01-01")

    assert os.path.isfile(os.path.join(base_dir, MARKERS_DIR, "2024-01-01"))
